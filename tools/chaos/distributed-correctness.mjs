#!/usr/bin/env node

import { spawn } from "node:child_process";
import { createHash, createHmac } from "node:crypto";
import { createReadStream, createWriteStream, existsSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import http from "node:http";
import { connect as connectTcp, createServer as createTcpServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const APP_ID = "app-id";
const APP_KEY = "app-key";
const APP_SECRET = "app-secret";
const EVENT_NAME = "distributed.correctness";

const args = parseArgs(process.argv.slice(2));
const seed = numberArg(args.seed, randomSeed());
const runToken = `${Date.now()}-${seed}`;
const prefix = `dc_${String(seed >>> 0)}_${Date.now()}`;
const artifactRoot = resolve(ROOT_DIR, args.artifactDir ?? "target/distributed-correctness");
const artifactDir = join(artifactRoot, runToken);
const artifactPath = join(artifactDir, "artifact.json");
const config = {
  seed,
  durationMs: numberArg(args.durationMs, 120_000),
  publishIntervalMs: numberArg(args.publishIntervalMs, 200),
  churnIntervalMs: numberArg(args.churnIntervalMs, 125),
  churnHoldMs: numberArg(args.churnHoldMs, 175),
  settleMs: numberArg(args.settleMs, 7_000),
  nodeTimeoutMs: numberArg(args.nodeTimeoutMs, 3_500),
  requireDistinctBuilds: boolArg(args.requireDistinctBuilds, true),
  oldBin: resolve(ROOT_DIR, args.oldBin ?? "sockudo-compatibility/.cache/sockudo/target/debug/sockudo"),
  newBin: resolve(ROOT_DIR, args.newBin ?? "target/debug/sockudo"),
  loadBalancerPort: numberArg(args.loadBalancerPort, 6200),
  nodePorts: [numberArg(args.nodeAPort, 6201), numberArg(args.nodeBPort, 6202)],
  metricsPorts: [numberArg(args.metricsAPort, 9801), numberArg(args.metricsBPort, 9802)],
  redisProxyPorts: [numberArg(args.redisProxyAPort, 17381), numberArg(args.redisProxyBPort, 17382)],
  redisHost: args.redisHost ?? "127.0.0.1",
  redisPort: numberArg(args.redisPort, 16_379),
  postgresHost: args.postgresHost ?? "127.0.0.1",
  postgresPort: numberArg(args.postgresPort, 15_432),
  postgresUsername: args.postgresUsername ?? "postgres",
  postgresPassword: args.postgresPassword ?? "postgres123",
  postgresDatabase: args.postgresDatabase ?? "sockudo_test",
};

if (config.durationMs < 30_000) {
  fail("--duration-ms must be at least 30000 so cluster-health cleanup is observed outside-in");
}
if (new Set([
  config.loadBalancerPort,
  ...config.nodePorts,
  ...config.metricsPorts,
  ...config.redisProxyPorts,
]).size !== 7) {
  fail("load-balancer, node, metrics, and Redis-proxy ports must be distinct");
}

const channelName = `distributed-${runToken}`;
const presenceChannel = `presence-distributed-${runToken}`;
const deadOwnerId = `dead-owner-${runToken}`;
const events = [];
const failures = [];
const publishResults = [];
const processRuns = [];
const presenceChurn = [];
const children = new Set();
const redisProxies = [];
let loadBalancer;
let stopping = false;
let workloadStopped = false;

const nodes = [
  makeNode("node-a", 0),
  makeNode("node-b", 1),
];

const counters = {
  processStarts: 0,
  processKills: 0,
  rollingReplacements: 0,
  loadBalancerRequests: 0,
  loadBalancerUpgrades: 0,
  redisPartitions: 0,
  redisProxyConnections: 0,
  publishesAttempted: 0,
  publishWireAttempts: 0,
  publishRetries: 0,
  publishTransientFailures: 0,
  publishesAccepted: 0,
  publishesRejected: 0,
  duplicateProbes: 0,
  churnAttempts: 0,
  churnCompleted: 0,
  churnFailed: 0,
};

function makeNode(name, index) {
  return {
    name,
    port: config.nodePorts[index],
    metricsPort: config.metricsPorts[index],
    redisProxyPort: config.redisProxyPorts[index],
    configPath: join(artifactDir, `${name}.toml`),
    child: null,
    generation: "old",
    healthy: false,
  };
}

async function main() {
  requireFile(config.oldBin, "old binary");
  requireFile(config.newBin, "new binary");
  if (typeof WebSocket === "undefined") {
    fail("this harness requires a Node.js runtime with global WebSocket support");
  }

  await mkdir(artifactDir, { recursive: true });
  const [oldIdentity, newIdentity] = await Promise.all([
    binaryIdentity(config.oldBin),
    binaryIdentity(config.newBin),
  ]);
  if (config.requireDistinctBuilds && oldIdentity.sha256 === newIdentity.sha256) {
    fail("old and new binaries have the same SHA-256; pass two separately built packages or disable --require-distinct-builds for a lifecycle-only smoke run");
  }

  record("run_start", { oldIdentity, newIdentity });
  await verifyDependency(`http://${config.redisHost}:${config.redisPort}`, "redis", async () => {
    const socket = connectTcp(config.redisPort, config.redisHost);
    await onceConnected(socket, 3_000);
    socket.end();
  });
  await verifyDependency(`${config.postgresHost}:${config.postgresPort}`, "postgres", async () => {
    const socket = connectTcp(config.postgresPort, config.postgresHost);
    await onceConnected(socket, 3_000);
    socket.end();
  });

  for (const node of nodes) {
    await writeFile(node.configPath, serverConfig(node));
    redisProxies.push(await startRedisProxy(node));
  }
  loadBalancer = await startLoadBalancer(nodes);

  await Promise.all(nodes.map((node) => startNode(node, config.oldBin, "old")));
  loadBalancer.setActive(nodes.map((node) => node.name));
  await waitForClusterFanout();

  const businessSubscriber = new ResilientClient({
    name: "load-balanced-business-subscriber",
    port: config.loadBalancerPort,
    channel: channelName,
  });
  const presenceObserver = new ResilientClient({
    name: "surviving-presence-observer",
    port: nodes[0].port,
    channel: presenceChannel,
    userId: `observer-${runToken}`,
  });
  businessSubscriber.start();
  presenceObserver.start();
  await Promise.all([
    businessSubscriber.waitUntilSubscribed(15_000),
    presenceObserver.waitUntilSubscribed(15_000),
  ]);

  const workloadDeadline = Date.now() + config.durationMs;
  const publisher = publishLoop(workloadDeadline);
  const churner = presenceChurnLoop(workloadDeadline);
  let deadOwner;

  try {
    await sleep(Math.max(3_000, Math.floor(config.durationMs * 0.08)));
    await rollNode(nodes[0]);
    await sleep(Math.max(3_000, Math.floor(config.durationMs * 0.08)));
    await rollNode(nodes[1]);
    record("rolling_deployment_complete", {
      active: loadBalancer.activeNames(),
      generations: Object.fromEntries(nodes.map((node) => [node.name, node.generation])),
    });
    await presenceObserver.waitUntilSubscribed(15_000);

    deadOwner = new ResilientClient({
      name: "dead-node-presence-owner",
      port: nodes[1].port,
      channel: presenceChannel,
      userId: deadOwnerId,
      reconnect: false,
    });
    deadOwner.start();
    await deadOwner.waitUntilSubscribed(15_000);
    await presenceObserver.waitForMember("member_added", deadOwnerId, 12_000);

    redisProxies[1].partition();
    counters.redisPartitions += 1;
    record("node_redis_partition", { node: nodes[1].name });
    await sleep(800);
    loadBalancer.setActive([nodes[0].name]);
    await killNode(nodes[1], "partitioned_node_sigkill");
    await presenceObserver.waitForMember(
      "member_removed",
      deadOwnerId,
      config.nodeTimeoutMs + 12_000,
    );
    record("dead_node_presence_cleaned", { userId: deadOwnerId });

    redisProxies[1].heal();
    record("node_redis_partition_healed", { node: nodes[1].name });
    await startNode(nodes[1], config.newBin, "new-after-partition");
    loadBalancer.setActive(nodes.map((node) => node.name));
    await waitForClusterFanout();

    const remaining = workloadDeadline - Date.now();
    if (remaining > 0) {
      await sleep(remaining);
    }
    workloadStopped = true;
    await Promise.all([publisher, churner]);
    await sleep(config.settleMs);

    presenceObserver.stop();
    businessSubscriber.stop();
    deadOwner.stop();
    await sleep(config.nodeTimeoutMs + 1_500);

    const audit = await auditCorrectness({
      oldIdentity,
      newIdentity,
      businessSubscriber,
      presenceObserver,
    });
    if (audit.missingAcceptedSequences.length > 0) {
      failures.push({
        type: "accepted_publish_missing_from_durable_history",
        sequences: audit.missingAcceptedSequences,
      });
    }
    if (audit.duplicateHistorySequences.length > 0) {
      failures.push({
        type: "duplicate_durable_publish",
        sequences: audit.duplicateHistorySequences,
      });
    }
    if (audit.duplicateLiveSequences.length > 0) {
      failures.push({
        type: "duplicate_live_delivery",
        sequences: audit.duplicateLiveSequences,
      });
    }
    if (audit.ghostMembers.length > 0) {
      failures.push({ type: "presence_ghosts_after_cleanup", members: audit.ghostMembers });
    }
    if (audit.deadOwnerTransitions.memberAdded !== 1 || audit.deadOwnerTransitions.memberRemoved !== 1) {
      failures.push({
        type: "dead_owner_transition_cardinality",
        expected: { memberAdded: 1, memberRemoved: 1 },
        actual: audit.deadOwnerTransitions,
      });
    }
    if (audit.deadOwnerWireTransitions.memberAdded !== 1 || audit.deadOwnerWireTransitions.memberRemoved !== 1) {
      failures.push({
        type: "dead_owner_wire_transition_cardinality",
        expected: { memberAdded: 1, memberRemoved: 1 },
        actual: audit.deadOwnerWireTransitions,
      });
    }
    if (!audit.presenceContinuity.complete || audit.presenceContinuity.degraded) {
      failures.push({
        type: "presence_history_continuity_not_proven",
        continuity: audit.presenceContinuity,
      });
    }
    if (counters.publishesAccepted === 0 || businessSubscriber.businessEvents.length === 0) {
      failures.push({
        type: "workload_not_exercised",
        publishesAccepted: counters.publishesAccepted,
        liveEvents: businessSubscriber.businessEvents.length,
      });
    }
    if (counters.publishesRejected !== 0) {
      failures.push({
        type: "publish_availability_regression",
        rejected: counters.publishesRejected,
      });
    }
    if (counters.rollingReplacements !== 2 || counters.processKills < 3) {
      failures.push({
        type: "fault_plan_incomplete",
        rollingReplacements: counters.rollingReplacements,
        processKills: counters.processKills,
      });
    }

    const ok = failures.length === 0;
    await writeArtifact(ok, { oldIdentity, newIdentity, audit });
    process.stdout.write(`${JSON.stringify({ ok, artifact: artifactPath, counters, failures }, null, 2)}\n`);
    process.exitCode = ok ? 0 : 1;
  } finally {
    workloadStopped = true;
    businessSubscriber.stop();
    presenceObserver.stop();
    deadOwner?.stop();
  }
}

async function rollNode(node) {
  const survivors = nodes.filter((candidate) => candidate !== node && candidate.healthy);
  if (survivors.length === 0) {
    throw new Error(`cannot roll ${node.name} without a healthy survivor`);
  }
  loadBalancer.setActive(survivors.map((candidate) => candidate.name));
  record("rolling_node_drained", { node: node.name, active: loadBalancer.activeNames() });
  await sleep(200);
  await killNode(node, "rolling_sigkill");
  await startNode(node, config.newBin, "new");
  loadBalancer.setActive(nodes.filter((candidate) => candidate.healthy).map((candidate) => candidate.name));
  counters.rollingReplacements += 1;
  record("rolling_node_replaced", { node: node.name, active: loadBalancer.activeNames() });
  await waitForClusterFanout();
}

async function startNode(node, binary, generation) {
  const run = {
    node: node.name,
    generation,
    binary,
    startedAt: new Date().toISOString(),
    log: join(artifactDir, `${node.name}-${processRuns.length + 1}-${generation}.log`),
  };
  const log = createWriteStream(run.log, { flags: "a" });
  const child = spawn(binary, ["--config", node.configPath], {
    cwd: ROOT_DIR,
    env: {
      ...process.env,
      RUST_LOG: process.env.RUST_LOG ?? "info",
      LOG_OUTPUT_FORMAT: process.env.LOG_OUTPUT_FORMAT ?? "json",
      INSTANCE_PROCESS_ID: `${prefix}-${node.name}-${generation}`,
      DATABASE_REDIS_HOST: "127.0.0.1",
      DATABASE_REDIS_PORT: String(node.redisProxyPort),
      DATABASE_REDIS_DB: "0",
      DATABASE_REDIS_KEY_PREFIX: `${prefix}:`,
      DATABASE_POSTGRES_HOST: config.postgresHost,
      DATABASE_POSTGRES_PORT: String(config.postgresPort),
      DATABASE_POSTGRES_USERNAME: config.postgresUsername,
      DATABASE_POSTGRES_PASSWORD: config.postgresPassword,
      DATABASE_POSTGRES_DATABASE: config.postgresDatabase,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.pipe(log, { end: false });
  child.stderr.pipe(log, { end: false });
  children.add(child);
  node.child = child;
  node.generation = generation;
  node.healthy = false;
  counters.processStarts += 1;
  processRuns.push(run);
  record("process_start", { node: node.name, generation, pid: child.pid, binary });
  child.once("exit", (code, signal) => {
    children.delete(child);
    node.healthy = false;
    run.exitedAt = new Date().toISOString();
    run.exitCode = code;
    run.signal = signal;
    record("process_exit", { node: node.name, generation, code, signal });
  });
  await waitForHttp(`http://127.0.0.1:${node.port}/up/${APP_ID}`, 30_000);
  node.healthy = true;
}

async function killNode(node, reason) {
  const child = node.child;
  if (!child || child.exitCode !== null) {
    node.healthy = false;
    return;
  }
  node.healthy = false;
  counters.processKills += 1;
  record(reason, { node: node.name, generation: node.generation, pid: child.pid });
  child.kill("SIGKILL");
  await onceExit(child, 5_000);
}

async function waitForClusterFanout() {
  let lastError;
  for (let attempt = 1; attempt <= 6; attempt += 1) {
    const probeA = new OneShotClient({
      name: `cluster-probe-a-${events.length}-${attempt}`,
      port: nodes[0].port,
      channel: channelName,
    });
    const probeB = new OneShotClient({
      name: `cluster-probe-b-${events.length}-${attempt}`,
      port: nodes[1].port,
      channel: channelName,
    });
    try {
      await Promise.all([probeA.connect(), probeB.connect()]);
      const sequenceAToB = -(events.length * 100 + attempt * 2);
      const sequenceBToA = sequenceAToB - 1;
      const aToB = await publish(sequenceAToB, nodes[0].port, false);
      if (!aToB.accepted) {
        throw new Error(`A-to-B cluster probe publish rejected: ${JSON.stringify(aToB)}`);
      }
      await probeB.waitForBusinessSequence(sequenceAToB, 4_000);
      const bToA = await publish(sequenceBToA, nodes[1].port, false);
      if (!bToA.accepted) {
        throw new Error(`B-to-A cluster probe publish rejected: ${JSON.stringify(bToA)}`);
      }
      await probeA.waitForBusinessSequence(sequenceBToA, 4_000);
      record("cluster_fanout_ready", { attempt, sequenceAToB, sequenceBToA });
      return;
    } catch (error) {
      lastError = error;
      record("cluster_fanout_retry", { attempt, message: error.message });
      await sleep(500);
    } finally {
      probeA.close();
      probeB.close();
    }
  }
  throw new Error(`cluster fanout did not become ready: ${lastError?.message ?? "unknown error"}`);
}

async function publishLoop(deadline) {
  let sequence = 0;
  while (!workloadStopped && Date.now() < deadline) {
    sequence += 1;
    const result = await publish(sequence, config.loadBalancerPort, false);
    publishResults.push(result);
    if (result.accepted && sequence % 7 === 0) {
      counters.duplicateProbes += 1;
      const duplicate = await publish(sequence, config.loadBalancerPort, true);
      publishResults.push(duplicate);
    }
    await sleep(config.publishIntervalMs);
  }
}

async function publish(sequence, port, duplicate) {
  counters.publishesAttempted += 1;
  const requestPath = `/apps/${APP_ID}/events`;
  const body = JSON.stringify({
    name: EVENT_NAME,
    channels: [channelName],
    data: JSON.stringify({ runToken, sequence }),
    idempotency_key: `${runToken}-${sequence}`,
  });
  const transientFailures = [];
  const maxAttempts = 5;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    counters.publishWireAttempts += 1;
    try {
      const response = await signedFetch(port, "POST", requestPath, body);
      const responseBody = await response.text();
      const accepted = response.status === 200 || response.status === 202;
      const retryable = response.status >= 500 && response.status <= 599;
      if (!accepted && retryable && attempt < maxAttempts) {
        counters.publishRetries += 1;
        counters.publishTransientFailures += 1;
        transientFailures.push({ attempt, status: response.status, body: responseBody.slice(0, 500) });
        await sleep(attempt * 100);
        continue;
      }
      if (!duplicate) {
        counters[accepted ? "publishesAccepted" : "publishesRejected"] += 1;
      }
      return {
        at: new Date().toISOString(),
        sequence,
        duplicate,
        accepted,
        status: response.status,
        attempts: attempt,
        transientFailures,
        body: accepted ? undefined : responseBody.slice(0, 500),
      };
    } catch (error) {
      if (attempt < maxAttempts) {
        counters.publishRetries += 1;
        counters.publishTransientFailures += 1;
        transientFailures.push({ attempt, error: error.message });
        await sleep(attempt * 100);
        continue;
      }
      if (!duplicate) {
        counters.publishesRejected += 1;
      }
      return {
        at: new Date().toISOString(),
        sequence,
        duplicate,
        accepted: false,
        attempts: attempt,
        transientFailures,
        error: error.message,
      };
    }
  }
  throw new Error("unreachable publish retry state");
}

async function presenceChurnLoop(deadline) {
  let sequence = 0;
  while (!workloadStopped && Date.now() < deadline) {
    sequence += 1;
    counters.churnAttempts += 1;
    const userId = `churn-${runToken}-${sequence}`;
    const client = new OneShotClient({
      name: userId,
      port: config.loadBalancerPort,
      channel: presenceChannel,
      userId,
    });
    const result = { userId, startedAt: new Date().toISOString() };
    try {
      await client.connect(8_000);
      await sleep(config.churnHoldMs);
      counters.churnCompleted += 1;
      result.completed = true;
    } catch (error) {
      counters.churnFailed += 1;
      result.completed = false;
      result.error = error.message;
    } finally {
      client.close();
      result.finishedAt = new Date().toISOString();
      presenceChurn.push(result);
    }
    await sleep(config.churnIntervalMs);
  }
}

class OneShotClient {
  constructor({ name, port, channel, userId }) {
    this.name = name;
    this.port = port;
    this.channel = channel;
    this.userId = userId;
    this.diagnostic = name.includes("presence-observer") || name.includes("dead-node-presence-owner");
    this.frames = [];
    this.businessEvents = [];
    this.socket = null;
    this.snapshot = null;
    this.currentSubscribed = false;
  }

  async connect(timeoutMs = 12_000) {
    const socket = new WebSocket(`ws://127.0.0.1:${this.port}/app/${APP_KEY}?protocol=7&client=distributed-chaos&version=1.0`);
    this.socket = socket;
    socket.addEventListener("message", (event) => this.onMessage(event.data));
    await waitFor(() => this.isSubscribed(), timeoutMs, `${this.name} subscription`);
  }

  onMessage(raw) {
    const frame = parseJson(String(raw));
    if (!frame) {
      return;
    }
    frame.data = parseJson(frame.data) ?? frame.data;
    this.frames.push(frame);
    const event = normalizeEvent(frame.event);
    if (this.diagnostic && (
      ["connection_established", "subscription_succeeded", "subscription_error"].includes(event)
      || (["member_added", "member_removed"].includes(event) && frame.data?.user_id === deadOwnerId)
    )) {
      record("diagnostic_client_frame", { client: this.name, event, channel: frame.channel, data: frame.data });
    }
    if (event === "connection_established") {
      const socketId = frame.data?.socket_id;
      if (!socketId) {
        return;
      }
      this.socket.send(JSON.stringify({
        event: "pusher:subscribe",
        data: this.userId
          ? presenceSubscription(socketId, this.channel, this.userId)
          : { channel: this.channel },
      }));
    } else if (event === "subscription_succeeded" && frame.channel === this.channel) {
      this.snapshot = frame.data?.presence ?? null;
      this.currentSubscribed = true;
    } else if (frame.channel === this.channel && frame.event === EVENT_NAME) {
      const data = parseJson(frame.data) ?? frame.data;
      if (data && Number.isFinite(Number(data.sequence))) {
        this.businessEvents.push(Number(data.sequence));
      }
    }
  }

  isSubscribed() {
    return this.frames.some((frame) => normalizeEvent(frame.event) === "subscription_succeeded" && frame.channel === this.channel);
  }

  waitForBusinessSequence(sequence, timeoutMs) {
    return waitFor(() => this.businessEvents.includes(sequence), timeoutMs, `${this.name} business event ${sequence}`);
  }

  close() {
    this.currentSubscribed = false;
    try {
      this.socket?.close();
    } catch {
      // Best-effort cleanup after deliberate process/network faults.
    }
  }
}

class ResilientClient extends OneShotClient {
  constructor(options) {
    super(options);
    this.reconnect = options.reconnect ?? true;
    this.running = false;
    this.loop = null;
    this.connections = 0;
  }

  start() {
    this.running = true;
    this.loop = this.connectionLoop();
  }

  stop() {
    this.running = false;
    this.close();
  }

  async connectionLoop() {
    while (this.running) {
      try {
        this.currentSubscribed = false;
        const socket = new WebSocket(`ws://127.0.0.1:${this.port}/app/${APP_KEY}?protocol=7&client=distributed-chaos&version=1.0`);
        this.socket = socket;
        this.connections += 1;
        await new Promise((resolveSocket) => {
          socket.addEventListener("message", (event) => this.onMessage(event.data));
          socket.addEventListener("close", resolveSocket, { once: true });
          socket.addEventListener("error", () => {}, { once: true });
        });
        this.currentSubscribed = false;
      } catch (error) {
        record("client_connection_error", { client: this.name, message: error.message });
      }
      if (!this.running || !this.reconnect) {
        this.running = false;
        return;
      }
      await sleep(150);
    }
  }

  waitUntilSubscribed(timeoutMs) {
    return waitFor(() => this.currentSubscribed, timeoutMs, `${this.name} active subscription`);
  }

  waitForMember(kind, userId, timeoutMs) {
    return waitFor(
      () => this.memberEvents(kind, userId).length > 0,
      timeoutMs,
      `${this.name} ${kind} ${userId}`,
    );
  }

  memberEvents(kind, userId) {
    return this.frames.filter((frame) => normalizeEvent(frame.event) === kind && frame.data?.user_id === userId);
  }
}

async function auditCorrectness({ businessSubscriber, presenceObserver }) {
  const accepted = publishResults
    .filter((result) => result.accepted && !result.duplicate && result.sequence > 0)
    .map((result) => result.sequence);
  const history = await readAllHistory(channelName);
  const historyCounts = new Map();
  for (const item of history.items ?? []) {
    const payload = extractHistoryPayload(item);
    if (payload?.runToken === runToken && Number.isFinite(Number(payload.sequence))) {
      const sequence = Number(payload.sequence);
      historyCounts.set(sequence, (historyCounts.get(sequence) ?? 0) + 1);
    }
  }
  const liveCounts = countValues(businessSubscriber.businessEvents.filter((sequence) => sequence > 0));
  const presenceHistory = await readAllPresenceHistory(presenceChannel);
  const deadOwnerRows = (presenceHistory.items ?? []).filter((item) => item.user_id === deadOwnerId);
  const ghostMembers = await waitForNoPresenceGhosts();
  return {
    acceptedSequences: accepted,
    missingAcceptedSequences: accepted.filter((sequence) => !historyCounts.has(sequence)),
    duplicateHistorySequences: [...historyCounts].filter(([, count]) => count !== 1).map(([sequence]) => sequence),
    duplicateLiveSequences: [...liveCounts].filter(([, count]) => count > 1).map(([sequence]) => sequence),
    durableHistoryItems: history.items?.length ?? 0,
    liveBusinessEvents: businessSubscriber.businessEvents.length,
    businessSubscriberConnections: businessSubscriber.connections,
    presenceObserverConnections: presenceObserver.connections,
    ghostMembers,
    deadOwnerTransitions: {
      memberAdded: deadOwnerRows.filter((item) => item.event === "member_added").length,
      memberRemoved: deadOwnerRows.filter((item) => item.event === "member_removed").length,
      causes: deadOwnerRows.map((item) => item.cause),
    },
    deadOwnerWireTransitions: {
      memberAdded: presenceObserver.memberEvents("member_added", deadOwnerId).length,
      memberRemoved: presenceObserver.memberEvents("member_removed", deadOwnerId).length,
    },
    presenceContinuity: presenceHistory.continuity ?? {},
  };
}

async function waitForNoPresenceGhosts() {
  const deadline = Date.now() + config.nodeTimeoutMs + 15_000;
  let lastMembers = [];
  while (Date.now() < deadline) {
    const userId = `auditor-${Date.now()}`;
    const client = new OneShotClient({
      name: userId,
      port: nodes[0].port,
      channel: presenceChannel,
      userId,
    });
    try {
      await client.connect();
      const hash = client.snapshot?.hash ?? {};
      lastMembers = Object.keys(hash).filter((member) => member !== userId);
    } finally {
      client.close();
    }
    if (lastMembers.length === 0) {
      return [];
    }
    await sleep(500);
  }
  return lastMembers;
}

async function readAllHistory(channel) {
  return signedJson(
    nodes[0].port,
    "GET",
    `/apps/${APP_ID}/channels/${encodeURIComponent(channel)}/history`,
    "",
    { direction: "oldest_first", limit: "10000" },
  );
}

async function readAllPresenceHistory(channel) {
  return signedJson(
    nodes[0].port,
    "GET",
    `/apps/${APP_ID}/channels/${encodeURIComponent(channel)}/presence/history`,
    "",
    { direction: "oldest_first", limit: "10000" },
  );
}

function extractHistoryPayload(item) {
  const message = item?.message?.message ?? item?.message;
  return parseJson(message?.data) ?? message?.data ?? null;
}

function presenceSubscription(socketId, channel, userId) {
  const channelData = JSON.stringify({ user_id: userId, user_info: { runToken } });
  const signature = createHmac("sha256", APP_SECRET)
    .update(`${socketId}:${channel}:${channelData}`)
    .digest("hex");
  return { channel, channel_data: channelData, auth: `${APP_KEY}:${signature}` };
}

async function signedJson(port, method, requestPath, body = "", query = {}) {
  const response = await signedFetch(port, method, requestPath, body, query);
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${method} ${requestPath} failed (${response.status}): ${text.slice(0, 1000)}`);
  }
  return text ? JSON.parse(text) : null;
}

function signedFetch(port, method, requestPath, body = "", extraQuery = {}) {
  const params = {
    ...extraQuery,
    auth_key: APP_KEY,
    auth_timestamp: String(Math.floor(Date.now() / 1000)),
    auth_version: "1.0",
  };
  if (body.length > 0) {
    params.body_md5 = createHash("md5").update(body).digest("hex");
  }
  const canonicalQuery = Object.entries(params)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  const signature = createHmac("sha256", APP_SECRET)
    .update(`${method}\n${requestPath}\n${canonicalQuery}`)
    .digest("hex");
  const encoded = new URLSearchParams({ ...params, auth_signature: signature });
  return fetch(`http://127.0.0.1:${port}${requestPath}?${encoded}`, {
    method,
    headers: body ? { "content-type": "application/json", connection: "close" } : { connection: "close" },
    body: body || undefined,
  });
}

async function startRedisProxy(node) {
  let partitioned = false;
  const sockets = new Set();
  const server = createTcpServer((client) => {
    counters.redisProxyConnections += 1;
    if (partitioned) {
      client.destroy();
      return;
    }
    const upstream = connectTcp(config.redisPort, config.redisHost);
    sockets.add(client);
    sockets.add(upstream);
    const discard = () => {
      client.destroy();
      upstream.destroy();
      sockets.delete(client);
      sockets.delete(upstream);
    };
    client.once("error", discard);
    upstream.once("error", discard);
    client.once("close", () => sockets.delete(client));
    upstream.once("close", () => sockets.delete(upstream));
    client.pipe(upstream).pipe(client);
  });
  await listen(server, node.redisProxyPort);
  return {
    node: node.name,
    partition() {
      partitioned = true;
      for (const socket of sockets) {
        socket.destroy();
      }
      sockets.clear();
    },
    heal() {
      partitioned = false;
    },
    close: () => closeServer(server, sockets),
  };
}

async function startLoadBalancer(upstreams) {
  let active = new Set();
  let cursor = 0;
  const sockets = new Set();
  const choose = () => {
    const candidates = upstreams.filter((node) => active.has(node.name) && node.healthy);
    if (candidates.length === 0) {
      return null;
    }
    const selected = candidates[cursor % candidates.length];
    cursor += 1;
    return selected;
  };
  const server = http.createServer((request, response) => {
    counters.loadBalancerRequests += 1;
    const selected = choose();
    if (!selected) {
      response.writeHead(503, { "content-type": "text/plain", connection: "close" });
      response.end("no healthy Sockudo upstream\n");
      return;
    }
    const proxy = http.request({
      hostname: "127.0.0.1",
      port: selected.port,
      method: request.method,
      path: request.url,
      headers: { ...request.headers, host: `127.0.0.1:${selected.port}`, connection: "close" },
    }, (upstreamResponse) => {
      response.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers);
      upstreamResponse.pipe(response);
    });
    proxy.once("error", (error) => {
      if (!response.headersSent) {
        response.writeHead(502, { "content-type": "text/plain", connection: "close" });
      }
      response.end(`upstream failure: ${error.message}\n`);
    });
    request.pipe(proxy);
  });
  server.on("upgrade", (request, client, head) => {
    counters.loadBalancerUpgrades += 1;
    const selected = choose();
    if (!selected) {
      client.end("HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n");
      return;
    }
    const upstream = connectTcp(selected.port, "127.0.0.1");
    sockets.add(client);
    sockets.add(upstream);
    const destroy = () => {
      client.destroy();
      upstream.destroy();
      sockets.delete(client);
      sockets.delete(upstream);
    };
    upstream.once("connect", () => {
      let raw = `${request.method} ${request.url} HTTP/${request.httpVersion}\r\n`;
      for (let index = 0; index < request.rawHeaders.length; index += 2) {
        const name = request.rawHeaders[index];
        const value = name.toLowerCase() === "host" ? `127.0.0.1:${selected.port}` : request.rawHeaders[index + 1];
        raw += `${name}: ${value}\r\n`;
      }
      upstream.write(`${raw}\r\n`);
      if (head.length > 0) {
        upstream.write(head);
      }
      client.pipe(upstream).pipe(client);
    });
    client.once("error", destroy);
    upstream.once("error", destroy);
    client.once("close", () => sockets.delete(client));
    upstream.once("close", () => sockets.delete(upstream));
  });
  await listen(server, config.loadBalancerPort);
  return {
    setActive(names) {
      active = new Set(names);
      record("load_balancer_active_set", { active: [...active] });
    },
    activeNames: () => [...active],
    close: () => closeServer(server, sockets),
  };
}

function serverConfig(node) {
  return `debug = false
host = "127.0.0.1"
port = ${node.port}
mode = "development"
path_prefix = "/"
shutdown_grace_period = 1
activity_timeout = 30
health_check_timeout_ms = 1000

[adapter]
driver = "redis"
enable_socket_counting = true
aggregate_counts = false
fast_presence_transitions = false
fallback_to_local = false

[adapter.redis]
requests_timeout = 3000
prefix = "${prefix}:adapter:"

[adapter.cluster_health]
enabled = true
heartbeat_interval_ms = 1000
node_timeout_ms = ${config.nodeTimeoutMs}
cleanup_interval_ms = 1000

[app_manager]
driver = "memory"

[app_manager.array]
[[app_manager.array.apps]]
id = "${APP_ID}"
key = "${APP_KEY}"
secret = "${APP_SECRET}"
enabled = true

[app_manager.array.apps.policy.limits]
max_connections = 10000
max_client_events_per_second = 10000
max_backend_events_per_second = 10000
max_read_requests_per_second = 10000
max_channel_name_length = 200
max_event_channels_at_once = 100
max_event_name_length = 200
max_event_payload_in_kb = 128
max_event_batch_size = 100

[app_manager.array.apps.policy.features]
enable_client_messages = true
enable_user_authentication = false

[app_manager.array.apps.policy.channels]
allowed_origins = ["*"]

[app_manager.array.apps.policy.connection_recovery]
enabled = true

[app_manager.array.apps.policy.history]
enabled = true
rewind_enabled = true
retention_window_seconds = 86400
max_messages_per_channel = 10000

[app_manager.array.apps.policy.idempotency]
enabled = true
ttl_seconds = 600

[cache]
driver = "redis"

[cache.redis]
prefix = "${prefix}:cache:"
url_override = "redis://127.0.0.1:${node.redisProxyPort}/"
cluster_mode = false

[queue]
driver = "memory"

[rate_limiter]
enabled = false
driver = "memory"

[metrics]
enabled = true
driver = "prometheus"
host = "127.0.0.1"
port = ${node.metricsPort}

[idempotency]
enabled = true
ttl_seconds = 600
max_key_length = 128

[connection_recovery]
enabled = true
buffer_ttl_seconds = 120
max_buffer_size = 10000

[history]
enabled = true
rewind_enabled = true
backend = "postgres"
retention_window_seconds = 86400
max_page_size = 10000
max_messages_per_channel = 10000
writer_shards = 2
writer_queue_capacity = 1024
purge_interval_seconds = 300
purge_batch_size = 1000
max_purge_per_tick = 100000

[history.postgres]
table_prefix = "${prefix}_history"
write_timeout_ms = 5000

[presence_history]
enabled = true
retention_window_seconds = 86400
max_page_size = 10000
max_events_per_channel = 10000

[versioned_messages]
enabled = false
driver = "postgres"
max_page_size = 100
retention_window_seconds = 0
purge_interval_seconds = 300
purge_batch_size = 1000
max_purge_per_tick = 100000

[annotations]
enabled = false

[ai_transport]
enabled = false

[push]
storage_driver = "memory"
queue_driver = "memory"
allow_memory_drivers = true
fcm_enabled = false
apns_enabled = false
webpush_enabled = false
hms_enabled = false
wns_enabled = false
`;
}

async function cleanup() {
  if (stopping) {
    return;
  }
  stopping = true;
  workloadStopped = true;
  loadBalancer?.setActive([]);
  for (const node of nodes) {
    await killNode(node, "cleanup_sigkill").catch(() => {});
  }
  await loadBalancer?.close().catch(() => {});
  for (const proxy of redisProxies) {
    await proxy.close().catch(() => {});
  }
  for (const child of children) {
    child.kill("SIGKILL");
  }
}

async function writeArtifact(ok, extra = {}) {
  await mkdir(artifactDir, { recursive: true });
  await writeFile(artifactPath, `${JSON.stringify({
    schema: "sockudo.distributed-correctness.v1",
    ok,
    generatedAt: new Date().toISOString(),
    runToken,
    command: buildReplayCommand(),
    config: { ...config, postgresPassword: "<redacted>" },
    channels: { business: channelName, presence: presenceChannel },
    counters,
    failures,
    events,
    processRuns,
    publishResults,
    presenceChurn,
    ...extra,
  }, null, 2)}\n`);
}

function buildReplayCommand() {
  const values = [
    ["seed", config.seed],
    ["duration-ms", config.durationMs],
    ["publish-interval-ms", config.publishIntervalMs],
    ["churn-interval-ms", config.churnIntervalMs],
    ["churn-hold-ms", config.churnHoldMs],
    ["settle-ms", config.settleMs],
    ["node-timeout-ms", config.nodeTimeoutMs],
    ["require-distinct-builds", config.requireDistinctBuilds],
    ["old-bin", config.oldBin],
    ["new-bin", config.newBin],
    ["load-balancer-port", config.loadBalancerPort],
    ["node-a-port", config.nodePorts[0]],
    ["node-b-port", config.nodePorts[1]],
    ["metrics-a-port", config.metricsPorts[0]],
    ["metrics-b-port", config.metricsPorts[1]],
    ["redis-proxy-a-port", config.redisProxyPorts[0]],
    ["redis-proxy-b-port", config.redisProxyPorts[1]],
    ["redis-host", config.redisHost],
    ["redis-port", config.redisPort],
    ["postgres-host", config.postgresHost],
    ["postgres-port", config.postgresPort],
    ["postgres-username", config.postgresUsername],
    ["postgres-database", config.postgresDatabase],
  ];
  return ["node", "tools/chaos/distributed-correctness.mjs", ...values.flatMap(([key, value]) => [
    `--${key}`,
    shellQuote(String(value)),
  ])].join(" ");
}

function record(type, details = {}) {
  events.push({ at: new Date().toISOString(), type, ...details });
}

async function verifyDependency(label, name, check) {
  try {
    await check();
  } catch (error) {
    throw new Error(`${name} dependency unavailable at ${label}: ${error.message}`);
  }
}

function requireFile(path, label) {
  if (!existsSync(path)) {
    fail(`${label} not found: ${path}`);
  }
}

function binaryIdentity(path) {
  return new Promise((resolveIdentity, rejectIdentity) => {
    const hash = createHash("sha256");
    const stream = createReadStream(path);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.once("error", rejectIdentity);
    stream.once("end", () => resolveIdentity({ path, sha256: hash.digest("hex") }));
  });
}

function onceConnected(socket, timeoutMs) {
  return new Promise((resolveConnect, rejectConnect) => {
    const timer = setTimeout(() => {
      socket.destroy();
      rejectConnect(new Error("connection timeout"));
    }, timeoutMs);
    socket.once("connect", () => {
      clearTimeout(timer);
      resolveConnect();
    });
    socket.once("error", (error) => {
      clearTimeout(timer);
      rejectConnect(error);
    });
  });
}

function listen(server, port) {
  return new Promise((resolveListen, rejectListen) => {
    server.once("error", rejectListen);
    server.listen(port, "127.0.0.1", () => {
      server.off("error", rejectListen);
      resolveListen();
    });
  });
}

async function closeServer(server, sockets = new Set()) {
  for (const socket of sockets) {
    socket.destroy();
  }
  if (!server.listening) {
    return;
  }
  await new Promise((resolveClose) => server.close(resolveClose));
}

async function waitForHttp(url, timeoutMs) {
  await waitFor(async () => {
    try {
      return (await fetch(url, { headers: { connection: "close" } })).ok;
    } catch {
      return false;
    }
  }, timeoutMs, url);
}

async function waitFor(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await sleep(50);
  }
  throw new Error(`timed out waiting for ${label}`);
}

function onceExit(child, timeoutMs) {
  if (!child || child.exitCode !== null) {
    return Promise.resolve();
  }
  return new Promise((resolveExit) => {
    const timer = setTimeout(resolveExit, timeoutMs);
    child.once("exit", () => {
      clearTimeout(timer);
      resolveExit();
    });
  });
}

function countValues(values) {
  const counts = new Map();
  for (const value of values) {
    counts.set(value, (counts.get(value) ?? 0) + 1);
  }
  return counts;
}

function normalizeEvent(event) {
  return String(event ?? "").replace(/^(?:pusher|sockudo)(?:_internal)?:/, "");
}

function parseJson(value) {
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function parseArgs(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const arg = values[index];
    if (!arg.startsWith("--")) {
      fail(`unexpected argument: ${arg}`);
    }
    const [rawKey, inlineValue] = arg.slice(2).split("=", 2);
    const key = rawKey.replace(/-([a-z])/g, (_, character) => character.toUpperCase());
    if (inlineValue !== undefined) {
      parsed[key] = inlineValue;
    } else if (values[index + 1] && !values[index + 1].startsWith("--")) {
      parsed[key] = values[index + 1];
      index += 1;
    } else {
      parsed[key] = "true";
    }
  }
  return parsed;
}

function numberArg(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    fail(`invalid numeric argument: ${value}`);
  }
  return parsed;
}

function boolArg(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function randomSeed() {
  return Math.floor(Math.random() * 0xffff_ffff);
}

function shellQuote(value) {
  if (/^[A-Za-z0-9_./:=+-]+$/.test(value)) {
    return value;
  }
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function fail(message) {
  console.error(`distributed-correctness: ${message}`);
  process.exit(2);
}

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, async () => {
    failures.push({ type: "interrupted", signal });
    await cleanup();
    await writeArtifact(false).catch(() => {});
    process.exit(130);
  });
}

await main()
  .catch(async (error) => {
    failures.push({ type: "runner_error", message: error?.stack ?? String(error) });
    await writeArtifact(false).catch(() => {});
    console.error(error?.stack ?? error);
    process.exitCode = 1;
  })
  .finally(cleanup);
