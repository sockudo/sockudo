#!/usr/bin/env node

import crypto from "node:crypto";
import { execFile, spawn } from "node:child_process";
import fs from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { promisify } from "node:util";

import {
  BoundedLatencySamples,
  parseDeliveryMetadata,
  StreamingDeliveryAudit,
} from "./capacity-evidence.mjs";

const exec = promisify(execFile);
const args = parseArgs(process.argv.slice(2));
const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../../..");
const profilePath = path.resolve(root, args.profile ?? "tests/load/ably-compat/profiles/smoke.json");
const profile = JSON.parse(await fs.readFile(profilePath, "utf8"));
const budgets = JSON.parse(await fs.readFile(path.join(root, "tests/load/ably-compat/budgets.json"), "utf8"));
const execute = booleanArg(args.execute, false);
const binary = path.resolve(root, args.binary ?? "target/release/sockudo");
const runId = args.runId ?? `${Date.now().toString(36)}-${process.pid.toString(36)}`;
const payloadLogSentinel = `sockudo-capacity-payload-${crypto.randomBytes(16).toString("hex")}`;
const artifactDir = path.resolve(root, args.artifactDir ?? `target/ably-compat-load/${runId}`);
const selected = args.scenario
  ? profile.scenarios.filter((scenario) => scenario.name === args.scenario)
  : profile.scenarios;
const topologies = args.topology ? [args.topology] : profile.topologies;

const plan = {
  schema: "sockudo.ably-compat.capacity-plan.v1",
  profile: profile.name,
  profilePath,
  binary,
  topologies,
  scenarios: selected,
};

async function main() {
  if (selected.length === 0) {
    throw new Error(`profile has no scenario named ${args.scenario}`);
  }
  if (topologies.some((topology) => !["one", "two"].includes(topology))) {
    throw new Error("topology must be one or two");
  }
  if (!execute) {
    process.stdout.write(`${JSON.stringify({ status: "planned", plan }, null, 2)}\n`);
    return;
  }

  await fs.access(binary, fs.constants.X_OK);
  await fs.mkdir(artifactDir, { recursive: true });
  const results = [];
  let managedServices = false;
  try {
    if (topologies.includes("two") && booleanArg(args.manageServices, true)) {
      await run("docker", ["compose", "-f", "docker-compose.test.yml", "up", "-d", "redis-test", "postgres-test"], root);
      managedServices = true;
      await Promise.all([waitForTcp(16379, 30000), waitForTcp(15432, 30000)]);
    }
    for (const topology of topologies) {
      results.push(await runTopology(topology));
    }
  } finally {
    if (managedServices && booleanArg(args.stopServices, false)) {
      await run("docker", ["compose", "-f", "docker-compose.test.yml", "stop", "redis-test", "postgres-test"], root).catch(() => {});
    }
  }

  const result = {
    schema: "sockudo.ably-compat.capacity-result.v1",
    status: results.every((entry) => entry.status === "passed") ? "passed" : "failed",
    generatedAt: new Date().toISOString(),
    runId,
    profile: profile.name,
    source: await sourceMetadata(),
    harness: await harnessMetadata(),
    host: await hostMetadata(),
    plan,
    topologies: results,
  };
  const output = `${JSON.stringify(result, null, 2)}\n`;
  const outputPath = path.resolve(root, args.output ?? path.join(artifactDir, "result.json"));
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, output);
  process.stdout.write(output);
  if (result.status !== "passed") process.exitCode = 1;
}

async function runTopology(topology) {
  const nodeCount = topology === "two" ? 2 : 1;
  const basePort = numberArg(args.basePort, topology === "two" ? 6711 : 6701);
  const metricsBasePort = numberArg(args.metricsBasePort, topology === "two" ? 9711 : 9701);
  const nodes = [];
  const configBytes = await fs.readFile(path.join(root, "config/config.toml"));
  const topologyDir = path.join(artifactDir, topology);
  await fs.mkdir(topologyDir, { recursive: true });
  const sampler = { stopped: false, phase: "startup", samples: [] };
  try {
    for (let index = 0; index < nodeCount; index += 1) {
      nodes.push(await startNode(topology, index, basePort + index, metricsBasePort + index, topologyDir));
    }
    const readiness = await topologyReadiness(topology, nodes);
    const sampling = sampleProcesses(nodes, sampler, profile.sampleIntervalMs ?? 1000);
    const scenarios = [];
    for (const scenario of selected) {
      sampler.phase = scenario.name;
      const before = await runtimeSnapshots(nodes);
      const started = performance.now();
      let outcome;
      try {
        outcome = await runScenario(scenario, nodes);
      } catch (error) {
        outcome = { status: "failed", error: error.message };
      }
      const after = await runtimeSnapshots(nodes);
      scenarios.push({
        ...outcome,
        name: scenario.name,
        configured: scenario,
        durationSeconds: secondsSince(started),
        runtimeBefore: before,
        runtimeAfter: after,
        runtimeDelta: snapshotDelta(before, after),
      });
    }
    sampler.phase = "post_disconnect";
    await sleep(profile.postDisconnectSettleMs ?? 5000);
    sampler.stopped = true;
    await sampling;
    const plateau = rssPlateau(sampler.samples, nodeCount);
    const shutdown = await shutdownNodes(nodes);
    const status = scenarios.every((scenario) => scenario.status === "passed")
      && plateau.stalledPeersPlateau
      && plateau.postDisconnectPlateau
      && shutdown.every((entry) => entry.continuityExplicit && !entry.sensitiveDataLogged);
    return {
      topology,
      status: status ? "passed" : "failed",
      config: {
        path: "config/config.toml",
        sha256: sha256(configBytes),
        runtimeOverrides: redactedOverrides(nodes),
      },
      nodes: nodes.map(publicNode),
      readiness,
      scenarios,
      resources: { samples: sampler.samples, plateau },
      shutdown,
    };
  } catch (error) {
    sampler.stopped = true;
    await shutdownNodes(nodes);
    return { topology, status: "failed", error: error.message };
  }
}

async function topologyReadiness(topology, nodes) {
  if (topology !== "two") return { crossNodeFanout: "not-applicable" };
  const channel = `capacity:${runId}:readiness`;
  const subscriber = await Subscriber.connect(nodes[0], channel);
  const started = performance.now();
  try {
    for (let attempt = 1; attempt <= 100; attempt += 1) {
      await ablyRequest(nodes[1], "POST", `/channels/${encodeURIComponent(channel)}/messages`, {
        id: `${runId}:readiness:${attempt}`,
        name: "capacity",
        data: { runId, sequence: attempt, sentAt: Date.now() },
      });
      await sleep(100);
      if (subscriber.deliveryAudit.received > 0) {
        return {
          crossNodeFanout: "ready",
          attempts: attempt,
          durationMs: Number((performance.now() - started).toFixed(3)),
        };
      }
    }
    throw new Error("cross-node fanout did not become ready within 10 seconds");
  } finally {
    await subscriber.close();
  }
}

async function startNode(topology, index, port, metricsPort, topologyDir) {
  const env = {
    ...process.env,
    HOST: "127.0.0.1",
    PORT: String(port),
    METRICS_PORT: String(metricsPort),
    INSTANCE_PROCESS_ID: `ably-capacity-${runId}-${topology}-${index}`,
    SOCKUDO_ABLY_COMPAT_ENABLED: "true",
    SOCKUDO_DEFAULT_APP_ID: "app-id",
    SOCKUDO_DEFAULT_APP_KEY: "app-key",
    SOCKUDO_DEFAULT_APP_SECRET: "capacity-secret",
    SOCKUDO_DEFAULT_APP_ENABLED: "true",
    SOCKUDO_DEFAULT_APP_MAX_CONNECTIONS: "20000",
    SOCKUDO_DEFAULT_APP_MAX_EVENT_PAYLOAD_IN_KB: "100",
    SOCKUDO_DEFAULT_APP_ENABLE_CLIENT_MESSAGES: "true",
    PUSH_ALLOW_MEMORY_DRIVERS: "true",
    RUST_LOG: args.rustLog ?? "warn",
  };
  if (topology === "two") {
    Object.assign(env, {
      ADAPTER_DRIVER: "redis",
      CACHE_DRIVER: "redis",
      QUEUE_DRIVER: "redis",
      DATABASE_REDIS_HOST: "127.0.0.1",
      DATABASE_REDIS_PORT: "16379",
      DATABASE_REDIS_DB: "0",
      DATABASE_REDIS_KEY_PREFIX: `sockudo-ably-capacity-${runId}:`,
      HISTORY_BACKEND: "postgres",
      VERSIONED_MESSAGES_DRIVER: "postgres",
      HISTORY_POSTGRES_TABLE_PREFIX: `capacity_${runId.replace(/[^A-Za-z0-9_]/g, "_")}`,
      DATABASE_POSTGRES_HOST: "127.0.0.1",
      DATABASE_POSTGRES_PORT: "15432",
      DATABASE_POSTGRES_USERNAME: "postgres",
      DATABASE_POSTGRES_PASSWORD: "postgres123",
      DATABASE_POSTGRES_DATABASE: "sockudo_test",
    });
  } else {
    Object.assign(env, { ADAPTER_DRIVER: "local", CACHE_DRIVER: "memory", QUEUE_DRIVER: "memory", HISTORY_BACKEND: "memory", VERSIONED_MESSAGES_DRIVER: "memory" });
  }
  const logPath = path.join(topologyDir, `node-${index + 1}.log`);
  const logHandle = await fs.open(logPath, "w");
  const child = spawn(binary, ["--config", path.join(root, "config/config.toml")], {
    cwd: root,
    env,
    stdio: ["ignore", logHandle.fd, logHandle.fd],
  });
  const node = { index, port, metricsPort, baseUrl: `http://127.0.0.1:${port}`, wsUrl: `ws://127.0.0.1:${port}`, child, env, logPath, logHandle };
  await waitForUrl(`${node.baseUrl}/up/app-id`, 60000);
  return node;
}

async function runScenario(scenario, nodes) {
  if (["steady_publish", "burst", "payload_64k", "encrypted_binary", "fanout_1000", "slow_consumers"].includes(scenario.name)) {
    return broadcastScenario(scenario, nodes);
  }
  if (scenario.name === "recovery_storm") return recoveryScenario(scenario, nodes);
  if (scenario.name === "presence_churn") return presenceScenario(scenario, nodes);
  if (scenario.name === "append_storm") return appendScenario(scenario, nodes);
  if (scenario.name === "stats_flush") return statsScenario(scenario, nodes);
  if (scenario.name === "push_enqueue") return pushScenario(scenario, nodes);
  throw new Error(`unsupported scenario ${scenario.name}`);
}

async function broadcastScenario(scenario, nodes) {
  const channel = `capacity:${runId}:${scenario.name}`;
  const slowCount = Math.floor(scenario.subscribers * (scenario.slowPercent ?? 0) / 100);
  const normalCount = scenario.subscribers - slowCount;
  const deliveryLatencies = new BoundedLatencySamples(profile.maxLatencySamples ?? 100_000);
  const clients = await openSubscribers(nodes[0], channel, normalCount, {
    expected: scenario.messages,
    deliveryLatencies,
  });
  const slow = await openPausedPeers(nodes[0], channel, slowCount);
  const deliveryBefore = await runtimeSnapshots(nodes);
  const publishNode = nodes.at(-1);
  const publishLatencies = [];
  const failureReasons = [];
  const started = performance.now();
  let failures = 0;
  const messages = Array.from({ length: scenario.messages }, (_, sequence) => sequence + 1);
  const rate = scenario.ratePerSecond ?? 0;
  const concurrency = scenario.concurrency ?? 1;
  for (let offset = 0; offset < messages.length; offset += concurrency) {
    if (rate > 0) {
      const target = started + offset * 1000 / rate;
      if (target > performance.now()) await sleep(target - performance.now());
    }
    await Promise.all(messages.slice(offset, offset + concurrency).map(async (sequence) => {
      const sentAt = Date.now();
      const data = payloadFor(scenario, sequence, sentAt);
      const requestStarted = performance.now();
      try {
        await ablyRequest(publishNode, "POST", `/channels/${encodeURIComponent(channel)}/messages`, {
          id: `${runId}:${scenario.name}:${sequence}`,
          name: "capacity",
          data,
          ...(scenario.name === "encrypted_binary" ? { encoding: "cipher+aes-256-cbc" } : {}),
        });
        publishLatencies.push(performance.now() - requestStarted);
      } catch (error) {
        failures += 1;
        if (failureReasons.length < 10) failureReasons.push(error.message);
      }
    }));
  }
  const expected = messages.length;
  await waitUntil(() => clients.every((client) => client.deliveryAudit.received >= expected), profile.settleTimeoutMs ?? 30000).catch(() => {});
  const audit = auditClients(clients, expected);
  const deliveryAfter = await runtimeSnapshots(nodes);
  const deliveryDelta = snapshotDelta(deliveryBefore, deliveryAfter);
  const encodeCount = deliveryDelta.reduce((sum, entry) => sum + (entry.delivery?.data_encoded ?? 0), 0);
  const expectedEncodeCount = normalCount > 0 || slowCount > 0 ? messages.length : 0;
  if (slowCount > 0) await sleep(scenario.stallHoldMs ?? 30000);
  await Promise.all(clients.map((client) => client.close()));
  for (const peer of slow) peer.destroy();
  const duration = secondsSince(started);
  return {
    status: failures === 0 && audit.loss === 0 && audit.duplicates === 0 && audit.reordered === 0 && audit.unexpected === 0 && encodeCount === expectedEncodeCount ? "passed" : "failed",
    attempted: messages.length,
    failures,
    failureReasons: [...new Set(failureReasons)],
    subscribers: { normal: normalCount, stalled: slowCount },
    throughputPerSecond: Number((messages.length / duration).toFixed(3)),
    deliveredPerSecond: Number((audit.received / duration).toFixed(3)),
    publishLatencyMs: summarize(publishLatencies),
    deliveryLatencyMs: deliveryLatencies.summary(),
    correctness: audit,
    encoding: { activeFormats: 1, encodeCount, expectedEncodeCount, exactlyOncePerFormat: encodeCount === expectedEncodeCount },
  };
}

async function recoveryScenario(scenario, nodes) {
  const channel = `capacity:${runId}:recovery`;
  const original = await openSubscribers(nodes[0], channel, scenario.connections);
  for (let sequence = 1; sequence <= scenario.messages; sequence += 1) {
    await ablyRequest(nodes[0], "POST", `/channels/${encodeURIComponent(channel)}/messages`, {
      id: `${runId}:recovery:before:${sequence}`,
      name: "capacity",
      data: { runId, sequence: -sequence, sentAt: Date.now() },
    });
  }
  await waitUntil(() => original.every((client) => client.deliveryAudit.received >= scenario.messages), profile.settleTimeoutMs ?? 30000);
  const sessions = original.map((client) => ({
    connectionKey: client.connectionKey,
    connectionId: client.connectionId,
    channelSerial: client.deliveryAudit.lastChannelSerial,
  }));
  await Promise.all(original.map((client) => client.close()));
  await sleep(50);
  for (let sequence = 1; sequence <= scenario.messages; sequence += 1) {
    await ablyRequest(nodes[0], "POST", `/channels/${encodeURIComponent(channel)}/messages`, {
      id: `${runId}:recovery:gap:${sequence}`,
      name: "capacity",
      data: { runId, sequence, sentAt: Date.now() },
    });
  }
  const replayBefore = await runtimeSnapshots(nodes);
  const recovered = [];
  for (let offset = 0; offset < sessions.length; offset += 100) {
    recovered.push(...await Promise.all(sessions.slice(offset, offset + 100).map((session, index) => Subscriber.connect(
      nodes[(offset + index) % nodes.length],
      channel,
      session.connectionKey,
      session.connectionId,
      { flags: 4, channelSerial: session.channelSerial },
      { expected: scenario.messages },
    ))));
  }
  await waitUntil(() => recovered.every((client) => client.deliveryAudit.received >= scenario.messages), profile.settleTimeoutMs ?? 30000).catch(() => {});
  const resumed = recovered.filter((client) => client.resumed).length;
  const audit = auditClients(recovered, scenario.messages);
  const replayAfter = await runtimeSnapshots(nodes);
  const replaySource = snapshotDelta(replayBefore, replayAfter).reduce((sum, entry) => sum + (entry.delivery?.replay_source ?? 0), 0);
  const backendCalls = snapshotDelta(replayBefore, replayAfter).reduce((sum, entry) => sum + (entry.delivery?.recovery_backend_calls ?? 0), 0);
  await Promise.all(recovered.map((client) => client.close()));
  return {
    status: resumed === recovered.length && audit.loss === 0 && audit.duplicates === 0 && audit.reordered === 0 && audit.unexpected === 0 && replaySource >= recovered.length && backendCalls > 0 && backendCalls <= recovered.length * 3 ? "passed" : "failed",
    attempted: sessions.length,
    resumed,
    failures: sessions.length - resumed,
    replaySource,
    backendCalls,
    backendCallBudget: recovered.length * 3,
    correctness: audit,
  };
}

async function presenceScenario(scenario, nodes) {
  const channel = `capacity:${runId}:presence`;
  const client = await Subscriber.connect(nodes[0], channel);
  const started = performance.now();
  let sendFailed = false;
  for (let index = 0; index < scenario.transitions; index += 1) {
    try {
      client.send({ action: 14, channel, msgSerial: index, count: 1, presence: [{ action: index % 2 === 0 ? 2 : 3, clientId: `member-${index % 10}`, data: { index } }] });
    } catch {
      sendFailed = true;
      break;
    }
    if ((index + 1) % 128 === 0) {
      const acknowledged = await waitUntil(
        () => client.ackedSerial >= index,
        profile.settleTimeoutMs ?? 30000,
      ).then(() => true, () => false);
      if (!acknowledged) {
        sendFailed = true;
        break;
      }
    }
  }
  await waitUntil(() => client.ackedSerial >= scenario.transitions - 1, profile.settleTimeoutMs ?? 30000).catch(() => {});
  const acknowledged = Math.min(scenario.transitions, client.ackedSerial + 1);
  await client.close();
  return {
    status: !sendFailed && acknowledged === scenario.transitions ? "passed" : "failed",
    transitions: scenario.transitions,
    acknowledged,
    throughputPerSecond: scenario.transitions / secondsSince(started),
    correctness: { ...zeroAudit(), loss: scenario.transitions - acknowledged },
  };
}

async function appendScenario(scenario, nodes) {
  const streams = [];
  let failures = 0;
  const latencies = [];
  for (let index = 0; index < scenario.streams; index += 1) {
    const channel = `private-ai-capacity-${runId}-${index}`;
    try {
      const created = await signedNative(nodes[index % nodes.length], "POST", "/events", { name: "ai-output", channel, data: "", extras: { ai: { transport: { status: "streaming", role: "assistant" } } } });
      streams.push({ channel, serial: created.channels[channel].message_serial, expected: "" });
    } catch { failures += 1; }
  }
  for (let append = 0; append < scenario.appendsPerStream; append += 1) {
    for (let offset = 0; offset < streams.length; offset += scenario.concurrency ?? 1) {
      await Promise.all(streams.slice(offset, offset + (scenario.concurrency ?? 1)).map(async (stream, local) => {
        const fragment = `${append.toString(36)}.`;
        stream.expected += fragment;
        const requestStarted = performance.now();
        try {
          await signedNative(nodes[(offset + local + append) % nodes.length], "POST", `/channels/${encodeURIComponent(stream.channel)}/messages/${encodeURIComponent(stream.serial)}/append`, { data: fragment, op_id: `${runId}-${offset + local}-${append}`, extras: { ai: { transport: { status: append + 1 === scenario.appendsPerStream ? "complete" : "streaming" } } } });
          latencies.push(performance.now() - requestStarted);
        } catch { failures += 1; }
      }));
    }
  }
  let mismatches = 0;
  for (const stream of streams.slice(0, 100)) {
    const latest = await signedNative(nodes[0], "GET", `/channels/${encodeURIComponent(stream.channel)}/messages/${encodeURIComponent(stream.serial)}`);
    if (latest?.item?.data !== stream.expected) mismatches += 1;
  }
  return { status: failures === 0 && mismatches === 0 ? "passed" : "failed", attempted: streams.length * scenario.appendsPerStream, failures, appendLatencyMs: summarize(latencies), correctness: { ...zeroAudit(), gaps: mismatches } };
}

async function statsScenario(scenario, nodes) {
  const channel = `capacity:${runId}:stats`;
  for (let index = 0; index < scenario.observations; index += 1) {
    await ablyRequest(nodes[index % nodes.length], "POST", `/channels/${encodeURIComponent(channel)}/messages`, { name: "stats", data: { index } });
  }
  await sleep(100);
  const page = await ablyRequest(nodes[0], "GET", "/stats?unit=minute&limit=10");
  return { status: Array.isArray(page) && page.length > 0 ? "passed" : "failed", observations: scenario.observations, intervals: Array.isArray(page) ? page.length : 0, correctness: zeroAudit() };
}

async function pushScenario(scenario, nodes) {
  let failures = 0;
  const latencies = [];
  for (let index = 0; index < scenario.publishes; index += 1) {
    const started = performance.now();
    try {
      await ablyRequest(nodes[0], "POST", "/push/publish", { recipient: { transportType: "ablyChannel", channel: `capacity:${runId}:push` }, notification: { title: "capacity", body: "redacted" }, data: { sequence: index } }, { "idempotency-key": `${runId}-push-${index}` });
      latencies.push(performance.now() - started);
    } catch { failures += 1; }
  }
  return { status: failures === 0 ? "passed" : "failed", attempted: scenario.publishes, failures, enqueueLatencyMs: summarize(latencies), correctness: zeroAudit() };
}

class Subscriber {
  constructor(ws, { expected = 0, deliveryLatencies = null } = {}) {
    this.ws = ws;
    this.deliveryAudit = new StreamingDeliveryAudit(expected);
    this.deliveryLatencies = deliveryLatencies;
    this.frames = [];
    this.waiters = [];
    this.ackedSerial = -1;
    this.messageListener = (event) => this.onMessage(event);
    ws.addEventListener("message", this.messageListener);
  }
  static async connect(node, channel, recover, expectedConnectionId, attach = {}, evidence = {}) {
    const url = realtimeUrl(node, recover);
    const ws = new WebSocket(url);
    const client = new Subscriber(ws, evidence);
    let stage = "opening";
    try {
      await webSocketOpen(ws, 15000);
      stage = "waiting for CONNECTED";
      const connected = await client.waitFor((frame) => frame.action === 4, 15000);
      client.connectionKey = connected.connectionDetails?.connectionKey;
      client.connectionId = connected.connectionId;
      client.resumed = expectedConnectionId === undefined || connected.connectionId === expectedConnectionId;
      stage = "waiting for ATTACHED";
      client.send({ action: 10, channel, ...attach });
      await client.waitFor((frame) => frame.action === 11 && frame.channel === channel, 15000);
      return client;
    } catch (error) {
      await client.close();
      throw new Error(`${stage}: ${error.message}`);
    }
  }
  onMessage(event) {
    const text = typeof event.data === "string" ? event.data : Buffer.from(event.data).toString("utf8");
    const deliveries = parseDeliveryMetadata(text);
    if (deliveries !== null) {
      for (const delivery of deliveries) {
        this.deliveryAudit.record(delivery);
        this.deliveryLatencies?.add(delivery.receivedAt - delivery.sentAt);
      }
      return;
    }

    let frame;
    try { frame = JSON.parse(text); } catch { return; }
    if (frame.action === 1 && Number.isFinite(Number(frame.msgSerial))) {
      this.ackedSerial = Math.max(this.ackedSerial, Number(frame.msgSerial) + Math.max(1, Number(frame.count) || 1) - 1);
    }
    this.frames.push(frame);
    if (this.frames.length > 64) this.frames.shift();
    const pending = this.waiters;
    this.waiters = [];
    for (const waiter of pending) waiter.predicate(frame) ? waiter.resolve(frame) : this.waiters.push(waiter);
  }
  waitFor(predicate, timeoutMs) {
    const existing = this.frames.find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const waiter = {
        predicate,
        resolve: (value) => { clearTimeout(waiter.timer); resolve(value); },
        reject,
        timer: null,
      };
      waiter.timer = setTimeout(() => {
        this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
        reject(new Error("WebSocket frame timeout"));
      }, timeoutMs);
      this.waiters.push(waiter);
    });
  }
  send(value) { this.ws.send(JSON.stringify(value)); }
  async close() {
    this.ws.removeEventListener("message", this.messageListener);
    this.frames.length = 0;
    for (const waiter of this.waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(new Error("WebSocket closed while waiting for a frame"));
    }
    this.waiters.length = 0;
    if (this.ws.readyState === WebSocket.CLOSED) return;
    const closed = eventOnce(this.ws, "close", 5000).catch(() => undefined);
    try { this.ws.close(); } catch {}
    await closed;
  }
}

async function openSubscribers(node, channel, count, evidence = {}) {
  const clients = [];
  for (let offset = 0; offset < count; offset += 100) {
    const settled = await Promise.allSettled(Array.from(
      { length: Math.min(100, count - offset) },
      () => Subscriber.connect(node, channel, undefined, undefined, {}, evidence),
    ));
    const opened = settled.filter((result) => result.status === "fulfilled").map((result) => result.value);
    clients.push(...opened);
    const failure = settled.find((result) => result.status === "rejected");
    if (failure) {
      await Promise.all(clients.map((client) => client.close()));
      throw failure.reason;
    }
  }
  return clients;
}

async function openPausedPeers(node, channel, count) {
  const peers = [];
  for (let index = 0; index < count; index += 1) peers.push(await pausedPeer(node, channel));
  return peers;
}

async function pausedPeer(node, channel) {
  const socket = net.createConnection({ host: "127.0.0.1", port: node.port });
  await eventOnce(socket, "connect", 10000);
  const key = crypto.randomBytes(16).toString("base64");
  const request = `GET ${new URL(realtimeUrl(node)).pathname}${new URL(realtimeUrl(node)).search} HTTP/1.1\r\nHost: 127.0.0.1:${node.port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`;
  socket.write(request);
  let headers = "";
  while (!headers.includes("\r\n\r\n")) headers += (await eventOnce(socket, "data", 10000)).toString("binary");
  if (!headers.startsWith("HTTP/1.1 101")) throw new Error("stalled peer upgrade failed");
  socket.write(maskedTextFrame(JSON.stringify({ action: 10, channel })));
  await sleep(25);
  socket.pause();
  return socket;
}

function maskedTextFrame(text) {
  const payload = Buffer.from(text);
  const mask = crypto.randomBytes(4);
  const head = payload.length < 126 ? Buffer.from([0x81, 0x80 | payload.length]) : Buffer.from([0x81, 0xfe, payload.length >> 8, payload.length & 0xff]);
  const masked = Buffer.allocUnsafe(payload.length);
  for (let index = 0; index < payload.length; index += 1) masked[index] = payload[index] ^ mask[index % 4];
  return Buffer.concat([head, mask, masked]);
}

function realtimeUrl(node, recover) {
  const url = new URL(`${node.wsUrl}/`);
  url.searchParams.set("key", "app-key:capacity-secret");
  url.searchParams.set("format", "json");
  url.searchParams.set("v", "2.0");
  if (recover) url.searchParams.set("recover", recover);
  return url.toString();
}

async function ablyRequest(node, method, requestPath, body, extraHeaders = {}) {
  const response = await fetch(`${node.baseUrl}${requestPath}`, { method, headers: { authorization: `Basic ${Buffer.from("app-key:capacity-secret").toString("base64")}`, "content-type": "application/json", ...extraHeaders }, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await response.text();
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${text.slice(0, 256)}`);
  return text ? JSON.parse(text) : {};
}

async function signedNative(node, method, requestPath, body) {
  const fullPath = `/apps/app-id${requestPath}`;
  const bodyText = body === undefined ? undefined : JSON.stringify(body);
  const query = { auth_key: "app-key", auth_timestamp: String(Math.floor(Date.now() / 1000)), auth_version: "1.0" };
  if (bodyText !== undefined) query.body_md5 = crypto.createHash("md5").update(bodyText).digest("hex");
  const canonical = Object.keys(query).sort().map((key) => `${key}=${query[key]}`).join("&");
  query.auth_signature = crypto.createHmac("sha256", "capacity-secret").update(`${method}\n${fullPath}\n${canonical}`).digest("hex");
  const url = new URL(`${node.baseUrl}${fullPath}`);
  for (const [key, value] of Object.entries(query)) url.searchParams.set(key, value);
  const response = await fetch(url, { method, headers: { "content-type": "application/json" }, body: bodyText });
  const text = await response.text();
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${text.slice(0, 256)}`);
  return text ? JSON.parse(text) : {};
}

async function runtimeSnapshots(nodes) {
  return Promise.all(nodes.map(async (node) => {
    const [delivery, aggregation, prometheus] = await Promise.all([
      fetchJson(`${node.baseUrl}/operator/stats/ably-runtime`).catch(() => null),
      fetchJson(`${node.baseUrl}/operator/stats/aggregation`).catch(() => null),
      fetch(`http://127.0.0.1:${node.metricsPort}/metrics`).then((response) => response.text()).catch(() => null),
    ]);
    return { node: node.index + 1, delivery, aggregation, prometheus: prometheus && extractMetrics(prometheus) };
  }));
}

function snapshotDelta(before, after) {
  return after.map((current, index) => ({
    node: current.node,
    delivery: numericDelta(before[index]?.delivery, current.delivery),
    aggregation: numericDelta(before[index]?.aggregation, current.aggregation),
  }));
}

function numericDelta(before, after) {
  if (!before || !after) return null;
  return Object.fromEntries(Object.entries(after).filter(([, value]) => typeof value === "number").map(([key, value]) => [key, value - (before[key] ?? 0)]));
}

function extractMetrics(text) {
  const wanted = /queue|history|recovery|duplicate|gap|disconnect|encode|backend|rollup|push|allocation/i;
  return text.split("\n").filter((line) => !line.startsWith("#") && wanted.test(line)).slice(0, 500);
}

async function sampleProcesses(nodes, state, intervalMs) {
  while (!state.stopped) {
    const atMs = Date.now();
    for (const node of nodes) {
      try {
        const { stdout } = await exec("ps", ["-o", "rss=,%cpu=", "-p", String(node.child.pid)]);
        const [rssKiB, cpuPercent] = stdout.trim().split(/\s+/).map(Number);
        state.samples.push({
          atMs,
          phase: state.phase,
          node: node.index + 1,
          rssBytes: rssKiB * 1024,
          heapBytes: await linuxDataBytes(node.child.pid),
          cpuPercent,
          allocationBytes: null,
        });
      } catch {}
    }
    await sleep(intervalMs);
  }
}

function rssPlateau(samples, nodeCount) {
  const stalledPeersExercised = selected.some((scenario) => scenario.name === "slow_consumers" && (scenario.slowPercent ?? 0) > 0);
  const byNode = Array.from({ length: nodeCount }, (_, index) => samples.filter((sample) => sample.node === index + 1));
  const nodes = byNode.map((nodeSamples) => {
    const stalled = nodeSamples.filter((sample) => sample.phase === "slow_consumers");
    const postDisconnect = nodeSamples.filter((sample) => sample.phase === "post_disconnect");
    const stalledSlope = linearSlope(stalled.map((sample) => [sample.atMs / 1000, sample.rssBytes]));
    const postDisconnectSlope = linearSlope(postDisconnect.map((sample) => [sample.atMs / 1000, sample.rssBytes]));
    const peak = Math.max(0, ...nodeSamples.map((sample) => sample.rssBytes));
    const final = postDisconnect.at(-1)?.rssBytes ?? 0;
    return {
      node: nodeSamples[0]?.node,
      peakRssBytes: peak,
      finalRssBytes: final,
      stalledSampleCount: stalled.length,
      postDisconnectSampleCount: postDisconnect.length,
      stalledSlopeBytesPerSecond: stalledSlope,
      postDisconnectSlopeBytesPerSecond: postDisconnectSlope,
      postDisconnectRatio: peak === 0 ? 0 : final / peak,
    };
  });
  const maxSlope = budgets.load.maxRssPlateauSlopeBytesPerSecond;
  return {
    nodes,
    stalledPeersExercised,
    stalledPeersPlateau: !stalledPeersExercised || nodes.every((node) => node.stalledSampleCount >= 3 && node.stalledSlopeBytesPerSecond <= maxSlope),
    postDisconnectPlateau: nodes.every((node) => node.postDisconnectSampleCount >= 3 && node.postDisconnectSlopeBytesPerSecond <= maxSlope && node.postDisconnectRatio <= budgets.load.maxPostDisconnectRssRatio),
  };
}

async function shutdownNodes(nodes) {
  return Promise.all(nodes.map(async (node) => {
    node.child.kill("SIGTERM");
    const exit = await childExit(node.child, 15000).catch(() => ({ code: null, signal: "timeout" }));
    if (exit.signal === "timeout") node.child.kill("SIGKILL");
    await node.logHandle.close();
    const log = await fs.readFile(node.logPath, "utf8").catch(() => "");
    const sensitiveDataLogged = [payloadLogSentinel, "capacity-secret", "app-key", "postgres123"]
      .some((value) => log.includes(value));
    return {
      node: node.index + 1,
      ...exit,
      continuityExplicit: exit.code === 0 || /continuity|drain|shutdown/i.test(log),
      sensitiveDataLogged,
      logPath: node.logPath,
    };
  }));
}

function auditClients(clients, expected) {
  let loss = 0, duplicates = 0, reordered = 0, unexpected = 0, received = 0;
  for (const client of clients) {
    const audit = client.deliveryAudit.result();
    received += audit.received;
    loss += audit.loss;
    duplicates += audit.duplicates;
    reordered += audit.reordered;
    unexpected += audit.unexpected;
  }
  return { subscribersAudited: clients.length, expectedPerSubscriber: expected, received, loss, duplicates, reordered, unexpected, gaps: loss };
}

function payloadFor(scenario, sequence, sentAt) {
  const base = {
    runId,
    sequence,
    sentAt,
    ...(sequence === 1 ? { logProbe: payloadLogSentinel } : {}),
  };
  const emptyMessage = { id: `${runId}:${scenario.name}:${sequence}`, name: "capacity", data: { ...base, filler: "" } };
  const target = Math.max(0, (scenario.payloadBytes ?? 256) - Buffer.byteLength(JSON.stringify(emptyMessage)) - 32);
  if (scenario.name === "encrypted_binary") return Buffer.from(JSON.stringify({ ...base, filler: "x".repeat(target) })).toString("base64");
  return { ...base, filler: "x".repeat(target) };
}

function zeroAudit() { return { loss: 0, duplicates: 0, reordered: 0, gaps: 0 }; }
function summarize(values) { if (!values.length) return { count: 0 }; const sorted = [...values].sort((a, b) => a - b); return { count: sorted.length, median: percentile(sorted, .5), p95: percentile(sorted, .95), p99: percentile(sorted, .99), max: Number(sorted.at(-1).toFixed(3)) }; }
function percentile(sorted, fraction) { return Number(sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))].toFixed(3)); }
function linearSlope(points) { if (points.length < 2) return 0; const meanX = points.reduce((sum, point) => sum + point[0], 0) / points.length; const meanY = points.reduce((sum, point) => sum + point[1], 0) / points.length; const numerator = points.reduce((sum, point) => sum + (point[0] - meanX) * (point[1] - meanY), 0); const denominator = points.reduce((sum, point) => sum + (point[0] - meanX) ** 2, 0); return denominator === 0 ? 0 : Number((numerator / denominator).toFixed(3)); }
async function fetchJson(url) { const response = await fetch(url); if (!response.ok) throw new Error(`HTTP ${response.status}`); return response.json(); }
async function waitForUrl(url, timeoutMs) { const deadline = Date.now() + timeoutMs; while (Date.now() < deadline) { try { const response = await fetch(url); if (response.ok) return; } catch {} await sleep(100); } throw new Error(`timeout waiting for ${url}`); }
async function waitForTcp(port, timeoutMs) { const deadline = Date.now() + timeoutMs; while (Date.now() < deadline) { const connected = await new Promise((resolve) => { const socket = net.createConnection({ host: "127.0.0.1", port }); socket.once("connect", () => { socket.destroy(); resolve(true); }); socket.once("error", () => resolve(false)); }); if (connected) return; await sleep(100); } throw new Error(`timeout waiting for TCP port ${port}`); }
async function waitUntil(predicate, timeoutMs) { const deadline = Date.now() + timeoutMs; while (Date.now() < deadline) { if (predicate()) return; await sleep(10); } throw new Error("condition timeout"); }
async function linuxDataBytes(pid) { if (os.platform() !== "linux") return null; const status = await fs.readFile(`/proc/${pid}/status`, "utf8").catch(() => ""); const match = /^VmData:\s+(\d+)\s+kB$/m.exec(status); return match ? Number(match[1]) * 1024 : null; }
function eventOnce(target, event, timeoutMs) { return new Promise((resolve, reject) => { const timer = setTimeout(() => reject(new Error(`${event} timeout`)), timeoutMs); const done = (value) => { clearTimeout(timer); resolve(value); }; target.addEventListener ? target.addEventListener(event, done, { once: true }) : target.once(event, done); }); }
function webSocketOpen(socket, timeoutMs) { return new Promise((resolve, reject) => { const timer = setTimeout(() => reject(new Error("WebSocket open timeout")), timeoutMs); const opened = () => { clearTimeout(timer); socket.removeEventListener("error", failed); resolve(); }; const failed = () => { clearTimeout(timer); socket.removeEventListener("open", opened); reject(new Error("WebSocket open failed")); }; socket.addEventListener("open", opened, { once: true }); socket.addEventListener("error", failed, { once: true }); }); }
function childExit(child, timeoutMs) { if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve({ code: child.exitCode, signal: child.signalCode }); return new Promise((resolve, reject) => { const timer = setTimeout(() => reject(new Error("timeout")), timeoutMs); child.once("exit", (code, signal) => { clearTimeout(timer); resolve({ code, signal }); }); }); }
async function run(command, argv, cwd) { await exec(command, argv, { cwd, maxBuffer: 10 * 1024 * 1024 }); }
async function sourceMetadata() { const [commit, status, binaryBytes] = await Promise.all([exec("git", ["rev-parse", "HEAD"], { cwd: root }).then((result) => result.stdout.trim()), exec("git", ["status", "--short"], { cwd: root }).then((result) => result.stdout), fs.readFile(binary)]); return { commit, dirty: status.length > 0, binarySha256: sha256(binaryBytes) }; }
async function harnessMetadata() {
  const files = [
    "tests/load/ably-compat/capacity-runner.mjs",
    "tests/load/ably-compat/capacity-evidence.mjs",
    path.relative(root, profilePath),
    "tests/load/ably-compat/budgets.json",
  ];
  return Object.fromEntries(await Promise.all(files.map(async (file) => [file, sha256(await fs.readFile(path.resolve(root, file)))])));
}
async function hostMetadata() { const versions = {}; for (const [name, command, argv] of [["node", "node", ["--version"]], ["rustc", "rustc", ["--version"]], ["cargo", "cargo", ["--version"]]]) { versions[name] = await exec(command, argv).then((result) => result.stdout.trim()).catch(() => null); } return { hostname: os.hostname(), platform: os.platform(), release: os.release(), architecture: os.arch(), cpus: os.cpus().map((cpu) => cpu.model), logicalCpuCount: os.cpus().length, totalMemoryBytes: os.totalmem(), versions }; }
function redactedOverrides(nodes) { return nodes.map((node) => Object.fromEntries(Object.entries(node.env).filter(([key]) => /^(HOST|PORT|METRICS_PORT|INSTANCE_PROCESS_ID|SOCKUDO_|ADAPTER_|CACHE_|QUEUE_|DATABASE_|HISTORY_|VERSIONED_|PUSH_)/.test(key)).map(([key, value]) => [key, /SECRET|PASSWORD|TOKEN|KEY$/.test(key) ? "<redacted>" : value]))); }
function publicNode(node) { return { node: node.index + 1, pid: node.child.pid, port: node.port, metricsPort: node.metricsPort, logPath: node.logPath }; }
function sha256(value) { return crypto.createHash("sha256").update(value).digest("hex"); }
function secondsSince(started) { return Number(((performance.now() - started) / 1000).toFixed(3)); }
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function numberArg(value, fallback) { if (value === undefined) return fallback; const number = Number(value); if (!Number.isFinite(number) || number < 0) throw new Error(`invalid number ${value}`); return number; }
function booleanArg(value, fallback) { if (value === undefined) return fallback; if (typeof value === "boolean") return value; if (["1", "true", "yes"].includes(String(value).toLowerCase())) return true; if (["0", "false", "no"].includes(String(value).toLowerCase())) return false; throw new Error(`invalid boolean ${value}`); }
function parseArgs(argv) { const parsed = {}; for (let index = 0; index < argv.length; index += 1) { const raw = argv[index]; if (!raw.startsWith("--")) throw new Error(`unexpected argument ${raw}`); const [name, inline] = raw.slice(2).split("=", 2); const key = name.replace(/-([a-z])/g, (_, character) => character.toUpperCase()); if (inline !== undefined) parsed[key] = inline; else if (argv[index + 1] && !argv[index + 1].startsWith("--")) parsed[key] = argv[++index]; else parsed[key] = true; } return parsed; }

await main();
