#!/usr/bin/env node
import { spawn } from "node:child_process";
import { createHash, createHmac } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { connect as connectTcp, createServer as createTcpServer } from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const DEFAULT_ARTIFACT_ROOT = join(ROOT_DIR, "target", "outside-in-chaos");
const APP_ID = "app-id";
const APP_KEY = "app-key";
const APP_SECRET = "app-secret";
const EVENT_NAME = "chaos.event";

const args = parseArgs(process.argv.slice(2));
const publicPort = numberArg(args.port, 6101);
const networkFaultMode = args.networkFaultMode ?? "proxy";
if (!["proxy", "publisher", "off"].includes(networkFaultMode)) {
  fail("--network-fault-mode must be proxy, publisher, or off");
}
const config = {
  seed: numberArg(args.seed, randomSeed()),
  durationMs: numberArg(args.durationMs, 12_000),
  settleMs: numberArg(args.settleMs, 2_000),
  publishIntervalMs: numberArg(args.publishIntervalMs, 250),
  clients: numberArg(args.clients, 2),
  clientConnectAttempts: numberArg(args.clientConnectAttempts, 4),
  clientConnectRetryMs: numberArg(args.clientConnectRetryMs, 500),
  clientMode: args.clientMode ?? "sdk-js",
  serverBin: resolve(ROOT_DIR, args.serverBin ?? "target/debug/sockudo"),
  artifactRoot: resolve(ROOT_DIR, args.artifactDir ?? DEFAULT_ARTIFACT_ROOT),
  port: publicPort,
  serverPort: numberArg(args.serverPort, networkFaultMode === "proxy" ? publicPort + 1000 : publicPort),
  metricsPort: numberArg(args.metricsPort, 9701),
  pushProviderPort: numberArg(args.pushProviderPort, 8791),
  killAtMs: numberArg(args.killAtMs, 3_500),
  restartAfterMs: numberArg(args.restartAfterMs, 900),
  clientDropAtMs: numberArg(args.clientDropAtMs, 5_500),
  clientReconnectAfterMs: numberArg(args.clientReconnectAfterMs, 900),
  configChangeAtMs: numberArg(args.configChangeAtMs, 8_000),
  networkFaultMode,
  networkDelayMs: numberArg(args.networkDelayMs, 80),
  networkDelayProbability: numberArg(args.networkDelayProbability, 0.20),
  networkDropProbability: numberArg(args.networkDropProbability, 0.05),
  networkDuplicateProbability: numberArg(args.networkDuplicateProbability, 0.05),
  strictDelivery: boolArg(args.strictDelivery, false),
  pushProviderProfile: args.pushProviderProfile ?? "off",
  exercisePush: boolArg(args.exercisePush, false),
  requirePushProviderHit: boolArg(
    args.requirePushProviderHit,
    boolArg(args.exercisePush, false) && (args.pushProviderProfile ?? "off") !== "off",
  ),
};

if (config.clients < 1) {
  fail("--clients must be at least 1");
}
if (config.clientConnectAttempts < 1) {
  fail("--client-connect-attempts must be at least 1");
}
if (config.networkFaultMode === "proxy" && config.serverPort === config.port) {
  fail("--server-port must differ from --port when --network-fault-mode proxy is enabled");
}

const runId = `${new Date().toISOString().replace(/[:.]/g, "-")}-seed-${config.seed}`;
const artifactDir = join(config.artifactRoot, runId);
const primaryConfigPath = join(artifactDir, "sockudo-chaos.toml");
const reloadConfigPath = join(artifactDir, "sockudo-chaos-restart.toml");
const artifactPath = join(artifactDir, "artifact.json");
const serverLogPath = join(artifactDir, "sockudo.log");
const providerLogPath = join(artifactDir, "push-provider.log");

const rng = mulberry32(config.seed);
const startedAt = new Date();
const faults = [];
const notes = [];
const failures = [];
const publishResults = [];
const receivedBySequence = new Map();
let activeServerConfigPath = primaryConfigPath;
let server = null;
let networkProxy = null;
let pushProvider = null;
let sdkClass = null;
let proxyConnectionId = 0;
let networkFaultsActive = false;
const activeProxySockets = new Set();

const counters = {
  processStarts: 0,
  processKills: 0,
  configRestarts: 0,
  clientReconnects: 0,
  publishAttempts: 0,
  publishAccepted: 0,
  publishFailed: 0,
  publishDroppedBeforeSend: 0,
  publishDuplicatesSent: 0,
  receivedEvents: 0,
  subscriptionSucceeded: 0,
  resumeSucceeded: 0,
  resumeFailed: 0,
  pushAttempts: 0,
  pushAccepted: 0,
  pushSkipped: 0,
  pushFailed: 0,
  pushProviderRequests: 0,
  networkProxyConnections: 0,
  networkProxyDelayedChunks: 0,
  networkProxyDroppedConnections: 0,
  networkProxyDroppedStreams: 0,
  networkProxyBytesClientToServer: 0,
  networkProxyBytesServerToClient: 0,
};

const publisherRng = mulberry32(config.seed ^ 0xa110_ca05);
const proxyRng = mulberry32(config.seed ^ 0xb17e_5eed);
const channelRng = mulberry32(config.seed ^ 0xc0de_fade);

async function main() {
  await mkdir(artifactDir, { recursive: true });
  await writeFile(primaryConfigPath, serverConfig({ pushEnabled: pushRequested(), activityTimeout: 8 }));
  await writeFile(reloadConfigPath, serverConfig({ pushEnabled: pushRequested(), activityTimeout: 5 }));

  if (config.clientMode === "sdk-js") {
    sdkClass = await loadSockudoSdk().catch((error) => {
      notes.push({
        type: "client_mode_fallback",
        requested: "sdk-js",
        fallback: "raw-v2",
        reason: error.message,
      });
      return null;
    });
  }

  if (config.pushProviderProfile !== "off") {
    pushProvider = startPushProvider();
    await waitForHttp(`http://127.0.0.1:${config.pushProviderPort}/health`, 10_000).catch(
      (error) => {
        notes.push({ type: "push_provider_unhealthy", message: error.message });
      },
    );
  }

  server = startServer(activeServerConfigPath);
  networkProxy = await startNetworkProxy();
  await waitForServer();

  const channelName = `chaos-${config.seed}-${seededHex(channelRng, 4)}`;
  const clients = [];
  for (let index = 0; index < config.clients; index += 1) {
    const client = createClient(`client-${index + 1}`, channelName);
    clients.push(client);
    await connectClientWithRetries(client, "initial_connect");
  }

  if (config.exercisePush) {
    await exercisePushPath(channelName);
  }
  enableNetworkFaults();

  const startMs = Date.now();
  const endMs = startMs + config.durationMs;
  let sequence = 0;
  let processRestarted = false;
  let configRestarted = false;
  let clientsReconnected = false;

  while (Date.now() < endMs) {
    const elapsed = Date.now() - startMs;
    if (!processRestarted && config.killAtMs >= 0 && elapsed >= config.killAtMs) {
      processRestarted = true;
      await killServer("process_kill");
      await sleep(config.restartAfterMs);
      server = startServer(activeServerConfigPath);
      await waitForServer();
      await reconnectClients(clients, "after_process_restart");
    }

    if (!clientsReconnected && config.clientDropAtMs >= 0 && elapsed >= config.clientDropAtMs) {
      clientsReconnected = true;
      recordFault("client_network_drop", { reconnectAfterMs: config.clientReconnectAfterMs });
      for (const client of clients) {
        client.disconnect();
      }
      await sleep(config.clientReconnectAfterMs);
      await reconnectClients(clients, "after_client_network_drop");
    }

    if (!configRestarted && config.configChangeAtMs >= 0 && elapsed >= config.configChangeAtMs) {
      configRestarted = true;
      recordFault("config_change_restart", {
        from: activeServerConfigPath,
        to: reloadConfigPath,
        note: "Sockudo config is loaded at process start; the harness applies config changes by restart.",
      });
      activeServerConfigPath = reloadConfigPath;
      counters.configRestarts += 1;
      await killServer("config_restart");
      server = startServer(activeServerConfigPath);
      await waitForServer();
      await reconnectClients(clients, "after_config_restart");
    }

    sequence += 1;
    await publishWithNetworkFaults(channelName, sequence);
    await sleep(config.publishIntervalMs);
  }

  await sleep(config.settleMs);

  for (const client of clients) {
    client.disconnect();
  }
  await cleanup();

  const acceptedSequences = publishResults
    .filter((result) => result.accepted)
    .map((result) => result.sequence);
  const missing = acceptedSequences.filter((item) => !receivedBySequence.has(item));
  if (config.strictDelivery && missing.length > 0) {
    failures.push({
      type: "strict_delivery_missing",
      missingSequences: missing,
      note: "Accepted publishes were not observed by any client before settle timeout.",
    });
  }
  if (counters.subscriptionSucceeded === 0) {
    failures.push({ type: "no_subscription_succeeded" });
  }
  if (counters.publishAccepted === 0) {
    failures.push({ type: "no_publish_accepted" });
  }

  const ok = failures.length === 0;
  await writeArtifact(ok, { channelName, acceptedSequences, missingSequences: missing });

  console.log(
    JSON.stringify(
      {
        ok,
        artifact: artifactPath,
        seed: config.seed,
        counters,
        failures,
      },
      null,
      2,
    ),
  );
  process.exit(ok ? 0 : 1);
}

function createClient(id, channelName) {
  if (sdkClass) {
    return new SdkClient(id, channelName, sdkClass);
  }
  return new RawV2Client(id, channelName);
}

class SdkClient {
  constructor(id, channelName, Sockudo) {
    this.id = id;
    this.channelName = channelName;
    this.client = new Sockudo(APP_KEY, {
      cluster: "local",
      forceTLS: false,
      enabledTransports: ["ws"],
      wsHost: "127.0.0.1",
      wsPort: config.port,
      wssPort: config.port,
      protocolVersion: 2,
      connectionRecovery: true,
      activityTimeout: 5_000,
      pongTimeout: 2_000,
    });
    this.channel = this.client.subscribe(channelName);
    this.channel.bind("sockudo:subscription_succeeded", () => {
      counters.subscriptionSucceeded += 1;
      recordFault("client_subscribed", { client: this.id });
    });
    this.channel.bind(EVENT_NAME, (data) => {
      this.recordEvent(data);
    });
    this.client.bind("sockudo:resume_success", (data) => {
      counters.resumeSucceeded += 1;
      recordFault("resume_success", { client: this.id, data });
    });
    this.client.bind("sockudo:resume_failed", (data) => {
      counters.resumeFailed += 1;
      recordFault("resume_failed", { client: this.id, data });
    });
    this.client.connection.bind("error", (error) => {
      notes.push({ type: "client_error", client: this.id, error: String(error?.message || error) });
    });
  }

  async connect() {
    this.client.connect();
    await waitFor(() => this.channel.subscribed === true, 10_000, `${this.id} subscribe`);
  }

  disconnect() {
    this.client.disconnect();
  }

  recordEvent(data) {
    const payload = typeof data === "string" ? parseJson(data) : data;
    recordReceived(this.id, payload);
  }
}

class RawV2Client {
  constructor(id, channelName) {
    this.id = id;
    this.channelName = channelName;
    this.socket = null;
    this.lastPosition = null;
  }

  async connect() {
    this.subscribed = false;
    const url = new URL(`ws://127.0.0.1:${config.port}/app/${APP_KEY}`);
    url.searchParams.set("protocol", "2");
    url.searchParams.set("client", "outside-in-chaos");
    url.searchParams.set("version", "1.0.0");
    url.searchParams.set("format", "json");
    const socket = new WebSocket(url);
    this.socket = socket;

    await new Promise((resolveConnect, rejectConnect) => {
      const timer = setTimeout(() => rejectConnect(new Error(`${this.id} websocket timeout`)), 8000);
      socket.addEventListener("open", () => {
        clearTimeout(timer);
        resolveConnect();
      }, { once: true });
      socket.addEventListener("error", () => {
        clearTimeout(timer);
        rejectConnect(new Error(`${this.id} websocket open error`));
      }, { once: true });
    });

    socket.addEventListener("message", (event) => this.handleMessage(event.data));
    this.send("sockudo:subscribe", { channel: this.channelName });
    await waitFor(() => this.subscribed === true, 10_000, `${this.id} raw subscribe`);
    if (this.lastPosition) {
      this.send("sockudo:resume", {
        channel_positions: {
          [this.channelName]: this.lastPosition,
        },
      });
    }
  }

  disconnect() {
    if (this.socket && this.socket.readyState <= WebSocket.OPEN) {
      this.socket.close();
    }
  }

  send(event, data) {
    this.socket.send(JSON.stringify({ event, data }));
  }

  handleMessage(raw) {
    const frame = parseJson(String(raw));
    if (!frame || typeof frame.event !== "string") {
      return;
    }
    if (frame.event === "sockudo_internal:subscription_succeeded") {
      this.subscribed = true;
      counters.subscriptionSucceeded += 1;
      const data = typeof frame.data === "string" ? parseJson(frame.data) : frame.data;
      if (data?.stream_id && Number.isFinite(Number(data?.serial))) {
        this.lastPosition = { stream_id: data.stream_id, serial: Number(data.serial) };
      }
      return;
    }
    if (frame.event === "sockudo:resume_success") {
      counters.resumeSucceeded += 1;
      recordFault("resume_success", { client: this.id, data: frame.data });
      return;
    }
    if (frame.event === "sockudo:resume_failed") {
      counters.resumeFailed += 1;
      recordFault("resume_failed", { client: this.id, data: frame.data });
      return;
    }
    if (frame.event === EVENT_NAME) {
      if (frame.stream_id && Number.isFinite(Number(frame.serial))) {
        this.lastPosition = { stream_id: frame.stream_id, serial: Number(frame.serial) };
      }
      const payload = typeof frame.data === "string" ? parseJson(frame.data) : frame.data;
      recordReceived(this.id, payload);
    }
  }
}

async function reconnectClients(clients, reason) {
  counters.clientReconnects += clients.length;
  recordFault("client_reconnect", { reason, clients: clients.map((client) => client.id) });
  for (const client of clients) {
    client.disconnect();
  }
  await sleep(250);
  for (const client of clients) {
    await connectClientWithRetries(client, reason);
  }
}

async function connectClientWithRetries(client, reason) {
  let lastError = null;
  for (let attempt = 1; attempt <= config.clientConnectAttempts; attempt += 1) {
    try {
      await client.connect();
      if (attempt > 1) {
        recordFault("client_connect_retry_succeeded", { client: client.id, reason, attempt });
      }
      return;
    } catch (error) {
      lastError = error;
      recordFault("client_connect_retry", {
        client: client.id,
        reason,
        attempt,
        maxAttempts: config.clientConnectAttempts,
        message: error.message,
      });
      client.disconnect();
      if (attempt < config.clientConnectAttempts) {
        await sleep(config.clientConnectRetryMs);
      }
    }
  }
  failures.push({
    type: reason === "initial_connect" ? "client_initial_connect_failed" : "client_reconnect_failed",
    client: client.id,
    reason,
    attempts: config.clientConnectAttempts,
    message: lastError?.message ?? "unknown client connect failure",
  });
}

async function publishWithNetworkFaults(channelName, sequence) {
  const idempotencyKey = `chaos-${config.seed}-${sequence}`;
  const payload = {
    seed: config.seed,
    sequence,
    sentAt: new Date().toISOString(),
    channel: channelName,
  };

  if (config.networkFaultMode === "publisher" && publisherRng() < config.networkDropProbability) {
    counters.publishDroppedBeforeSend += 1;
    recordFault("network_drop_publish", { sequence });
    publishResults.push({ sequence, accepted: false, droppedBeforeSend: true });
    return;
  }
  if (config.networkFaultMode === "publisher" && publisherRng() < config.networkDelayProbability) {
    const delayMs = Math.max(1, Math.round(config.networkDelayMs * (0.5 + publisherRng())));
    recordFault("network_delay_publish", { sequence, delayMs });
    await sleep(delayMs);
  }

  const first = await publishToSockudo(channelName, payload, idempotencyKey);
  publishResults.push({ sequence, ...first });

  if (config.networkFaultMode !== "off" && publisherRng() < config.networkDuplicateProbability) {
    counters.publishDuplicatesSent += 1;
    recordFault("network_duplicate_publish", { sequence, idempotencyKey });
    await publishToSockudo(channelName, payload, idempotencyKey, true);
  }
}

async function publishToSockudo(channelName, payload, idempotencyKey, duplicate = false) {
  counters.publishAttempts += 1;
  const path = `/apps/${APP_ID}/events`;
  const body = JSON.stringify({
    name: EVENT_NAME,
    channels: [channelName],
    data: JSON.stringify(payload),
    idempotency_key: idempotencyKey,
  });
  try {
    const response = await signedFetch("POST", path, body);
    const text = await response.text();
    if (response.status === 200 || response.status === 202) {
      counters.publishAccepted += duplicate ? 0 : 1;
      return { accepted: true, status: response.status, duplicate };
    }
    counters.publishFailed += duplicate ? 0 : 1;
    return { accepted: false, status: response.status, body: text.slice(0, 500), duplicate };
  } catch (error) {
    counters.publishFailed += duplicate ? 0 : 1;
    return { accepted: false, error: error.message, duplicate };
  }
}

async function exercisePushPath(channelName) {
  counters.pushAttempts += 1;
  const basePath = `/apps/${APP_ID}/push`;
  const deviceId = `chaos-device-${config.seed}`;
  const providerBefore = await readPushProviderMetrics().catch(() => null);
  const providerRequestsBefore = providerBefore?.requests ?? 0;
  try {
    const deviceBody = JSON.stringify({
      appId: APP_ID,
      id: deviceId,
      clientId: "chaos-client",
      formFactor: "phone",
      platform: "android",
      deviceSecret: "placeholder",
      timezone: "UTC",
      locale: "en",
      push: {
        recipient: { transportType: "gcm", registrationToken: `chaos-token-${config.seed}` },
        state: "ACTIVE",
      },
    });
    const deviceResponse = await signedFetch("POST", `${basePath}/deviceRegistrations`, deviceBody, {
      "x-sockudo-push-capability": "push-admin",
    });
    if (deviceResponse.status === 404) {
      counters.pushSkipped += 1;
      notes.push({ type: "push_skipped", reason: "push routes are not available in this binary" });
      return;
    }
    if (!deviceResponse.ok) {
      throw new Error(`device registration HTTP ${deviceResponse.status}: ${await deviceResponse.text()}`);
    }
    const deviceJson = await deviceResponse.json();
    const tokenHash = deviceJson.tokenHash || deviceJson.token_hash || "unknown";
    const subscriptionBody = JSON.stringify({
      appId: APP_ID,
      channel: channelName,
      deviceId,
      clientId: "chaos-client",
      provider: "fcm",
      tokenHash,
      credentialVersion: 1,
    });
    const subscriptionResponse = await signedFetch("POST", `${basePath}/channelSubscriptions`, subscriptionBody, {
      "x-sockudo-push-capability": "push-admin",
    });
    if (!subscriptionResponse.ok) {
      throw new Error(
        `channel subscription HTTP ${subscriptionResponse.status}: ${await subscriptionResponse.text()}`,
      );
    }
    const publishId = `chaos-push-${config.seed}`;
    const publishBody = JSON.stringify({
      publishId,
      recipients: [
        { type: "channel", channel: channelName },
        {
          type: "recipient",
          recipient: { transportType: "gcm", registrationToken: `chaos-direct-token-${config.seed}` },
        },
      ],
      payload: { title: "Chaos", body: `seed ${config.seed}` },
      providerOverrides: [{ provider: "fcm", payload: { data: { seed: String(config.seed) } } }],
      sync: false,
    });
    const pushResponse = await signedFetch("POST", `${basePath}/publish`, publishBody, {
      "x-sockudo-push-capability": "push-admin",
    });
    if (pushResponse.status === 202 || pushResponse.status === 200) {
      counters.pushAccepted += 1;
      recordFault("push_publish_accepted", { publishId, status: pushResponse.status });
      await observePushProviderOutcome(providerRequestsBefore, publishId);
    } else {
      counters.pushFailed += 1;
      notes.push({
        type: "push_publish_rejected",
        status: pushResponse.status,
        body: (await pushResponse.text()).slice(0, 800),
      });
    }
  } catch (error) {
    counters.pushFailed += 1;
    notes.push({ type: "push_exercise_failed", message: error.message });
  }
}

async function observePushProviderOutcome(providerRequestsBefore, publishId) {
  if (config.pushProviderProfile === "off") {
    return;
  }

  const readMetrics = async () => {
    const metrics = await readPushProviderMetrics();
    counters.pushProviderRequests = metrics.requests ?? 0;
    return metrics;
  };

  let dispatchObserved = false;
  try {
    await waitFor(async () => {
      const metrics = await readMetrics();
      return (metrics.requests ?? 0) > providerRequestsBefore;
    }, 4_000, "Sockudo push provider dispatch");
    const metrics = await readMetrics();
    dispatchObserved = true;
    recordFault("push_provider_outcome_observed", {
      publishId,
      profile: config.pushProviderProfile,
      source: "sockudo_dispatch",
      requestsBefore: providerRequestsBefore,
      requestsAfter: metrics.requests,
      byStatus: metrics.byStatus ?? {},
    });
  } catch (error) {
    notes.push({
      type: "sockudo_push_provider_dispatch_not_observed",
      publishId,
      profile: config.pushProviderProfile,
      message: error.message,
      note: "Local HTTP provider endpoints can be rejected by Sockudo's provider destination guard.",
    });
  }

  if (!dispatchObserved) {
    await exerciseMockProviderDirectly(publishId);
  }
}

async function exerciseMockProviderDirectly(publishId) {
  try {
    const response = await fetch(
      `http://127.0.0.1:${config.pushProviderPort}/v1/projects/chaos-project/messages:send`,
      {
        method: "POST",
        headers: {
          authorization: "Bearer chaos-provider-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({
          validate_only: false,
          message: {
            token: `chaos-direct-token-${config.seed}`,
            notification: { title: "Chaos", body: `seed ${config.seed}` },
            data: { publishId, seed: String(config.seed) },
          },
        }),
      },
    );
    const body = await response.text();
    const metrics = await readPushProviderMetrics();
    counters.pushProviderRequests = metrics.requests ?? 0;
    recordFault("push_provider_fake_outcome_observed", {
      publishId,
      profile: config.pushProviderProfile,
      source: "harness_direct_probe",
      status: response.status,
      body: body.slice(0, 800),
      byStatus: metrics.byStatus ?? {},
    });
  } catch (error) {
    const note = {
      type: "push_provider_fake_outcome_missing",
      publishId,
      profile: config.pushProviderProfile,
      message: error.message,
    };
    notes.push(note);
    if (config.requirePushProviderHit) {
      failures.push(note);
    }
  }
}

async function readPushProviderMetrics() {
  const response = await fetch(`http://127.0.0.1:${config.pushProviderPort}/metrics`);
  if (!response.ok) {
    throw new Error(`push provider metrics HTTP ${response.status}`);
  }
  return response.json();
}

async function signedFetch(method, requestPath, body = "", headers = {}) {
  const bodyMd5 = createHash("md5").update(body).digest("hex");
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const params = new URLSearchParams({
    auth_key: APP_KEY,
    auth_timestamp: timestamp,
    auth_version: "1.0",
    body_md5: bodyMd5,
  });
  const canonicalQuery = [...params.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  const signature = createHmac("sha256", APP_SECRET)
    .update(`${method}\n${requestPath}\n${canonicalQuery}`)
    .digest("hex");
  return fetch(
    `http://127.0.0.1:${config.port}${requestPath}?${canonicalQuery}&auth_signature=${signature}`,
    {
      method,
      headers: { "content-type": "application/json", ...headers },
      body: body === "" ? undefined : body,
    },
  );
}

async function startNetworkProxy() {
  if (config.networkFaultMode !== "proxy") {
    notes.push({
      type: "network_fault_mode",
      mode: config.networkFaultMode,
      note:
        config.networkFaultMode === "publisher"
          ? "Network faults are applied around publisher HTTP calls."
          : "Network fault injection is disabled.",
    });
    return null;
  }

  recordFault("network_proxy_start", {
    listenPort: config.port,
    upstreamPort: config.serverPort,
    delayProbability: config.networkDelayProbability,
    dropProbability: config.networkDropProbability,
    note: "Local TCP proxy sits between clients/publishers and the Sockudo server process.",
  });

  const proxy = createTcpServer((clientSocket) => {
    const connectionId = ++proxyConnectionId;
    counters.networkProxyConnections += 1;
    trackProxySocket(clientSocket);

    if (networkFaultsActive && proxyRng() < config.networkDropProbability) {
      counters.networkProxyDroppedConnections += 1;
      recordFault("network_proxy_drop_connection", { connectionId });
      clientSocket.destroy();
      return;
    }

    const upstreamSocket = connectTcp({ host: "127.0.0.1", port: config.serverPort });
    trackProxySocket(upstreamSocket);

    upstreamSocket.on("error", (error) => {
      recordFault("network_proxy_upstream_error", { connectionId, message: error.message });
      clientSocket.destroy();
    });
    clientSocket.on("error", (error) => {
      recordFault("network_proxy_client_error", { connectionId, message: error.message });
      upstreamSocket.destroy();
    });

    pipeWithProxyFaults(clientSocket, upstreamSocket, "client_to_server", connectionId);
    pipeWithProxyFaults(upstreamSocket, clientSocket, "server_to_client", connectionId);
  });

  await new Promise((resolveListen, rejectListen) => {
    proxy.once("error", rejectListen);
    proxy.listen(config.port, "127.0.0.1", () => {
      proxy.off("error", rejectListen);
      resolveListen();
    });
  });
  proxy.on("error", (error) => {
    recordFault("network_proxy_error", { message: error.message });
  });

  return proxy;
}

function enableNetworkFaults() {
  if (config.networkFaultMode !== "proxy") {
    return;
  }
  networkFaultsActive = true;
  recordFault("network_proxy_faults_enabled", {
    note: "Enabled after initial client subscription and optional push setup completed.",
  });
}

function pipeWithProxyFaults(source, target, direction, connectionId) {
  source.on("data", (chunk) => {
    if (direction === "client_to_server") {
      counters.networkProxyBytesClientToServer += chunk.length;
    } else {
      counters.networkProxyBytesServerToClient += chunk.length;
    }

    if (networkFaultsActive && proxyRng() < config.networkDropProbability) {
      counters.networkProxyDroppedStreams += 1;
      recordFault("network_proxy_drop_stream", {
        connectionId,
        direction,
        bytes: chunk.length,
        note: "The proxy closes the TCP stream instead of silently deleting bytes.",
      });
      source.destroy();
      target.destroy();
      return;
    }

    const writeChunk = () => {
      if (!target.destroyed && target.writable) {
        target.write(chunk);
      }
    };

    if (networkFaultsActive && proxyRng() < config.networkDelayProbability) {
      const delayMs = Math.max(1, Math.round(config.networkDelayMs * (0.5 + proxyRng())));
      counters.networkProxyDelayedChunks += 1;
      recordFault("network_proxy_delay_chunk", {
        connectionId,
        direction,
        bytes: chunk.length,
        delayMs,
      });
      setTimeout(writeChunk, delayMs);
    } else {
      writeChunk();
    }
  });

  source.on("end", () => {
    if (!target.destroyed) {
      target.end();
    }
  });
  source.on("close", () => {
    if (!target.destroyed) {
      target.destroy();
    }
  });
}

function trackProxySocket(socket) {
  activeProxySockets.add(socket);
  socket.once("close", () => {
    activeProxySockets.delete(socket);
  });
}

async function closeNetworkProxy() {
  if (!networkProxy) {
    return;
  }
  for (const socket of activeProxySockets) {
    socket.destroy();
  }
  await new Promise((resolveClose) => {
    networkProxy.close(() => resolveClose());
  });
  networkProxy = null;
}

function startServer(configPath) {
  counters.processStarts += 1;
  recordFault("process_start", { configPath });
  const log = createWriteStream(serverLogPath, { flags: "a" });
  const child = spawn(config.serverBin, ["--config", configPath], {
    cwd: ROOT_DIR,
    env: {
      ...process.env,
      RUST_LOG: process.env.RUST_LOG ?? "info",
      LOG_OUTPUT_FORMAT: process.env.LOG_OUTPUT_FORMAT ?? "json",
      PUSH_FCM_ENDPOINT: `http://127.0.0.1:${config.pushProviderPort}`,
      PUSH_FCM_PROVIDER_TOKEN: "chaos-provider-token",
      PUSH_FCM_PROJECT_ID: "chaos-project",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.pipe(log, { end: false });
  child.stderr.pipe(log, { end: false });
  child.on("exit", (code, signal) => {
    recordFault("process_exit", { code, signal });
  });
  return child;
}

function startPushProvider() {
  recordFault("push_provider_start", { profile: config.pushProviderProfile });
  const log = createWriteStream(providerLogPath, { flags: "a" });
  const child = spawn(
    process.execPath,
    [
      "scripts/push-mock-provider.mjs",
      "--host",
      "127.0.0.1",
      "--port",
      String(config.pushProviderPort),
      "--profile",
      config.pushProviderProfile,
      "--failure-rate",
      "0.35",
      "--latency-ms",
      "25",
      "--jitter-ms",
      "25",
      "--seed",
      String((config.seed ^ 0xf00d_f00d) >>> 0),
    ],
    { cwd: ROOT_DIR, stdio: ["ignore", "pipe", "pipe"] },
  );
  child.stdout.pipe(log, { end: false });
  child.stderr.pipe(log, { end: false });
  return child;
}

async function killServer(reason) {
  if (!server || server.exitCode !== null) {
    return;
  }
  if (reason !== "cleanup_kill") {
    counters.processKills += 1;
  }
  recordFault(reason, { pid: server.pid });
  server.kill("SIGKILL");
  await onceExit(server, 5000);
}

async function cleanup() {
  await killServer("cleanup_kill").catch(() => {});
  await closeNetworkProxy().catch(() => {});
  if (pushProvider && pushProvider.exitCode === null) {
    pushProvider.kill("SIGTERM");
    await onceExit(pushProvider, 2000).catch(() => pushProvider.kill("SIGKILL"));
  }
}

async function waitForServer() {
  await waitForHttp(`http://127.0.0.1:${config.port}/up/${APP_ID}`, 15_000);
}

async function waitForHttp(url, timeoutMs) {
  await waitFor(async () => {
    try {
      const response = await fetch(url);
      return response.ok;
    } catch {
      return false;
    }
  }, timeoutMs, url);
}

async function loadSockudoSdk() {
  globalThis.window = globalThis;
  const location = { protocol: "http:" };
  globalThis.location = location;
  globalThis.VERSION = "outside-in-chaos";
  globalThis.CDN_HTTP = "";
  globalThis.CDN_HTTPS = "";
  globalThis.DEPENDENCY_SUFFIX = "";
  globalThis.document = {
    location,
    readyState: "complete",
    body: { appendChild() {} },
    documentElement: {},
    getElementsByTagName() {
      return [{ appendChild() {} }];
    },
    createElement() {
      return { setAttribute() {}, appendChild() {}, parentNode: { removeChild() {} } };
    },
    addEventListener() {},
    removeEventListener() {},
  };
  const sdkPath = join(ROOT_DIR, "client-sdks/sockudo-js/dist/web/sockudo.mjs");
  await readFile(sdkPath);
  const module = await import(pathToFileURL(sdkPath).href);
  return module.default;
}

function serverConfig({ pushEnabled, activityTimeout }) {
  return `debug = true
host = "127.0.0.1"
port = ${config.serverPort}
mode = "development"
path_prefix = "/"
shutdown_grace_period = 1
activity_timeout = ${activityTimeout}
health_check_timeout_ms = 1000

[adapter]
driver = "local"
enable_socket_counting = true
fallback_to_local = true

[app_manager]
driver = "memory"

[app_manager.array]
[[app_manager.array.apps]]
id = "${APP_ID}"
key = "${APP_KEY}"
secret = "${APP_SECRET}"
enabled = true

[app_manager.array.apps.policy.limits]
max_connections = 1000
max_client_events_per_second = 1000
max_backend_events_per_second = 1000
max_read_requests_per_second = 1000
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
max_messages_per_channel = 1000

[cache]
driver = "memory"

[queue]
driver = "memory"

[rate_limiter]
enabled = false
driver = "memory"

[metrics]
enabled = true
driver = "prometheus"
host = "127.0.0.1"
port = ${config.metricsPort}

[connection_recovery]
enabled = true
buffer_ttl_seconds = 120
max_buffer_size = 200

[history]
enabled = true
rewind_enabled = true
backend = "memory"
retention_window_seconds = 86400
max_page_size = 100
max_messages_per_channel = 1000
writer_shards = 2
writer_queue_capacity = 256
purge_interval_seconds = 300
purge_batch_size = 1000
max_purge_per_tick = 100000

[presence_history]
enabled = true
retention_window_seconds = 86400
max_page_size = 100
max_events_per_channel = 1000

[versioned_messages]
enabled = false
driver = "memory"
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
fcm_enabled = ${pushEnabled}
apns_enabled = false
webpush_enabled = false
hms_enabled = false
wns_enabled = false
dry_run = false
analytics_enabled = true
`;
}

function pushRequested() {
  return config.pushProviderProfile !== "off" || config.exercisePush;
}

function recordReceived(clientId, payload) {
  counters.receivedEvents += 1;
  if (payload && Number.isFinite(Number(payload.sequence))) {
    const sequence = Number(payload.sequence);
    const existing = receivedBySequence.get(sequence) ?? [];
    existing.push({ clientId, at: new Date().toISOString() });
    receivedBySequence.set(sequence, existing);
  }
}

function recordFault(type, details = {}) {
  faults.push({ at: new Date().toISOString(), type, ...details });
}

async function writeArtifact(ok, extra = {}) {
  await mkdir(artifactDir, { recursive: true });
  const artifact = {
    type: "sockudo.outside_in_binary_chaos.v1",
    label: "outside-in chaos",
    manualOnly: true,
    ok,
    seed: config.seed,
    startedAt: startedAt.toISOString(),
    finishedAt: new Date().toISOString(),
    command: ["node", "tools/chaos/sockudo-binary-chaos.mjs", ...process.argv.slice(2)].join(" "),
    replayCommand: buildReplayCommand(),
    faultPlan: {
      processKillAtMs: config.killAtMs,
      restartAfterMs: config.restartAfterMs,
      clientDropAtMs: config.clientDropAtMs,
      clientReconnectAfterMs: config.clientReconnectAfterMs,
      configChangeAtMs: config.configChangeAtMs,
      networkFaultMode: config.networkFaultMode,
      note: "Outside-in process and socket timing is wall-clock based; this command reuses the same effective seed and config.",
    },
    config,
    files: {
      artifact: artifactPath,
      primaryConfig: primaryConfigPath,
      restartConfig: reloadConfigPath,
      serverLog: serverLogPath,
      pushProviderLog: config.pushProviderProfile === "off" ? null : providerLogPath,
    },
    counters,
    faults,
    notes,
    failures,
    publishResults,
    receivedBySequence: Object.fromEntries(receivedBySequence.entries()),
    ...extra,
  };
  await writeFile(artifactPath, `${JSON.stringify(artifact, null, 2)}\n`);
}

function buildReplayCommand() {
  const entries = [
    ["seed", config.seed],
    ["duration-ms", config.durationMs],
    ["settle-ms", config.settleMs],
    ["publish-interval-ms", config.publishIntervalMs],
    ["clients", config.clients],
    ["client-connect-attempts", config.clientConnectAttempts],
    ["client-connect-retry-ms", config.clientConnectRetryMs],
    ["client-mode", config.clientMode],
    ["server-bin", config.serverBin],
    ["artifact-dir", config.artifactRoot],
    ["port", config.port],
    ["server-port", config.serverPort],
    ["metrics-port", config.metricsPort],
    ["push-provider-port", config.pushProviderPort],
    ["kill-at-ms", config.killAtMs],
    ["restart-after-ms", config.restartAfterMs],
    ["client-drop-at-ms", config.clientDropAtMs],
    ["client-reconnect-after-ms", config.clientReconnectAfterMs],
    ["config-change-at-ms", config.configChangeAtMs],
    ["network-fault-mode", config.networkFaultMode],
    ["network-delay-ms", config.networkDelayMs],
    ["network-delay-probability", config.networkDelayProbability],
    ["network-drop-probability", config.networkDropProbability],
    ["network-duplicate-probability", config.networkDuplicateProbability],
    ["strict-delivery", config.strictDelivery],
    ["push-provider-profile", config.pushProviderProfile],
    ["exercise-push", config.exercisePush],
    ["require-push-provider-hit", config.requirePushProviderHit],
  ];
  return ["node", "tools/chaos/sockudo-binary-chaos.mjs", ...entries.flatMap(([key, value]) => [
    `--${key}`,
    shellQuote(String(value)),
  ])].join(" ");
}

function shellQuote(value) {
  if (/^[A-Za-z0-9_./:=+-]+$/.test(value)) {
    return value;
  }
  return `'${value.replaceAll("'", "'\\''")}'`;
}

function parseArgs(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const arg = values[index];
    if (!arg.startsWith("--")) {
      fail(`unexpected argument ${arg}`);
    }
    const [rawKey, inlineValue] = arg.slice(2).split("=", 2);
    const key = rawKey.replace(/-([a-z])/g, (_, char) => char.toUpperCase());
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

function seededHex(rngSource, byteCount) {
  let value = "";
  for (let index = 0; index < byteCount; index += 1) {
    value += Math.floor(rngSource() * 256)
      .toString(16)
      .padStart(2, "0");
  }
  return value;
}

function mulberry32(seed) {
  let state = seed >>> 0;
  return function next() {
    state += 0x6d2b79f5;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function waitFor(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  let lastValue = false;
  while (Date.now() < deadline) {
    lastValue = await predicate();
    if (lastValue) {
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

function parseJson(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function fail(message) {
  console.error(`sockudo-binary-chaos: ${message}`);
  process.exit(2);
}

await main().catch(async (error) => {
  failures.push({ type: "runner_error", message: error?.stack || String(error) });
  await cleanup();
  await writeArtifact(false);
  console.error(`sockudo-binary-chaos: failed; artifact=${artifactPath}`);
  console.error(error?.stack || error);
  process.exit(1);
});
