#!/usr/bin/env node
import { createHash, createHmac } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const requiredFixtures = [
  "abortPartial",
  "cancelTurn",
  "concurrentTurns",
  "edit",
  "errorTurn",
  "lateJoinHistory",
  "normalTurn",
  "recoverySmoke",
  "regenerate",
  "suspendedContinuation",
];

const thisDir = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(thisDir, "../../..");
const fixtureDir = join(rootDir, "test/ai-conformance/fixtures/golden");

if (process.env.AIT_CONFORMANCE_OFFLINE === "1") {
  await validateFixtures();
  console.log("offline fixture validation passed");
  process.exit(0);
}

const config = {
  baseUrl:
    process.env.SOCKUDO_BASE_URL ??
    process.env.SOCKUDO_URL ??
    "http://127.0.0.1:6001",
  appId: process.env.SOCKUDO_APP_ID ?? "app-id",
  appKey: process.env.SOCKUDO_APP_KEY ?? "app-key",
  appSecret: process.env.SOCKUDO_APP_SECRET ?? "app-secret",
};

await validateFixtures();
await assertHealthy();

const runId = `${Date.now()}-${process.pid}`;
const scenarios = [
  testTokenStreamingAndLatestView,
  testCancellationAndLifecycle,
  testConcurrentTurns,
  testBranchMetadata,
  testHistoryFallback,
];

for (const scenario of scenarios) {
  await scenario(runId);
}

console.log(`AI conformance passed (${scenarios.length} live scenarios)`);

async function validateFixtures() {
  const files = (await readdir(fixtureDir))
    .filter((file) => file.endsWith(".json"))
    .sort();
  const names = files.map((file) => file.replace(/\.json$/u, ""));
  assertDeepEqual(names, requiredFixtures, "golden fixture set changed");
  for (const file of files) {
    const frames = JSON.parse(await readFile(join(fixtureDir, file), "utf8"));
    assert(Array.isArray(frames), `${file} must be a JSON frame array`);
    assert(frames.length > 0, `${file} must not be empty`);
    for (const [index, frame] of frames.entries()) {
      assert(typeof frame.event === "string", `${file}[${index}].event`);
      assert(
        frame.channel === undefined || typeof frame.channel === "string",
        `${file}[${index}].channel`,
      );
    }
  }
}

async function assertHealthy() {
  const response = await fetch(`${config.baseUrl}/up/${config.appId}`);
  assert(response.ok, `Sockudo health check failed: ${response.status}`);
}

async function testTokenStreamingAndLatestView(runId) {
  const channel = channelName(runId, "stream");
  await publish(channel, {
    name: "ai-output",
    data: "",
    extras: aiHeaders({
      turnId: "turn-stream",
      role: "assistant",
      status: "streaming",
      stream: true,
      streamId: "text:turn-stream:text",
      codecType: "text-start",
      codecId: "text",
    }),
  });

  const created = await eventually(() => latestHistoryMessage(channel));
  const messageSerial = created.message.message_serial;
  assertString(messageSerial, "stream create message_serial");

  await post(
    `/apps/${config.appId}/channels/${channel}/messages/${messageSerial}/append`,
    { data: "hello" },
  );
  await post(
    `/apps/${config.appId}/channels/${channel}/messages/${messageSerial}/update`,
    {
      data: "hello world",
      extras: aiHeaders({
        turnId: "turn-stream",
        role: "assistant",
        status: "complete",
        stream: true,
        streamId: "text:turn-stream:text",
        codecType: "text-end",
        codecId: "text",
      }),
    },
  );

  const latest = await get(
    `/apps/${config.appId}/channels/${channel}/messages/${messageSerial}`,
  );
  assertEqual(latest.item.action, "update", "latest action is update");
  assertEqual(latest.item.data, "hello world", "latest data is accumulated");
  assertEqual(
    latest.item.extras.headers["x-sockudo-status"],
    "complete",
    "latest status is complete",
  );

  const history = await getHistory(channel);
  const substituted = history.items.find(
    (item) => item.message?.message_serial === messageSerial,
  );
  assert(substituted, "history contains versioned stream message");
  assertEqual(
    substituted.message.data,
    "hello world",
    "history exposes latest version for late join",
  );
}

async function testCancellationAndLifecycle(runId) {
  const channel = channelName(runId, "cancel");
  await publish(channel, {
    name: "ai-turn-start",
    data: "",
    extras: aiHeaders({
      turnId: "turn-cancel",
      invocationId: "inv-cancel",
      clientId: "client-a",
    }),
  });
  await publish(channel, {
    name: "ai-cancel",
    data: "",
    extras: aiHeaders({ turnId: "turn-cancel", clientId: "client-b" }),
  });
  await publish(channel, {
    name: "ai-turn-end",
    data: "",
    extras: aiHeaders({ turnId: "turn-cancel", turnReason: "cancelled" }),
  });

  const history = await eventually(() => historyWithAtLeast(channel, 3));
  assert(
    history.items.some((item) => item.message?.name === "ai-cancel"),
    "cancel signal is durable",
  );
  assert(
    history.items.some(
      (item) =>
        item.message?.name === "ai-turn-end" &&
        item.message?.extras?.headers?.["x-sockudo-turn-reason"] ===
          "cancelled",
    ),
    "cancelled turn end is durable",
  );
}

async function testConcurrentTurns(runId) {
  const channel = channelName(runId, "concurrent");
  const first = await createStreamingMessage(channel, "turn-a", "A");
  const second = await createStreamingMessage(channel, "turn-b", "B");

  await post(
    `/apps/${config.appId}/channels/${channel}/messages/${first}/append`,
    { data: "1" },
  );
  await post(
    `/apps/${config.appId}/channels/${channel}/messages/${second}/append`,
    { data: "2" },
  );
  await post(
    `/apps/${config.appId}/channels/${channel}/messages/${first}/update`,
    {
      data: "A1",
      extras: aiHeaders({
        turnId: "turn-a",
        role: "assistant",
        status: "complete",
        stream: true,
        streamId: "text:turn-a:text",
        codecType: "text-end",
        codecId: "text",
      }),
    },
  );
  await post(
    `/apps/${config.appId}/channels/${channel}/messages/${second}/update`,
    {
      data: "B2",
      extras: aiHeaders({
        turnId: "turn-b",
        role: "assistant",
        status: "cancelled",
        stream: true,
        streamId: "text:turn-b:text",
        codecType: "text-end",
        codecId: "text",
      }),
    },
  );

  const latestFirst = await get(
    `/apps/${config.appId}/channels/${channel}/messages/${first}`,
  );
  const latestSecond = await get(
    `/apps/${config.appId}/channels/${channel}/messages/${second}`,
  );
  assertEqual(latestFirst.item.data, "A1", "first turn accumulated");
  assertEqual(latestSecond.item.data, "B2", "second turn accumulated");
  assertEqual(
    latestSecond.item.extras.headers["x-sockudo-status"],
    "cancelled",
    "second turn can terminate independently",
  );
}

async function testBranchMetadata(runId) {
  const channel = channelName(runId, "branch");
  await publish(channel, {
    name: "ai-output",
    data: "branch answer",
    extras: aiHeaders({
      turnId: "turn-branch",
      parent: "msg-parent",
      forkOf: "msg-original",
      role: "assistant",
      status: "complete",
      stream: true,
      streamId: "text:turn-branch:text",
      codecType: "text-delta",
      codecId: "text",
    }),
  });

  const latest = await eventually(() => latestHistoryMessage(channel));
  assertEqual(
    latest.message.extras.headers["x-sockudo-parent"],
    "msg-parent",
    "parent metadata round-trips",
  );
  assertEqual(
    latest.message.extras.headers["x-sockudo-fork-of"],
    "msg-original",
    "fork metadata round-trips",
  );
}

async function testHistoryFallback(runId) {
  const channel = channelName(runId, "history");
  const messageSerial = await createStreamingMessage(
    channel,
    "turn-history",
    "recover",
  );
  await post(
    `/apps/${config.appId}/channels/${channel}/messages/${messageSerial}/update`,
    {
      data: "recoverable",
      extras: aiHeaders({
        turnId: "turn-history",
        role: "assistant",
        status: "complete",
        stream: true,
        streamId: "text:turn-history:text",
        codecType: "text-end",
        codecId: "text",
      }),
    },
  );

  const history = await getHistory(channel, { direction: "newest_first" });
  assert(
    history.items.some(
      (item) =>
        item.message?.message_serial === messageSerial &&
        item.message?.data === "recoverable",
    ),
    "history fallback returns accumulated latest state",
  );
}

async function createStreamingMessage(channel, turnId, initialData) {
  await publish(channel, {
    name: "ai-output",
    data: initialData,
    extras: aiHeaders({
      turnId,
      role: "assistant",
      status: "streaming",
      stream: true,
      streamId: `text:${turnId}:text`,
      codecType: "text-start",
      codecId: "text",
    }),
  });
  const latest = await eventually(() => latestHistoryMessage(channel));
  const messageSerial = latest.message.message_serial;
  assertString(messageSerial, `${turnId} message_serial`);
  return messageSerial;
}

async function publish(channel, message) {
  return post(`/apps/${config.appId}/events`, {
    channel,
    name: message.name,
    data: message.data,
    extras: message.extras,
  });
}

function aiHeaders(options) {
  const headers = {};
  set(headers, "x-sockudo-turn-id", options.turnId);
  set(headers, "x-sockudo-client-id", options.clientId);
  set(headers, "x-sockudo-turn-reason", options.turnReason);
  set(headers, "x-sockudo-invocation-id", options.invocationId);
  set(headers, "x-sockudo-parent", options.parent);
  set(headers, "x-sockudo-fork-of", options.forkOf);
  set(headers, "x-sockudo-role", options.role);
  set(headers, "x-sockudo-status", options.status);
  set(headers, "x-sockudo-stream", options.stream);
  set(headers, "x-sockudo-stream-id", options.streamId);
  set(headers, "x-sockudo-codec-type", options.codecType);
  set(headers, "x-sockudo-codec-id", options.codecId);
  return { headers };
}

function set(target, key, value) {
  if (value !== undefined) {
    target[key] = value;
  }
}

async function latestHistoryMessage(channel) {
  const history = await getHistory(channel, { direction: "newest_first" });
  const item = history.items.find((entry) => entry.message?.message_serial);
  assert(item, `no history message found for ${channel}`);
  return item;
}

async function historyWithAtLeast(channel, count) {
  const history = await getHistory(channel, {
    direction: "oldest_first",
    limit: Math.max(count, 10),
  });
  assert(
    history.items.length >= count,
    `${channel} expected at least ${count} history rows`,
  );
  return history;
}

async function getHistory(channel, options = {}) {
  const params = {
    limit: String(options.limit ?? 50),
    direction: options.direction ?? "oldest_first",
  };
  return get(`/apps/${config.appId}/channels/${channel}/history`, params);
}

async function get(path, params = {}) {
  return request("GET", path, undefined, params);
}

async function post(path, body) {
  return request("POST", path, body);
}

async function request(method, path, body, params = {}) {
  const bodyText = body === undefined ? "" : JSON.stringify(body);
  const query = signedQuery(method, path, bodyText, params);
  const response = await fetch(`${config.baseUrl}${path}?${query}`, {
    method,
    headers:
      body === undefined
        ? undefined
        : {
            "content-type": "application/json",
          },
    body: body === undefined ? undefined : bodyText,
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${method} ${path} failed ${response.status}: ${text}`);
  }
  return text.trim() === "" ? undefined : JSON.parse(text);
}

function signedQuery(method, path, bodyText, params) {
  const entries = {
    ...params,
    auth_key: config.appKey,
    auth_timestamp: String(Math.floor(Date.now() / 1000)),
    auth_version: "1.0",
  };
  if (method === "POST" && bodyText.length > 0) {
    entries.body_md5 = createHash("md5").update(bodyText).digest("hex");
  }
  const sorted = Object.entries(entries)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  const stringToSign = `${method}\n${path}\n${sorted}`;
  const signature = createHmac("sha256", config.appSecret)
    .update(stringToSign)
    .digest("hex");
  return `${new URLSearchParams(entries).toString()}&auth_signature=${signature}`;
}

function channelName(runId, scenario) {
  return `private-ai-conformance-${runId}-${scenario}`;
}

async function eventually(fn) {
  let lastError;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      await sleep(50);
    }
  }
  throw lastError;
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function assert(value, message) {
  if (!value) {
    throw new Error(message);
  }
}

function assertString(value, message) {
  assert(typeof value === "string" && value.length > 0, message);
}

function assertEqual(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(
      `${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
    );
  }
}

function assertDeepEqual(actual, expected, message) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
    );
  }
}
