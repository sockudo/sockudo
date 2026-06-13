#!/usr/bin/env node
import crypto from "node:crypto";
import { performance } from "node:perf_hooks";

const args = parseArgs(process.argv.slice(2));
const config = {
  baseUrl: args.url ?? "http://127.0.0.1:6001",
  appId: args.appId ?? "app-id",
  key: args.key ?? "app-key",
  secret: args.secret ?? "app-secret",
  channel: args.channel ?? "private-ai-rollup-load",
  event: args.event ?? "ai-output",
  tokensPerSecond: numberArg(args.tokensPerSecond, 1000),
  durationSeconds: numberArg(args.duration, 60),
  tokenBytes: numberArg(args.tokenBytes, 8),
  maxP99Ms: numberArg(args.maxP99Ms, 20),
  messageSerial: args.messageSerial,
};

const started = performance.now();
const messageSerial = config.messageSerial ?? await createMessage();
const appendCount = Math.floor(config.tokensPerSecond * config.durationSeconds);
let content = "";
const latencies = [];
let failures = 0;

for (let index = 0; index < appendCount; index += 1) {
  const targetTime = started + (index * 1000) / config.tokensPerSecond;
  const waitMs = targetTime - performance.now();
  if (waitMs > 0) {
    await sleep(waitMs);
  }

  const fragment = tokenFor(index);
  content += fragment;
  const requestStarted = performance.now();
  try {
    await signedJson(
      "POST",
      `/channels/${encodeURIComponent(config.channel)}/messages/${encodeURIComponent(messageSerial)}/append`,
      {
        data: fragment,
        op_id: `rollup-load-${Date.now()}-${index}`,
        extras: {
          ai: {
            transport: {
              status: index + 1 === appendCount ? "complete" : "streaming",
            },
          },
        },
      },
    );
    latencies.push(performance.now() - requestStarted);
  } catch (error) {
    failures += 1;
    if (args.verbose) {
      console.error(`append ${index} failed: ${error.message}`);
    }
  }
}

const latest = await signedJson(
  "GET",
  `/channels/${encodeURIComponent(config.channel)}/messages/${encodeURIComponent(messageSerial)}`,
);
const latestContent = latest?.item?.data;
const expectedRate = appendCount / config.durationSeconds;
const appendLatencyMs = summarize(latencies);

console.log(JSON.stringify({
  messageSerial,
  appendsAttempted: appendCount,
  failures,
  configuredTokensPerSecond: config.tokensPerSecond,
  requestRatePerSecond: Number(expectedRate.toFixed(2)),
  durationSeconds: secondsSince(started),
  appendLatencyMs,
  finalContentMatches: latestContent === content,
  finalContentBytes: Buffer.byteLength(content),
}, null, 2));

if (failures !== 0) {
  fail(`expected zero failed appends, got ${failures}`);
}
if (appendLatencyMs.p99 !== undefined && appendLatencyMs.p99 > config.maxP99Ms) {
  fail(`append p99 ${appendLatencyMs.p99}ms exceeded ${config.maxP99Ms}ms budget`);
}
if (latestContent !== content) {
  fail("latest message content did not match appended fragments");
}

async function createMessage() {
  const response = await signedJson("POST", "/events", {
    name: config.event,
    channel: config.channel,
    data: "",
    extras: {
      ai: {
        transport: {
          status: "streaming",
          role: "assistant",
        },
      },
    },
  });
  const channelInfo = response?.channels?.[config.channel];
  if (!channelInfo?.message_serial) {
    throw new Error("create response did not include message_serial; pass --messageSerial");
  }
  return channelInfo.message_serial;
}

async function signedJson(method, path, body) {
  const fullPath = `/apps/${config.appId}${path}`;
  const bodyText = body === undefined ? undefined : JSON.stringify(body);
  const query = {
    auth_key: config.key,
    auth_timestamp: `${Math.floor(Date.now() / 1_000)}`,
    auth_version: "1.0",
  };
  if (bodyText !== undefined) {
    query.body_md5 = crypto.createHash("md5").update(bodyText).digest("hex");
  }
  const canonicalQuery = Object.keys(query)
    .map((key) => [key.toLowerCase(), query[key]])
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  const stringToSign = `${method}\n${fullPath}\n${canonicalQuery}`;
  query.auth_signature = crypto
    .createHmac("sha256", config.secret)
    .update(stringToSign)
    .digest("hex");

  const url = new URL(`${config.baseUrl}${fullPath}`);
  for (const [key, value] of Object.entries(query)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url, {
    method,
    headers: { "content-type": "application/json" },
    body: bodyText,
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

function tokenFor(index) {
  const seed = index.toString(36);
  if (seed.length >= config.tokenBytes) {
    return seed.slice(0, config.tokenBytes);
  }
  return seed.padEnd(config.tokenBytes, "x");
}

function summarize(values) {
  if (values.length === 0) {
    return { count: 0 };
  }
  const sorted = [...values].sort((left, right) => left - right);
  return {
    count: sorted.length,
    p50: percentile(sorted, 0.50),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max: sorted.at(-1),
  };
}

function percentile(sorted, fraction) {
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * fraction));
  return Number(sorted[index].toFixed(3));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function secondsSince(startedAt) {
  return Number(((performance.now() - startedAt) / 1000).toFixed(3));
}

function numberArg(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`invalid numeric argument: ${value}`);
  }
  return parsed;
}

function fail(message) {
  console.error(message);
  process.exitCode = 1;
}

function parseArgs(argv) {
  const parsed = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg.startsWith("--")) {
      continue;
    }
    const key = arg.slice(2);
    const next = argv[index + 1];
    if (next === undefined || next.startsWith("--")) {
      parsed[key] = true;
    } else {
      parsed[key] = next;
      index += 1;
    }
  }
  return parsed;
}
