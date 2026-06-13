#!/usr/bin/env node
import crypto from "node:crypto";
import { performance } from "node:perf_hooks";

const args = parseArgs(process.argv.slice(2));
const config = {
  urls: splitList(args.urls ?? process.env.SOCKUDO_NODE_URLS ?? "http://127.0.0.1:6001,http://127.0.0.1:6002,http://127.0.0.1:6003"),
  appId: args.appId ?? process.env.SOCKUDO_APP_ID ?? "app-id",
  key: args.key ?? process.env.SOCKUDO_APP_KEY ?? "app-key",
  secret: args.secret ?? process.env.SOCKUDO_APP_SECRET ?? "app-secret",
  channelPrefix: args.channelPrefix ?? "private-ai-scale",
  streams: numberArg(args.streams, 10),
  appendsPerStream: numberArg(args.appendsPerStream, 200),
  tokenBytes: numberArg(args.tokenBytes, 8),
  maxAppendAddedP99Ms: numberArg(args.maxAppendAddedP99Ms ?? args.maxAppendP99Ms, 5),
};

if (config.urls.length < 2) {
  fail("expected at least two node URLs");
}

const started = performance.now();
const runId = `${Date.now().toString(36)}-${process.pid.toString(36)}`;
const baseline = await runWorkload("baseline", (streamIndex, _appendIndex) => {
  return config.urls[streamIndex % config.urls.length];
});
const crossNode = await runWorkload("cross-node", (streamIndex, appendIndex) => {
  return config.urls[(streamIndex + appendIndex + 1) % config.urls.length];
});

const baselineAppendLatencyMs = summarize(baseline.appendLatencies);
const crossNodeAppendLatencyMs = summarize(crossNode.appendLatencies);
const latestLatencyMs = summarize([...baseline.latestLatencies, ...crossNode.latestLatencies]);
const appendAddedP99Ms =
  crossNodeAppendLatencyMs.p99 === undefined || baselineAppendLatencyMs.p99 === undefined
    ? undefined
    : Number(Math.max(0, crossNodeAppendLatencyMs.p99 - baselineAppendLatencyMs.p99).toFixed(3));
const failures = baseline.failures + crossNode.failures;

const result = {
  urls: config.urls,
  streams: config.streams,
  appendsPerStream: config.appendsPerStream,
  appendsAttempted: config.streams * config.appendsPerStream * 2,
  failures,
  durationSeconds: secondsSince(started),
  baselineAppendLatencyMs,
  crossNodeAppendLatencyMs,
  appendAddedP99Ms,
  latestLatencyMs,
};
console.log(JSON.stringify(result, null, 2));

if (failures !== 0) {
  fail(`expected zero failed appends, got ${failures}`);
}
if (appendAddedP99Ms !== undefined && appendAddedP99Ms > config.maxAppendAddedP99Ms) {
  fail(`cross-node append added p99 ${appendAddedP99Ms}ms exceeded ${config.maxAppendAddedP99Ms}ms budget`);
}

async function runWorkload(label, appendNodeFor) {
  const appendLatencies = [];
  const latestLatencies = [];
  let failures = 0;

  for (let streamIndex = 0; streamIndex < config.streams; streamIndex += 1) {
    const channel = `${config.channelPrefix}-${label}-${runId}-${streamIndex}`;
    const createNode = config.urls[streamIndex % config.urls.length];
    const messageSerial = await createMessage(createNode, channel);
    let expected = "";

    for (let appendIndex = 0; appendIndex < config.appendsPerStream; appendIndex += 1) {
      const node = appendNodeFor(streamIndex, appendIndex);
      const fragment = tokenFor(streamIndex, appendIndex, config.tokenBytes);
      expected += fragment;
      const requestStarted = performance.now();
      try {
        await signedJson(node, "POST", `/channels/${encodeURIComponent(channel)}/messages/${encodeURIComponent(messageSerial)}/append`, {
          data: fragment,
          op_id: `${label}-${streamIndex}-${appendIndex}-${runId}`,
          extras: {
            ai: {
              transport: {
                status: appendIndex + 1 === config.appendsPerStream ? "complete" : "streaming",
              },
            },
          },
        });
        appendLatencies.push(performance.now() - requestStarted);
      } catch (error) {
        failures += 1;
        if (args.verbose) {
          console.error(`append failed label=${label} node=${node} stream=${streamIndex} append=${appendIndex}: ${error.message}`);
        }
      }
    }

    for (const node of config.urls) {
      const requestStarted = performance.now();
      const latest = await signedJson(node, "GET", `/channels/${encodeURIComponent(channel)}/messages/${encodeURIComponent(messageSerial)}`);
      latestLatencies.push(performance.now() - requestStarted);
      const actual = latest?.item?.data;
      if (actual !== expected) {
        fail(`node ${node} returned divergent aggregate for ${channel}: expected ${expected.length} bytes, got ${actual === undefined ? "undefined" : Buffer.byteLength(String(actual))}`);
      }
    }
  }

  return { appendLatencies, latestLatencies, failures };
}

async function createMessage(baseUrl, channel) {
  const response = await signedJson(baseUrl, "POST", "/events", {
    name: "ai-output",
    channel,
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
  const messageSerial = response?.channels?.[channel]?.message_serial;
  if (!messageSerial) {
    throw new Error(`create response from ${baseUrl} did not include message_serial`);
  }
  return messageSerial;
}

async function signedJson(baseUrl, method, path, body) {
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
  const signature = crypto
    .createHmac("sha256", config.secret)
    .update(`${method}\n${fullPath}\n${canonicalQuery}`)
    .digest("hex");

  const url = new URL(`${baseUrl}${fullPath}`);
  for (const [key, value] of Object.entries(query)) {
    url.searchParams.set(key, value);
  }
  url.searchParams.set("auth_signature", signature);

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

function tokenFor(streamIndex, appendIndex, bytes) {
  const seed = `${streamIndex.toString(36)}-${appendIndex.toString(36)}`;
  return seed.length >= bytes ? seed.slice(0, bytes) : seed.padEnd(bytes, "x");
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
    max: Number(sorted.at(-1).toFixed(3)),
  };
}

function percentile(sorted, fraction) {
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * fraction));
  return Number(sorted[index].toFixed(3));
}

function secondsSince(startedAt) {
  return Number(((performance.now() - startedAt) / 1000).toFixed(3));
}

function splitList(value) {
  return value.split(",").map((entry) => entry.trim()).filter(Boolean);
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

function fail(message) {
  console.error(message);
  process.exit(1);
}
