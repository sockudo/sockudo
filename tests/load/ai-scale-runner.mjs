#!/usr/bin/env node
import crypto from "node:crypto";
import fs from "node:fs/promises";
import os from "node:os";
import { performance } from "node:perf_hooks";

const args = parseArgs(process.argv.slice(2));
const profile = await loadProfile(args.profile ?? "test/load/profiles/smoke.json");
const config = normalizeConfig(profile, args);
const runId = args.runId ?? `${Date.now().toString(36)}-${process.pid.toString(36)}`;
const started = performance.now();

const plan = buildPlan(config);
const metricsBefore = await collectMetrics(config.metricsUrls);
let workload = { skipped: true, reason: "plan_only" };

if (!config.planOnly) {
  workload = await runExecutableWorkload(config, runId);
}

const metricsAfter = await collectMetrics(config.metricsUrls);
const result = {
  schema: "sockudo.ai-transport.scale-result.v1",
  status: config.planOnly ? "planned" : workload.failures === 0 ? "passed" : "failed",
  runId,
  generatedAt: new Date().toISOString(),
  profile: config.name,
  host: {
    hostname: os.hostname(),
    platform: os.platform(),
    release: os.release(),
    cpus: os.cpus().length,
    totalMemoryBytes: os.totalmem()
  },
  configuration: publicConfig(config),
  plan,
  metrics: {
    before: metricsBefore,
    after: metricsAfter
  },
  workload,
  durationSeconds: secondsSince(started)
};

const text = `${JSON.stringify(result, null, 2)}\n`;
if (args.output) {
  await fs.mkdir(dirname(args.output), { recursive: true });
  await fs.writeFile(args.output, text);
}
process.stdout.write(text);

if (!config.planOnly && workload.failures !== 0) {
  process.exitCode = 1;
}
if (!config.planOnly && workload.appendToLatestLatencyMs.p99 !== undefined && workload.appendToLatestLatencyMs.p99 > config.maxAppendDeliveryP99Ms) {
  console.error(`append->latest p99 ${workload.appendToLatestLatencyMs.p99}ms exceeded ${config.maxAppendDeliveryP99Ms}ms`);
  process.exitCode = 1;
}

async function runExecutableWorkload(config, runId) {
  const latencies = [];
  const latestLatencies = [];
  const serials = [];
  const transcripts = [];
  const failureSamples = [];
  let failures = 0;
  let appendsAttempted = 0;
  const streams = [];

  for (let streamIndex = 0; streamIndex < config.activeStreams; streamIndex += 1) {
    const channel = `${config.channelPrefix}-${config.name}-${runId}-${streamIndex}`;
    const node = config.urls[streamIndex % config.urls.length];
    try {
      const messageSerial = await createMessage(config, node, channel);
      streams.push({ streamIndex, channel, messageSerial, expected: "" });
      serials.push(messageSerial);
    } catch (error) {
      failures += 1;
      recordFailure(failureSamples, {
        stage: "create",
        streamIndex,
        error: boundedError(error)
      });
      logVerbose(`create failed stream=${streamIndex}: ${error.message}`);
    }
  }

  const maxAppendsByTokens = Math.max(1, Math.ceil(config.meanResponseTokens));
  const appendsPerStream = Math.max(1, Math.min(maxAppendsByTokens, Math.ceil(config.tokensPerSecond * config.durationSeconds)));
  for (let appendIndex = 0; appendIndex < appendsPerStream; appendIndex += 1) {
    const targetTime = started + (appendIndex * 1000) / Math.max(1, config.tokensPerSecond);
    const waitMs = targetTime - performance.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }

    await Promise.all(streams.map(async (stream) => {
      const node = config.urls[(stream.streamIndex + appendIndex) % config.urls.length];
      const fragment = tokenFor(stream.streamIndex, appendIndex, config.tokenBytes);
      appendsAttempted += 1;
      const requestStarted = performance.now();
      try {
        await signedJson(config, node, "POST", `/channels/${encodeURIComponent(stream.channel)}/messages/${encodeURIComponent(stream.messageSerial)}/append`, {
          data: fragment,
          op_id: `${runId}-${stream.streamIndex}-${appendIndex}`,
          extras: {
            ai: {
              transport: {
                status: appendIndex + 1 === appendsPerStream ? "complete" : "streaming"
              }
            }
          }
        });
        stream.expected += fragment;
        latencies.push(performance.now() - requestStarted);
      } catch (error) {
        failures += 1;
        recordFailure(failureSamples, {
          stage: "append",
          streamIndex: stream.streamIndex,
          appendIndex,
          error: boundedError(error)
        });
        logVerbose(`append failed stream=${stream.streamIndex} append=${appendIndex}: ${error.message}`);
      }
    }));
  }

  const samples = streams.slice(0, Math.min(config.sampleTranscriptCount, streams.length));
  for (const stream of samples) {
    const node = config.urls[stream.streamIndex % config.urls.length];
    const requestStarted = performance.now();
    try {
      const latest = await signedJson(config, node, "GET", `/channels/${encodeURIComponent(stream.channel)}/messages/${encodeURIComponent(stream.messageSerial)}`);
      const elapsed = performance.now() - requestStarted;
      latestLatencies.push(elapsed);
      const actual = latest?.item?.data;
      const matches = actual === stream.expected;
      if (!matches) {
        failures += 1;
      }
      transcripts.push({
        channel: stream.channel,
        messageSerial: stream.messageSerial,
        expectedBytes: Buffer.byteLength(stream.expected),
        actualBytes: actual === undefined ? null : Buffer.byteLength(String(actual)),
        matches
      });
    } catch (error) {
      failures += 1;
      recordFailure(failureSamples, {
        stage: "latest",
        streamIndex: stream.streamIndex,
        error: boundedError(error)
      });
      logVerbose(`latest failed stream=${stream.streamIndex}: ${error.message}`);
    }
  }

  return {
    skipped: false,
    appendsAttempted,
    failures,
    failureSamples,
    appendRequestLatencyMs: summarize(latencies),
    appendToLatestLatencyMs: summarize(latestLatencies),
    serialAudit: auditSerials(serials),
    transcriptAudit: {
      sampled: transcripts.length,
      mismatches: transcripts.filter((entry) => !entry.matches).length,
      samples: transcripts
    }
  };
}

function recordFailure(samples, failure) {
  if (samples.length < 64) {
    samples.push(failure);
  }
}

function boundedError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return message.length <= 512 ? message : `${message.slice(0, 509)}...`;
}

async function createMessage(config, baseUrl, channel) {
  const response = await signedJson(config, baseUrl, "POST", "/events", {
    name: "ai-output",
    channel,
    data: "",
    extras: {
      ai: {
        transport: {
          status: "streaming",
          role: "assistant"
        }
      }
    }
  });
  const messageSerial = response?.channels?.[channel]?.message_serial;
  if (!messageSerial) {
    throw new Error("create response did not include message_serial");
  }
  return messageSerial;
}

async function signedJson(config, baseUrl, method, path, body) {
  const fullPath = `/apps/${config.appId}${path}`;
  const bodyText = body === undefined ? undefined : JSON.stringify(body);
  const timestamp = Math.floor((Date.now() + config.clientClockSkewMs) / 1000);
  const query = {
    auth_key: config.key,
    auth_timestamp: `${timestamp}`,
    auth_version: "1.0"
  };
  if (bodyText !== undefined) {
    query.body_md5 = crypto.createHash("md5").update(bodyText).digest("hex");
  }

  const canonicalQuery = Object.keys(query)
    .map((key) => [key.toLowerCase(), query[key]])
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  query.auth_signature = crypto
    .createHmac("sha256", config.secret)
    .update(`${method}\n${fullPath}\n${canonicalQuery}`)
    .digest("hex");

  const url = new URL(`${baseUrl}${fullPath}`);
  for (const [key, value] of Object.entries(query)) {
    url.searchParams.set(key, value);
  }
  const response = await fetch(url, {
    method,
    headers: { "content-type": "application/json" },
    body: bodyText,
    signal: AbortSignal.timeout(config.requestTimeoutMs)
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

async function collectMetrics(metricsUrls) {
  const snapshots = [];
  for (const url of metricsUrls) {
    try {
      const response = await fetch(url);
      const text = await response.text();
      snapshots.push({
        url,
        ok: response.ok,
        bytes: Buffer.byteLength(text),
        interesting: extractInterestingMetrics(text)
      });
    } catch (error) {
      snapshots.push({ url, ok: false, error: error.message });
    }
  }
  return snapshots;
}

function extractInterestingMetrics(text) {
  const names = [
    "sockudo_connected",
    "sockudo_new_connections_total",
    "sockudo_new_disconnections_total",
    "sockudo_rate_limit_triggered_total",
    "sockudo_broadcast_latency_ms",
    "sockudo_ai_active_streams",
    "sockudo_appends_received_total",
    "sockudo_appends_delivered_total",
    "sockudo_flush_latency",
    "sockudo_history_recovery_success_total",
    "sockudo_history_recovery_failures_total",
    "sockudo_horizontal_transport_messages_dropped_total",
    "sockudo_horizontal_transport_reconnections_total"
  ];
  const lines = text.split("\n").filter((line) => names.some((name) => line.startsWith(name)));
  return lines.slice(0, 200);
}

function buildPlan(config) {
  const loadGenerators = Math.max(1, config.loadGenerators);
  const appendsPerStream = Math.max(1, Math.ceil(config.meanResponseTokens));
  const appendRate = config.activeStreams * config.tokensPerSecond;
  const fanoutMin = appendRate * config.subscribersPerSession.min;
  const fanoutMax = appendRate * config.subscribersPerSession.max;
  const perGenerator = {
    logicalConnections: Math.ceil(config.logicalConnections / loadGenerators),
    sessions: Math.ceil(config.sessions / loadGenerators),
    activeStreams: Math.ceil(config.activeStreams / loadGenerators),
    appendRatePerSecond: Math.ceil(appendRate / loadGenerators),
    fanoutPerSecondMin: Math.ceil(fanoutMin / loadGenerators),
    fanoutPerSecondMax: Math.ceil(fanoutMax / loadGenerators)
  };
  return {
    nodeCount: config.urls.length,
    metricsNodeCount: config.metricsUrls.length,
    loadGenerators,
    appendsPerStream,
    aggregate: {
      appendRatePerSecond: appendRate,
      fanoutPerSecondMin: fanoutMin,
      fanoutPerSecondMax: fanoutMax,
      expectedTotalAppends: config.activeStreams * appendsPerStream,
      expectedSubscriberDevicesMin: config.sessions * config.subscribersPerSession.min,
      expectedSubscriberDevicesMax: config.sessions * config.subscribersPerSession.max
    },
    perNode: {
      logicalConnections: Math.ceil(config.logicalConnections / config.urls.length),
      activeStreams: Math.ceil(config.activeStreams / config.urls.length),
      appendRatePerSecond: Math.ceil(appendRate / config.urls.length)
    },
    perGenerator
  };
}

function normalizeConfig(profile, args) {
  const urls = splitList(args.urls ?? process.env.SOCKUDO_NODE_URLS ?? profile.urls ?? "http://127.0.0.1:6001,http://127.0.0.1:6002,http://127.0.0.1:6003");
  const metricsUrls = splitList(args.metricsUrls ?? process.env.SOCKUDO_METRICS_URLS ?? profile.metricsUrls ?? "http://127.0.0.1:9601/metrics,http://127.0.0.1:9602/metrics,http://127.0.0.1:9603/metrics");
  const execute = Boolean(args.execute);
  const planOnly = Boolean(args.plan) || (!execute && Boolean(profile.planOnly));
  return {
    ...profile,
    name: args.name ?? profile.name ?? "ai-scale",
    urls,
    metricsUrls,
    appId: args.appId ?? process.env.SOCKUDO_APP_ID ?? profile.appId ?? "app-id",
    key: args.key ?? process.env.SOCKUDO_APP_KEY ?? profile.key ?? "app-key",
    secret: args.secret ?? process.env.SOCKUDO_APP_SECRET ?? profile.secret ?? "app-secret",
    channelPrefix: args.channelPrefix ?? profile.channelPrefix ?? "private-ai-scale",
    logicalConnections: numberArg(args.logicalConnections, profile.logicalConnections ?? 1000),
    sessions: numberArg(args.sessions, profile.sessions ?? 100),
    activeStreams: numberArg(args.activeStreams, profile.activeStreams ?? 10),
    durationSeconds: numberArg(args.durationSeconds ?? args.duration, profile.durationSeconds ?? 10),
    tokensPerSecond: numberArg(args.tokensPerSecond, profile.tokensPerSecond ?? 100),
    meanResponseTokens: numberArg(args.meanResponseTokens, profile.meanResponseTokens ?? 100),
    tokenBytes: numberArg(args.tokenBytes, profile.tokenBytes ?? 8),
    cancelProbability: numberArg(args.cancelProbability, profile.cancelProbability ?? 0),
    reconnectProbability: numberArg(args.reconnectProbability, profile.reconnectProbability ?? 0),
    historyOnJoinProbability: numberArg(args.historyOnJoinProbability, profile.historyOnJoinProbability ?? 0),
    sampleTranscriptCount: numberArg(args.sampleTranscriptCount, profile.sampleTranscriptCount ?? 10),
    maxAppendDeliveryP99Ms: numberArg(args.maxAppendDeliveryP99Ms, profile.maxAppendDeliveryP99Ms ?? 25),
    requestTimeoutMs: numberArg(args.requestTimeoutMs, profile.requestTimeoutMs ?? 30000),
    clientClockSkewMs: signedNumberArg(args.clientClockSkewMs, profile.clientClockSkewMs ?? 0),
    loadGenerators: numberArg(args.loadGenerators, profile.loadGenerators ?? 1),
    subscribersPerSession: {
      min: numberArg(args.subscribersMin, profile.subscribersPerSession?.min ?? 1),
      max: numberArg(args.subscribersMax, profile.subscribersPerSession?.max ?? 1)
    },
    planOnly
  };
}

async function loadProfile(path) {
  const text = await fs.readFile(path, "utf8");
  return JSON.parse(text);
}

function publicConfig(config) {
  const { secret: _secret, key: _key, ...rest } = config;
  return { ...rest, key: "<redacted>" };
}

function auditSerials(serials) {
  const numeric = serials.map((value) => Number(value)).filter(Number.isFinite);
  let monotonic = true;
  for (let index = 1; index < numeric.length; index += 1) {
    if (numeric[index] < numeric[index - 1]) {
      monotonic = false;
      break;
    }
  }
  return {
    sampled: serials.length,
    numeric: numeric.length,
    monotonic
  };
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
    max: Number(sorted.at(-1).toFixed(3))
  };
}

function percentile(sorted, fraction) {
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * fraction));
  return Number(sorted[index].toFixed(3));
}

function splitList(value) {
  if (Array.isArray(value)) {
    return value;
  }
  return String(value).split(",").map((entry) => entry.trim()).filter(Boolean);
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

function signedNumberArg(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`invalid numeric argument: ${value}`);
  }
  return parsed;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function secondsSince(startedAt) {
  return Number(((performance.now() - startedAt) / 1000).toFixed(3));
}

function dirname(path) {
  const parts = path.split("/");
  parts.pop();
  return parts.join("/") || ".";
}

function logVerbose(message) {
  if (args.verbose) {
    console.error(message);
  }
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
