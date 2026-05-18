#!/usr/bin/env node
import crypto from "node:crypto";
import { performance } from "node:perf_hooks";

const args = parseArgs(process.argv.slice(2));
const provider = normalizeProvider(args.provider ?? "fcm");

const config = {
  baseUrl: args.url ?? "http://127.0.0.1:6001",
  appId: args.appId ?? "app-id",
  key: args.key ?? "app-key",
  secret: args.secret ?? "app-secret",
  mode: args.mode ?? "admission",
  provider,
  devices: numberArg(args.devices, 10_000),
  channels: numberArg(args.channels, 100),
  concurrency: numberArg(args.concurrency, 50),
  durationSeconds: numberArg(args.duration, 60),
  statusSample: numberArg(args.statusSample, 100),
  prefix: args.prefix ?? `push-bench-${Date.now()}`,
  payloadBytes: numberArg(args.payloadBytes, 256),
  hotChannel: booleanArg(args.hotChannel, false),
};

if (!["seed", "admission", "e2e", "all"].includes(config.mode)) {
  fail(`unknown --mode ${config.mode}; expected seed, admission, e2e, or all`);
}

const runStartedAt = new Date().toISOString();
const summary = {
  startedAt: runStartedAt,
  config: redactConfig(config),
};

if (config.mode === "seed" || config.mode === "all") {
  summary.seed = await seedRegistry();
}
if (config.mode === "admission" || config.mode === "e2e" || config.mode === "all") {
  summary.admission = await runAdmission();
}
if (config.mode === "e2e" || config.mode === "all") {
  summary.status = await pollStatusSamples(summary.admission.publishIds);
}

console.log(JSON.stringify(summary, null, 2));

async function seedRegistry() {
  const started = performance.now();
  const counters = { devices: 0, subscriptions: 0, failures: 0 };
  await runPool(config.devices, config.concurrency, async (index) => {
    const deviceId = deviceIdFor(index);
    const channel = channelFor(index);
    try {
      const activation = await signedJson("POST", "/push/deviceRegistrations", {
        appId: config.appId,
        id: deviceId,
        deviceSecret: `${config.prefix}-secret-${index}`,
        formFactor: formFactorForProvider(config.provider),
        platform: platformForProvider(config.provider),
        timezone: "UTC",
        locale: "en",
        push: {
          state: "ACTIVE",
          recipient: recipientForProvider(index),
        },
      }, {
        "x-sockudo-push-capability": "push-admin",
        "x-sockudo-rotate-device-identity-token": "true",
      });
      counters.devices += 1;
      await signedJson("POST", "/push/channelSubscriptions", {
        appId: config.appId,
        channel,
        deviceId,
        provider: config.provider,
        tokenHash: activation.tokenHash,
        credentialVersion: 1,
      }, {
        "x-sockudo-push-capability": "push-admin",
      });
      counters.subscriptions += 1;
    } catch (error) {
      counters.failures += 1;
      if (args.verbose) {
        console.error(`seed failed for ${deviceId}: ${error.message}`);
      }
    }
  });

  return {
    ...counters,
    durationSeconds: secondsSince(started),
    devicesPerSecond: rate(counters.devices, started),
    subscriptionsPerSecond: rate(counters.subscriptions, started),
  };
}

async function runAdmission() {
  const started = performance.now();
  const stopAt = started + config.durationSeconds * 1_000;
  const latencies = [];
  const publishIds = [];
  let accepted = 0;
  let failed = 0;
  let next = 0;

  await Promise.all(Array.from({ length: config.concurrency }, async () => {
    while (performance.now() < stopAt) {
      const index = next++;
      const channel = config.hotChannel ? channelFor(0) : channelFor(index);
      const requestStarted = performance.now();
      try {
        const response = await signedJson("POST", "/push/publish", {
          publishId: `${config.prefix}-publish-${index}`,
          recipients: [{ type: "channel", channel }],
          payload: payloadFor(index),
          providerOverrides: [],
          sync: false,
        }, {
          "x-sockudo-push-capability": "push-admin",
        });
        latencies.push(performance.now() - requestStarted);
        accepted += 1;
        if (publishIds.length < config.statusSample) {
          publishIds.push(response.publishId ?? response.publish_id);
        }
      } catch (error) {
        failed += 1;
        latencies.push(performance.now() - requestStarted);
        if (args.verbose) {
          console.error(`publish failed: ${error.message}`);
        }
      }
    }
  }));

  latencies.sort((a, b) => a - b);
  return {
    accepted,
    failed,
    durationSeconds: secondsSince(started),
    requestsPerSecond: rate(accepted, started),
    publishIds: publishIds.filter(Boolean),
    latencyMs: {
      min: percentile(latencies, 0),
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      p99: percentile(latencies, 99),
      max: percentile(latencies, 100),
    },
  };
}

async function pollStatusSamples(publishIds) {
  const started = performance.now();
  const states = {};
  let planned = 0;
  let dispatched = 0;
  let failed = 0;

  await runPool(publishIds.length, config.concurrency, async (index) => {
    const status = await signedJson("GET", `/push/publish/${encodeURIComponent(publishIds[index])}/status`, undefined, {
      "x-sockudo-push-capability": "push-admin",
    });
    states[status.state] = (states[status.state] ?? 0) + 1;
    planned += status.counters?.planned ?? 0;
    dispatched += status.counters?.dispatched ?? 0;
    failed += status.counters?.failed ?? 0;
  });

  return {
    sampled: publishIds.length,
    durationSeconds: secondsSince(started),
    states,
    counters: { planned, dispatched, failed },
  };
}

async function signedJson(method, pushPath, body, headers = {}) {
  const fullPath = `/apps/${config.appId}${pushPath}`;
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
    headers: {
      "content-type": "application/json",
      ...headers,
    },
    body: bodyText,
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

async function runPool(total, concurrency, task) {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(total, concurrency) }, async () => {
    while (true) {
      const index = next++;
      if (index >= total) {
        return;
      }
      await task(index);
    }
  }));
}

function payloadFor(index) {
  const fillerBytes = Math.max(0, config.payloadBytes - 64);
  return {
    title: "Benchmark",
    body: `Push benchmark message ${index}`,
    templateData: {
      index,
      filler: "x".repeat(fillerBytes),
    },
  };
}

function recipientForProvider(index) {
  const token = `${config.prefix}-token-${index}`;
  switch (config.provider) {
    case "fcm":
      return {
        transportType: "gcm",
        registrationToken: token,
      };
    case "apns":
      return {
        transportType: "apns",
        deviceToken: token,
      };
    case "webPush":
      return {
        transportType: "web",
        endpoint: `https://example.com/sockudo-push-benchmark/${config.prefix}/${index}`,
        p256dh: "BJ6eU_eWmn18g4rB7ADTJMbI1haNmKUOEb_rUsvg43xzsRLgUwt3qP8F-s9Jw6UFbl4dIhxOr3GMt5I-IJjVTPw",
        auth: "CX-4I1SF2WaUs9IcL21dUg",
      };
    case "hms":
      return {
        transportType: "hms",
        registrationToken: token,
      };
    case "wns":
      return {
        transportType: "wns",
        channelUri: `https://notify.windows.com/?token=${encodeURIComponent(token)}`,
      };
    default:
      fail(`unsupported provider: ${config.provider}`);
  }
}

function platformForProvider(provider) {
  switch (provider) {
    case "fcm":
      return "android";
    case "apns":
      return "ios";
    case "webPush":
      return "browser";
    case "hms":
      return "android";
    case "wns":
      return "windows";
    default:
      return "other";
  }
}

function formFactorForProvider(provider) {
  return provider === "webPush" || provider === "wns" ? "desktop" : "phone";
}

function deviceIdFor(index) {
  return `${config.prefix}-device-${index}`;
}

function channelFor(index) {
  return `${config.prefix}-channel-${index % config.channels}`;
}

function percentile(values, p) {
  if (values.length === 0) {
    return 0;
  }
  const index = Math.ceil((p / 100) * values.length) - 1;
  return round(values[Math.min(values.length - 1, Math.max(0, index))]);
}

function rate(count, started) {
  return round(count / secondsSince(started));
}

function secondsSince(started) {
  return Math.max((performance.now() - started) / 1_000, 0.001);
}

function round(value) {
  return Math.round(value * 100) / 100;
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
  if (!Number.isFinite(parsed) || parsed < 0) {
    fail(`invalid numeric argument: ${value}`);
  }
  return parsed;
}

function booleanArg(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  return value === true || value === "true" || value === "1";
}

function normalizeProvider(value) {
  const normalized = String(value).toLowerCase();
  switch (normalized) {
    case "fcm":
    case "gcm":
      return "fcm";
    case "apns":
    case "apple":
      return "apns";
    case "webpush":
    case "web-push":
    case "web_push":
      return "webPush";
    case "hms":
      return "hms";
    case "wns":
      return "wns";
    default:
      fail(`unknown --provider ${value}; expected fcm, apns, webpush, hms, or wns`);
  }
}

function redactConfig(input) {
  return {
    ...input,
    secret: "[REDACTED]",
  };
}

function fail(message) {
  console.error(message);
  process.exit(2);
}
