#!/usr/bin/env node
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

const args = parseArgs(process.argv.slice(2));
const config = {
  host: args.host ?? "127.0.0.1",
  port: numberArg(args.port, 8791),
  profile: args.profile ?? "ok",
  latencyMs: numberArg(args.latencyMs, 25),
  jitterMs: numberArg(args.jitterMs, 0),
  throttleEvery: numberArg(args.throttleEvery, 0),
  failureRate: numberArg(args.failureRate, 0),
  retryAfterSeconds: numberArg(args.retryAfterSeconds, 2),
  seed: args.seed === undefined ? null : numberArg(args.seed, 0),
};

if (!["ok", "throttle", "invalid-token", "auth-failure", "flaky"].includes(config.profile)) {
  fail("unknown --profile; expected ok, throttle, invalid-token, auth-failure, or flaky");
}

const stats = {
  startedAt: new Date().toISOString(),
  requests: 0,
  byStatus: {},
  byPath: {},
  latenciesMs: [],
};
const rng = config.seed === null ? Math.random : mulberry32(config.seed);

const server = createServer(async (req, res) => {
  const started = performance.now();
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }

  if (req.method === "GET" && req.url === "/health") {
    return sendJson(res, 200, { ok: true, profile: config.profile });
  }
  if (req.method === "GET" && req.url === "/metrics") {
    return sendJson(res, 200, {
      ...stats,
      config,
      latenciesMs: summarize(stats.latenciesMs),
    });
  }

  stats.requests += 1;
  stats.byPath[req.url] = (stats.byPath[req.url] ?? 0) + 1;
  await sleep(config.latencyMs + rng() * config.jitterMs);

  const response = providerResponse(req, Buffer.concat(chunks).toString("utf8"));
  stats.byStatus[response.status] = (stats.byStatus[response.status] ?? 0) + 1;
  stats.latenciesMs.push(performance.now() - started);

  for (const [key, value] of Object.entries(response.headers)) {
    res.setHeader(key, value);
  }
  sendJson(res, response.status, response.body);
});

server.listen(config.port, config.host, () => {
  console.log(
    JSON.stringify(
      {
        ok: true,
        message: "Sockudo mock push provider listening",
        url: `http://${config.host}:${config.port}`,
        profile: config.profile,
      },
      null,
      2,
    ),
  );
});

function providerResponse(req, rawBody) {
  const forced = forcedResponse();
  if (forced) {
    return forced;
  }

  if (req.url?.includes("/3/device/")) {
    return {
      status: 200,
      headers: { "apns-id": randomUUID() },
      body: {},
    };
  }
  if (req.url?.includes("/messages:send")) {
    return {
      status: 200,
      headers: {},
      body: { name: `projects/mock/messages/${randomUUID()}` },
    };
  }
  if (req.url?.includes("webpush") || req.headers.ttl) {
    return {
      status: 201,
      headers: {},
      body: {},
    };
  }
  if (req.url?.includes("/push/send") || rawBody.includes("validate_only")) {
    return {
      status: 200,
      headers: {},
      body: { code: "80000000", msg: "Success", requestId: randomUUID() },
    };
  }
  if (req.headers["x-wns-type"] || req.url?.includes("notify.windows.com")) {
    return {
      status: 200,
      headers: { "x-wns-notificationstatus": "received" },
      body: {},
    };
  }
  return {
    status: 200,
    headers: {},
    body: { id: randomUUID() },
  };
}

function forcedResponse() {
  if (config.throttleEvery > 0 && stats.requests % config.throttleEvery === 0) {
    return throttled();
  }
  if (config.profile === "throttle") {
    return throttled();
  }
  if (config.profile === "invalid-token") {
    return {
      status: 410,
      headers: {},
      body: { reason: "Unregistered", error: "invalid_token" },
    };
  }
  if (config.profile === "auth-failure") {
    return {
      status: 401,
      headers: {},
      body: { reason: "InvalidProviderToken", error: "auth_failure" },
    };
  }
  if (config.profile === "flaky" && rng() < config.failureRate) {
    return {
      status: 503,
      headers: {},
      body: { error: "mock_unavailable" },
    };
  }
  return null;
}

function throttled() {
  return {
    status: 429,
    headers: { "retry-after": String(config.retryAfterSeconds) },
    body: { error: "mock_rate_limited" },
  };
}

function sendJson(res, status, body) {
  res.writeHead(status, {
    "cache-control": "no-store",
    "content-type": "application/json; charset=utf-8",
  });
  res.end(JSON.stringify(body, null, 2));
}

function summarize(values) {
  if (values.length === 0) {
    return { count: 0, min: 0, p50: 0, p95: 0, p99: 0, max: 0 };
  }
  const sorted = [...values].sort((a, b) => a - b);
  return {
    count: sorted.length,
    min: round(sorted[0]),
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: round(sorted[sorted.length - 1]),
  };
}

function percentile(values, p) {
  const index = Math.ceil((p / 100) * values.length) - 1;
  return round(values[Math.min(values.length - 1, Math.max(0, index))]);
}

function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function round(value) {
  return Math.round(value * 100) / 100;
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

function fail(message) {
  console.error(message);
  process.exit(2);
}
