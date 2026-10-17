// Publishes a steady stream of events through one Sockudo node while a
// subscriber on another node counts what arrives, optionally killing an Apache
// Iggy replica mid-stream. Reports publishes the HTTP API acknowledged but no
// subscriber received (lost), deliveries of rejected publishes, duplicates, and
// latency. Exits non-zero when an acknowledged publish is lost.
//
//   docker compose -f docker-compose.iggy-cluster.yml up -d
//   KILL_COMMAND="docker kill sockudo-iggy-cluster-1" node scripts/e2e_iggy_failover_drill.mjs
//
// Kill the current leader (the `leader` field of the Sockudo
// "cluster topology discovered" log) to exercise an election. `docker kill`
// skips graceful shutdown, so followers only notice the loss once Iggy's
// cluster heartbeat_timeout expires, which is the slow path.

import { exec } from "node:child_process";
import crypto from "node:crypto";
import process from "node:process";

const APP_ID = process.env.APP_ID ?? "demo-app";
const APP_KEY = process.env.APP_KEY ?? "demo-key";
const APP_SECRET = process.env.APP_SECRET ?? "demo-secret";
const EVENT_NAME = process.env.EVENT_NAME ?? "failover-drill";
const WS_URL = process.env.WS_URL ?? `ws://127.0.0.1:6001/app/${APP_KEY}?protocol=2`;
const HTTP_BASE = process.env.HTTP_BASE ?? "http://127.0.0.1:6002";
const DURATION_MS = Number(process.env.DURATION_MS ?? "40000");
const INTERVAL_MS = Number(process.env.INTERVAL_MS ?? "100");
const KILL_AT_MS = Number(process.env.KILL_AT_MS ?? "10000");
const KILL_COMMAND = process.env.KILL_COMMAND ?? "";
const SETTLE_MS = Number(process.env.SETTLE_MS ?? "20000");

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function signedQuery(method, path, body) {
  const params = {
    auth_key: APP_KEY,
    auth_timestamp: Math.floor(Date.now() / 1000).toString(),
    auth_version: "1.0",
    body_md5: crypto.createHash("md5").update(body).digest("hex"),
  };
  const queryForSig = Object.keys(params)
    .sort()
    .map((key) => `${key}=${params[key]}`)
    .join("&");
  const auth_signature = crypto
    .createHmac("sha256", APP_SECRET)
    .update(`${method}\n${path}\n${queryForSig}`)
    .digest("hex");
  return new URLSearchParams({ ...params, auth_signature });
}

async function publish(channel, seq) {
  const path = `/apps/${APP_ID}/events`;
  const body = JSON.stringify({
    channel,
    name: EVENT_NAME,
    data: JSON.stringify({ seq, sent_at: Date.now() }),
  });
  const started = Date.now();
  try {
    const response = await fetch(`${HTTP_BASE}${path}?${signedQuery("POST", path, body)}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body,
    });
    await response.text();
    return { seq, started, ok: response.ok, status: response.status, latency: Date.now() - started };
  } catch (error) {
    return { seq, started, ok: false, status: String(error.cause?.code ?? error), latency: Date.now() - started };
  }
}

function subscribe(channel) {
  const received = new Map();
  let duplicates = 0;
  const ws = new WebSocket(WS_URL);
  const ready = new Promise((resolve, reject) => {
    ws.onerror = (event) => reject(new Error(`websocket error: ${event?.message ?? "unknown"}`));
    ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (payload.event === "sockudo:connection_established") {
        ws.send(JSON.stringify({ event: "sockudo:subscribe", data: { channel } }));
      } else if (payload.event === "sockudo_internal:subscription_succeeded") {
        resolve();
      } else if (payload.event === EVENT_NAME && payload.channel === channel) {
        const data = JSON.parse(payload.data);
        if (received.has(data.seq)) {
          duplicates += 1;
        } else {
          received.set(data.seq, Date.now() - data.sent_at);
        }
      }
    };
  });
  return { ws, ready, received, duplicates: () => duplicates };
}

function percentile(values, p) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

async function main() {
  const channel = `public-iggy-failover-${Date.now()}`;
  const subscriber = subscribe(channel);
  await subscriber.ready;

  const started = Date.now();
  if (KILL_COMMAND) {
    setTimeout(() => {
      console.error(`[+${Date.now() - started}ms] ${KILL_COMMAND}`);
      exec(KILL_COMMAND, (error) => error && console.error(`kill command failed: ${error.message}`));
    }, KILL_AT_MS);
  }

  const publishes = [];
  for (let seq = 0; Date.now() - started < DURATION_MS; seq += 1) {
    publishes.push(publish(channel, seq));
    await wait(INTERVAL_MS);
  }
  const results = await Promise.all(publishes);
  await wait(SETTLE_MS);
  subscriber.ws.close();

  const acked = results.filter((r) => r.ok);
  const rejected = results.filter((r) => !r.ok);
  const lost = acked.filter((r) => !subscriber.received.has(r.seq)).map((r) => r.seq);
  const deliveredDespiteError = rejected.filter((r) => subscriber.received.has(r.seq)).length;
  const deliveryDelays = [...subscriber.received.values()];
  const failureStatuses = {};
  for (const r of rejected) failureStatuses[r.status] = (failureStatuses[r.status] ?? 0) + 1;

  const report = {
    channel,
    published: results.length,
    acked: acked.length,
    rejected: rejected.length,
    rejected_by_status: failureStatuses,
    received: subscriber.received.size,
    lost_acked: lost.length,
    lost_acked_seqs: lost.slice(0, 20),
    delivered_despite_error: deliveredDespiteError,
    duplicates: subscriber.duplicates(),
    publish_latency_ms: {
      p50: percentile(results.map((r) => r.latency), 50),
      p99: percentile(results.map((r) => r.latency), 99),
      max: Math.max(...results.map((r) => r.latency)),
    },
    delivery_delay_ms: {
      p50: percentile(deliveryDelays, 50),
      p99: percentile(deliveryDelays, 99),
      max: deliveryDelays.length ? Math.max(...deliveryDelays) : 0,
    },
    rejected_window_ms: rejected.length
      ? [Math.min(...rejected.map((r) => r.started - started)), Math.max(...rejected.map((r) => r.started - started))]
      : null,
  };
  console.log(JSON.stringify(report, null, 2));
  if (lost.length > 0) process.exit(1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
