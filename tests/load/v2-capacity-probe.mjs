#!/usr/bin/env node
// Memory per connection, publish latency, and overload behavior for one
// Sockudo node with durable history, the shape measured in sockudo/sockudo#533.
// Raw Protocol V2 WebSockets on private channels; no npm dependencies (Node 22+).
//
//   node tests/load/v2-capacity-probe.mjs
//
// Environment:
//   HOST            server host:port (default 127.0.0.1:6001)
//   CONTAINER       read CPU and memory from this container's cgroup (docker exec)
//   PID             or from this local process (/proc, Linux only)
//   STEPS           idle connection steps, e.g. 500,1000,2000 (default)
//   CHANNELS        private channels, client i joins channel i % CHANNELS (default 20)
//   TICK_MS         one publish per channel every TICK_MS; 2000/500/250 ms with
//                   2,000 clients give 1,000/4,000/8,000 deliveries/s (default 2000)
//   DURATION_MS     publish phase length (default 10000); PUBLISH=0 skips it
//   CLIENT_TIMEOUT_MS  publish request timeout, counted as status 0 (default 2000)
import { createHash, createHmac } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

const HOST = process.env.HOST ?? "127.0.0.1:6001";
const KEY = "probe-key", SECRET = "probe-secret", APP_ID = "probe-app";
const STEPS = (process.env.STEPS ?? "500,1000,2000").split(",").map(Number);
const CHANNELS = Number(process.env.CHANNELS ?? 20);
const TICK_MS = Number(process.env.TICK_MS ?? 2000);
const DURATION_MS = Number(process.env.DURATION_MS ?? 10_000);
const CLIENT_TIMEOUT_MS = Number(process.env.CLIENT_TIMEOUT_MS ?? 2000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// { cpuUsec, memKiB } of the server, or null when neither CONTAINER nor PID is set.
function sample() {
  if (process.env.CONTAINER) {
    const out = execFileSync("docker", ["exec", process.env.CONTAINER, "cat",
      "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/memory.current"], { encoding: "utf8" });
    const cpuUsec = Number(/usage_usec (\d+)/.exec(out)[1]);
    const memKiB = Number(out.trim().split("\n").at(-1)) / 1024;
    return { cpuUsec, memKiB };
  }
  if (process.env.PID) {
    const pid = process.env.PID;
    const f = readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1].split(" ");
    const memKiB = Number(/VmRSS:\s+(\d+)/.exec(readFileSync(`/proc/${pid}/status`, "utf8"))[1]);
    return { cpuUsec: (Number(f[11]) + Number(f[12])) * 10_000, memKiB };
  }
  return null;
}

const sockets = [];
let subscribed = 0; // open sockets whose subscription succeeded
let opened = 0; // monotonic, so channel assignment stays stable
const subscribedByChannel = new Array(CHANNELS).fill(0);
const arrivals = new Map();

function open() {
  const i = opened++;
  const ws = new WebSocket(`ws://${HOST}/app/${KEY}?protocol=2`);
  const channel = `private-probe-${i % CHANNELS}`;
  let didSubscribe = false;
  ws.onmessage = (ev) => {
    const msg = JSON.parse(String(ev.data));
    if (msg.event === "sockudo:connection_established") {
      const socketId = JSON.parse(msg.data).socket_id;
      const sig = createHmac("sha256", SECRET).update(`${socketId}:${channel}`).digest("hex");
      ws.send(JSON.stringify({ event: "sockudo:subscribe", data: { channel, auth: `${KEY}:${sig}` } }));
    } else if (msg.event === "sockudo_internal:subscription_succeeded") {
      if (!didSubscribe) {
        didSubscribe = true;
        subscribed++;
        subscribedByChannel[i % CHANNELS]++;
      }
    } else if (msg.event === "probe") {
      const s = JSON.parse(msg.data).s;
      if (!arrivals.has(s)) arrivals.set(s, []);
      arrivals.get(s).push(Date.now());
    }
  };
  ws.onclose = () => {
    const at = sockets.indexOf(ws);
    if (at >= 0) sockets.splice(at, 1);
    if (didSubscribe) {
      didSubscribe = false;
      subscribed--;
      subscribedByChannel[i % CHANNELS]--;
    }
  };
  sockets.push(ws);
}

async function publish(channel, s) {
  const path = `/apps/${APP_ID}/events`;
  const body = JSON.stringify({ name: "probe", channels: [channel], data: JSON.stringify({ s }) });
  const params = {
    auth_key: KEY,
    auth_timestamp: String(Math.floor(Date.now() / 1000)),
    auth_version: "1.0",
    body_md5: createHash("md5").update(body).digest("hex"),
  };
  const query = Object.keys(params).sort().map((k) => `${k}=${params[k]}`).join("&");
  const sig = createHmac("sha256", SECRET).update(`POST\n${path}\n${query}`).digest("hex");
  try {
    const res = await fetch(`http://${HOST}${path}?${query}&auth_signature=${sig}`, {
      method: "POST", headers: { "content-type": "application/json" }, body,
      signal: AbortSignal.timeout(CLIENT_TIMEOUT_MS),
    });
    await res.arrayBuffer();
    return res.status;
  } catch {
    return 0;
  }
}

const pct = (xs, q) => {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.min(s.length - 1, Math.ceil(q * s.length) - 1)] : NaN;
};
const fmtMem = (kib) => `${(kib / 1024).toFixed(1)} MiB`;

const base = sample();
if (base) console.log(`idle memory ${fmtMem(base.memKiB)}`);
let prev = { n: 0, mem: base?.memKiB };
for (const n of STEPS) {
  const growDeadline = Date.now() + 60_000;
  while (sockets.length < n && Date.now() < growDeadline) {
    open();
    if (opened % 200 === 0) await sleep(250);
  }
  const t = Date.now();
  while (subscribed < n && Date.now() - t < 60_000) await sleep(100);
  await sleep(3000);
  const now = sample();
  if (subscribed < n) {
    console.log(`${n} requested, ${sockets.length} open, ${subscribed} subscribed: target not reached, no memory sample`);
    prev = { n: sockets.length, mem: now?.memKiB ?? prev.mem };
    continue;
  }
  const perStep = now && subscribed > prev.n && prev.mem != null
    ? `${((now.memKiB - prev.mem) / (subscribed - prev.n)).toFixed(1)} KiB/conn over the previous step, `
    : "";
  const mem = now
    ? `, memory ${fmtMem(now.memKiB)}, ${perStep}${((now.memKiB - base.memKiB) / subscribed).toFixed(1)} KiB/conn overall`
    : "";
  console.log(`${n} connections, ${subscribed} subscribed${mem}`);
  prev = { n: subscribed, mem: now?.memKiB };
}

if (process.env.PUBLISH !== "0") {
  const sent = [], statuses = {}, byStatus = {};
  const before = sample(), t0 = Date.now(), offered = subscribed;
  const end = t0 + DURATION_MS;
  const inflight = [];
  let seq = 0;
  while (Date.now() < end) {
    for (let k = 0; k < CHANNELS; k++) {
      const s = seq++, at = Date.now(), subs = subscribedByChannel[k];
      inflight.push(publish(`private-probe-${k}`, s).then((st) => {
        statuses[st] = (statuses[st] ?? 0) + 1;
        (byStatus[st] ??= []).push(Date.now() - at);
        sent.push({ s, at, k, subs });
      }));
    }
    await sleep(TICK_MS);
  }
  await Promise.all(inflight);
  const after = sample(), elapsed = Date.now() - t0;
  await sleep(1500);
  const lat = [];
  let expected = 0;
  for (const p of sent) {
    expected += p.subs;
    for (const t of arrivals.get(p.s) ?? []) lat.push(t - p.at);
  }
  const deliveriesPerSec = (offered * 1000) / TICK_MS;
  console.log(`publish phase: ${deliveriesPerSec.toFixed(0)} deliveries/s offered for ${DURATION_MS} ms`);
  if (before && after) {
    console.log(`server cpu ${((after.cpuUsec - before.cpuUsec) / elapsed).toFixed(0)}m`);
  }
  console.log(`publish statuses (0 = client timeout) ${JSON.stringify(statuses)}`);
  for (const [st, xs] of Object.entries(byStatus)) {
    console.log(`  status ${st}: ${xs.length} requests, p50/p95/p99 ${pct(xs, 0.5)}/${pct(xs, 0.95)}/${pct(xs, 0.99)} ms`);
  }
  console.log(`delivery p50/p95/p99 ${pct(lat, 0.5)}/${pct(lat, 0.95)}/${pct(lat, 0.99)} ms, delivered ${lat.length}/${expected}`);
}
for (const ws of [...sockets]) ws.close();
process.exit(0);
