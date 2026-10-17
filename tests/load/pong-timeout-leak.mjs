#!/usr/bin/env node
// Pong-timeout cleanup leak reproducer and benchmark.
//
// Opens Pusher protocol V1 connections that join a presence channel each, then
// stop answering `pusher:ping` so the server's activity-timeout task fires the
// pong timeout (close 4201) and runs disconnect cleanup. After every round it
// compares what the server still tracks (connected gauge, presence channels,
// tokio tasks, RSS) against what really exists (zero client sockets).
//
// Peer models:
//   throttled-tab  JS is frozen (no pong) but the network stack is alive and
//                  answers the server's Close frame at once, like a background
//                  browser tab. The reader then races the timeout task's cleanup.
//   dead           The peer stops reading entirely (NAT drop, sleeping phone).
//   clean          Control and cleanup benchmark: every client closes normally
//                  at once; reports how long the server takes to finish cleanup.
//
// Run the server with tests/load/pong-timeout-leak.toml (activity_timeout = 5, so a
// timeout round takes about 35 s with the 30 s PONG_TIMEOUT). See README.md.
//
//   node tests/load/pong-timeout-leak.mjs --connections 2000 --rounds 3 \
//     --pid "$(pgrep -n sockudo)"

import crypto from 'node:crypto';
import net from 'node:net';
import { execFileSync } from 'node:child_process';
import { parseArgs } from 'node:util';

const { values: opts } = parseArgs({
  options: {
    host: { type: 'string', default: '127.0.0.1' },
    port: { type: 'string', default: '6001' },
    'metrics-url': { type: 'string', default: 'http://127.0.0.1:9601/metrics' },
    key: { type: 'string', default: 'app-key' },
    secret: { type: 'string', default: 'app-secret' },
    connections: { type: 'string', default: '1000' },
    rounds: { type: 'string', default: '3' },
    concurrency: { type: 'string', default: '200' },
    peer: { type: 'string', default: 'throttled-tab' },
    'settle-seconds': { type: 'string', default: '10' },
    'round-timeout-seconds': { type: 'string', default: '180' },
    pid: { type: 'string' },
    label: { type: 'string', default: '' },
    json: { type: 'boolean', default: false },
  },
});

const PORT = Number(opts.port);
const CONNECTIONS = Number(opts.connections);
const ROUNDS = Number(opts.rounds);
const CONCURRENCY = Number(opts.concurrency);
const SETTLE_MS = Number(opts['settle-seconds']) * 1000;
const ROUND_TIMEOUT_MS = Number(opts['round-timeout-seconds']) * 1000;
if (!['throttled-tab', 'dead', 'clean'].includes(opts.peer)) {
  throw new Error(`unknown --peer ${opts.peer}`);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const hmac = (payload) =>
  crypto.createHmac('sha256', opts.secret).update(payload).digest('hex');

// --- Minimal RFC 6455 client: enough framing to control exactly what the peer answers.

function encodeFrame(opcode, payload) {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const mask = crypto.randomBytes(4);
  let header;
  if (body.length < 126) {
    header = Buffer.from([0x80 | opcode, 0x80 | body.length]);
  } else if (body.length < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(body.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(body.length), 2);
  }
  const masked = Buffer.alloc(body.length);
  for (let i = 0; i < body.length; i++) masked[i] = body[i] ^ mask[i % 4];
  return Buffer.concat([header, mask, masked]);
}

function* decodeFrames(state) {
  for (;;) {
    const buf = state.buffer;
    if (buf.length < 2) return;
    const opcode = buf[0] & 0x0f;
    let len = buf[1] & 0x7f;
    let offset = 2;
    if (len === 126) {
      if (buf.length < 4) return;
      len = buf.readUInt16BE(2);
      offset = 4;
    } else if (len === 127) {
      if (buf.length < 10) return;
      len = Number(buf.readBigUInt64BE(2));
      offset = 10;
    }
    if (buf.length < offset + len) return;
    state.buffer = buf.subarray(offset + len);
    yield { opcode, payload: buf.subarray(offset, offset + len) };
  }
}

class Client {
  constructor(index) {
    this.index = index;
    this.channel = `presence-user-${index}`;
    this.state = { buffer: Buffer.alloc(0) };
    this.closed = false;
    this.serverCloseCode = null;
  }

  connect() {
    return new Promise((resolve, reject) => {
      const socket = net.connect({ host: opts.host, port: PORT });
      this.socket = socket;
      socket.setNoDelay(true);
      const key = crypto.randomBytes(16).toString('base64');
      let handshake = Buffer.alloc(0);
      let upgraded = false;
      const timer = setTimeout(() => reject(new Error('subscribe timeout')), 30_000);

      socket.on('error', (err) => {
        if (!upgraded) reject(err);
      });
      socket.on('close', () => {
        this.closed = true;
      });
      socket.on('connect', () => {
        socket.write(
          `GET /app/${opts.key}?protocol=7&client=js&version=8.4.0 HTTP/1.1\r\n` +
            `Host: ${opts.host}:${PORT}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
            `Sec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`,
        );
      });
      socket.on('data', (chunk) => {
        if (!upgraded) {
          handshake = Buffer.concat([handshake, chunk]);
          const end = handshake.indexOf('\r\n\r\n');
          if (end === -1) return;
          if (!handshake.subarray(0, 12).toString().includes('101')) {
            clearTimeout(timer);
            reject(new Error(`upgrade failed: ${handshake.subarray(0, end)}`));
            return;
          }
          upgraded = true;
          chunk = handshake.subarray(end + 4);
        }
        this.state.buffer = Buffer.concat([this.state.buffer, chunk]);
        for (const frame of decodeFrames(this.state)) {
          if (frame.opcode === 0x8) {
            // The network stack answers Close even when page JS is frozen.
            this.serverCloseCode = frame.payload.length >= 2 ? frame.payload.readUInt16BE(0) : 1005;
            socket.end(encodeFrame(0x8, frame.payload.subarray(0, 2)));
            continue;
          }
          if (frame.opcode === 0x9) {
            socket.write(encodeFrame(0xa, frame.payload));
            continue;
          }
          if (frame.opcode !== 0x1) continue;
          const message = JSON.parse(frame.payload.toString());
          if (message.event === 'pusher:connection_established') {
            this.socketId = JSON.parse(message.data).socket_id;
            const channelData = JSON.stringify({ user_id: `user-${this.index}`, user_info: {} });
            const auth = `${opts.key}:${hmac(`${this.socketId}:${this.channel}:${channelData}`)}`;
            this.send({
              event: 'pusher:subscribe',
              data: { channel: this.channel, auth, channel_data: channelData },
            });
          } else if (message.event === 'pusher_internal:subscription_succeeded') {
            clearTimeout(timer);
            resolve();
          } else if (message.event === 'pusher:error') {
            clearTimeout(timer);
            reject(new Error(`pusher:error ${message.data?.code ?? JSON.stringify(message.data)}`));
          }
          // pusher:ping is deliberately ignored: the page is frozen.
        }
      });
    });
  }

  send(message) {
    this.socket.write(encodeFrame(0x1, JSON.stringify(message)));
  }

  goDark() {
    // A dead peer neither reads nor answers anything; the kernel still holds the socket.
    if (opts.peer === 'dead') this.socket.pause();
  }

  close() {
    const payload = Buffer.alloc(2);
    payload.writeUInt16BE(1000);
    this.socket.write(encodeFrame(0x8, payload));
  }

  destroy() {
    this.socket.destroy();
  }
}

// --- Server observation

async function scrapeMetrics() {
  const text = await (await fetch(opts['metrics-url'])).text();
  // Series appear on first use, so a missing one counts as zero.
  const sum = (name, label = '') => {
    let total = 0;
    for (const line of text.split('\n')) {
      if (line.startsWith('#')) continue;
      const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9.eE+-]+)$/);
      if (match && match[1].endsWith(name) && (match[2] ?? '').includes(label)) {
        total += Number(match[3]);
      }
    }
    return total;
  };
  return {
    connected: sum('_connected'),
    newConnections: sum('_new_connections_total'),
    newDisconnections: sum('_new_disconnections_total'),
    tokioActiveTasks: sum('_tokio_active_tasks'),
    // Per-node gauge; the /channels HTTP listing is cached and would lag.
    presenceChannels: sum('_active_channels', 'channel_type="presence"'),
  };
}

function rssMiB() {
  if (!opts.pid) return null;
  try {
    const kib = Number(execFileSync('ps', ['-o', 'rss=', '-p', opts.pid]).toString().trim());
    return Math.round((kib / 1024) * 10) / 10;
  } catch {
    return null;
  }
}

async function snapshot() {
  return { ...(await scrapeMetrics()), rssMiB: rssMiB() };
}

async function runPool(items, limit, fn) {
  const results = [];
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) {
        const i = next++;
        results[i] = await fn(items[i]);
      }
    }),
  );
  return results;
}

// --- Rounds

async function waitFor(predicate, timeoutMs) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (await predicate()) return Date.now() - started;
    await sleep(50);
  }
  return null;
}

async function round(roundIndex, baseline) {
  const offset = roundIndex * CONNECTIONS;
  const clients = Array.from({ length: CONNECTIONS }, (_, i) => new Client(offset + i));
  const connectStarted = Date.now();
  const outcomes = await runPool(clients, CONCURRENCY, (client) =>
    client.connect().then(
      () => true,
      (err) => {
        client.error = err.message;
        client.destroy();
        return false;
      },
    ),
  );
  const live = clients.filter((_, i) => outcomes[i]);
  const connectMs = Date.now() - connectStarted;
  const errors = clients.filter((c) => c.error).map((c) => c.error);
  for (const client of live) client.goDark();
  const loaded = await snapshot();

  // Wait for the server to time out every client (4201), or for the round budget.
  const timeoutStarted = Date.now();
  if (opts.peer === 'clean') {
    const disconnectsBefore = loaded.newDisconnections;
    for (const client of live) client.close();
    await waitFor(async () => {
      const m = await scrapeMetrics();
      return m.newDisconnections - disconnectsBefore >= live.length;
    }, ROUND_TIMEOUT_MS);
  } else if (opts.peer === 'throttled-tab') {
    await waitFor(() => live.every((c) => c.closed), ROUND_TIMEOUT_MS);
  } else {
    // A paused socket never observes the close, so wait on the server's view instead.
    await waitFor(async () => {
      const m = await scrapeMetrics();
      return m.newDisconnections - baseline.newDisconnections >= live.length;
    }, ROUND_TIMEOUT_MS);
  }
  const timeoutWaitMs = Date.now() - timeoutStarted;
  const closedBy4201 = live.filter((c) => c.serverCloseCode === 4201).length;

  for (const client of live) client.destroy();
  await sleep(SETTLE_MS);
  const after = await snapshot();
  return {
    round: roundIndex + 1,
    opened: live.length,
    connectErrors: errors.length,
    firstConnectError: errors[0] ?? null,
    connectMs,
    // clean: time until the server counted every disconnect; otherwise until every timeout fired.
    timeoutWaitMs,
    closedBy4201,
    loaded,
    after,
    leakedConnections: after.connected - baseline.connected,
    leakedPresenceChannels: after.presenceChannels - baseline.presenceChannels,
    missingDisconnects:
      after.newConnections - baseline.newConnections -
      (after.newDisconnections - baseline.newDisconnections),
  };
}

const baseline = await snapshot();
const rounds = [];
for (let i = 0; i < ROUNDS; i++) {
  const result = await round(i, baseline);
  rounds.push(result);
  if (!opts.json) {
    console.log(
      `round ${result.round}: opened=${result.opened} 4201=${result.closedBy4201} ` +
        `leaked_connections=${result.leakedConnections} ` +
        `leaked_presence_channels=${result.leakedPresenceChannels} ` +
        `missing_disconnects=${result.missingDisconnects} ` +
        `tokio_tasks=${result.after.tokioActiveTasks} rss_mib=${result.after.rssMiB} ` +
        `(connect ${result.connectMs} ms, timeout wait ${result.timeoutWaitMs} ms)`,
    );
  }
}

const totalOpened = rounds.reduce((n, r) => n + r.opened, 0);
const final = rounds.at(-1);
const summary = {
  label: opts.label,
  peer: opts.peer,
  connectionsPerRound: CONNECTIONS,
  rounds: ROUNDS,
  totalOpened,
  baseline,
  final: final.after,
  leakedConnections: final.leakedConnections,
  leakedPresenceChannels: final.leakedPresenceChannels,
  leakRatePercent: totalOpened ? Math.round((final.leakedConnections / totalOpened) * 10000) / 100 : 0,
  rssGrowthMiB:
    final.after.rssMiB != null && baseline.rssMiB != null
      ? Math.round((final.after.rssMiB - baseline.rssMiB) * 10) / 10
      : null,
  rounds,
};

if (opts.json) {
  console.log(JSON.stringify(summary, null, 2));
} else {
  console.log(
    `\n${opts.label || 'run'}: ${summary.leakedConnections}/${totalOpened} connections leaked ` +
      `(${summary.leakRatePercent}%), ${summary.leakedPresenceChannels} presence channels leaked, ` +
      `RSS growth ${summary.rssGrowthMiB} MiB`,
  );
}
process.exit(0);
