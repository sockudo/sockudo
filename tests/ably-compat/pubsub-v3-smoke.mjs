import { createClient } from '@ably/pubsub-device';
import { createHttpClient, createRealtimeClient } from '@ably/pubsub-server';
import assert from 'node:assert/strict';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const key = process.env.ABLY_KEY ?? 'app-key:app-secret';
const clientId = process.env.ABLY_CLIENT_ID ?? 'sockudo-ably-v3-smoke';
const channelName = process.env.ABLY_CHANNEL ?? `ably-v3-smoke-${Date.now()}`;
const options = { key, endpoint, port, tls };

function withTimeout(promise, label, ms = 10_000) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

function nextMessage(channel, name) {
  return withTimeout(
    new Promise((resolve) => {
      channel.subscribe(name, resolve);
    }),
    `${name} delivery`,
  );
}

const device = createClient({ ...options, clientId, useBinaryProtocol: false });
const server = createRealtimeClient(options);
const http = createHttpClient(options);

try {
  await withTimeout(device.connection.whenState('connected'), 'device connect');
  await withTimeout(server.connection.whenState('connected'), 'server connect');

  const deviceChannel = device.channels.get(channelName);
  const serverChannel = server.channels.get(channelName);
  await withTimeout(deviceChannel.attach(), 'device attach');
  await withTimeout(serverChannel.attach(), 'server attach');

  const atServer = nextMessage(serverChannel, 'device-event');
  await withTimeout(deviceChannel.publish('device-event', { from: 'device' }), 'device publish');
  assert.deepEqual((await atServer).data, { from: 'device' });

  const atDevice = nextMessage(deviceChannel, 'http-event');
  await withTimeout(
    http.channels.get(channelName).publish('http-event', { from: 'http' }),
    'http publish',
  );
  assert.deepEqual((await atDevice).data, { from: 'http' });

  const history = await withTimeout(
    http.channels.get(channelName).history({ limit: 2 }),
    'http history',
  );
  assert.deepEqual(
    history.items.map((message) => message.name).sort(),
    ['device-event', 'http-event'],
  );

  console.log(JSON.stringify({ ok: true, endpoint, port, tls, channel: channelName }));
} finally {
  device.close();
  server.close();
}
