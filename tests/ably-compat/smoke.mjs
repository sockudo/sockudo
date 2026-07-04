import * as Ably from 'ably';
import assert from 'node:assert/strict';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const key = process.env.ABLY_KEY ?? 'app-key:app-secret';
const clientId = process.env.ABLY_CLIENT_ID ?? 'sockudo-ably-smoke';
const channelName = process.env.ABLY_CHANNEL ?? `ably-smoke-${Date.now()}`;

function withTimeout(promise, label, ms = 10_000) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    }),
  ]).finally(() => clearTimeout(timer));
}

function onceConnection(client, state) {
  if (client.connection.state === state) {
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    const onState = (change) => {
      if (change.current === state) {
        client.connection.off(onState);
        resolve(change);
      } else if (change.current === 'failed' || change.current === 'closed') {
        client.connection.off(onState);
        reject(change.reason ?? new Error(`connection ${change.current}`));
      }
    };
    client.connection.on(onState);
  });
}

const client = new Ably.Realtime({
  key,
  clientId,
  endpoint,
  port,
  tls,
  useBinaryProtocol: false,
  autoConnect: false,
});

try {
  client.connect();
  await withTimeout(onceConnection(client, 'connected'), 'connect');

  const channel = client.channels.get(channelName);
  await withTimeout(channel.attach(), 'attach');

  const received = withTimeout(
    new Promise((resolve) => {
      channel.subscribe('smoke-event', resolve);
    }),
    'subscribe delivery',
  );

  await withTimeout(channel.publish('smoke-event', { ok: true, text: 'hello' }), 'publish');
  const message = await received;

  assert.equal(message.name, 'smoke-event');
  assert.deepEqual(message.data, { ok: true, text: 'hello' });

  await withTimeout(channel.history({ limit: 1, untilAttach: true }), 'history');
  await withTimeout(channel.detach(), 'detach');

  console.log(
    JSON.stringify({
      ok: true,
      endpoint,
      port,
      tls,
      channel: channelName,
      received: message.name,
    }),
  );
} finally {
  client.close();
}
