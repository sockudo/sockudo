import * as Ably from 'ably';
import assert from 'node:assert/strict';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const key = process.env.ABLY_KEY ?? 'app-key:app-secret';
const clientId = process.env.ABLY_CLIENT_ID ?? 'sockudo-ably-ait';
const channelName = process.env.ABLY_CHANNEL ?? `private-ai-ably-ait-${Date.now()}`;

function withTimeout(promise, label, ms = 15_000) {
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

function waitFor(events, predicate, label) {
  const found = events.find(predicate);
  if (found) {
    return Promise.resolve(found);
  }
  return withTimeout(
    new Promise((resolve) => {
      const interval = setInterval(() => {
        const match = events.find(predicate);
        if (match) {
          clearInterval(interval);
          resolve(match);
        }
      }, 25);
    }),
    label,
  );
}

function aiExtras({ runId, streamId, status, role = 'assistant', extraTransport = {} }) {
  return {
    ai: {
      transport: {
        'run-id': runId,
        'run-client-id': clientId,
        role,
        ...extraTransport,
      },
      codec: {
        stream: 'true',
        'stream-id': streamId,
        status,
      },
    },
  };
}

function messageAction(message) {
  return message.action ?? 'message.create';
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

  const delivered = [];
  await channel.subscribe((message) => {
    delivered.push(message);
  });

  const runId = `run-${Date.now()}`;
  const streamId = `${runId}:stream`;
  await withTimeout(
    channel.publish({
      name: 'ai-run-start',
      data: null,
      extras: aiExtras({ runId, streamId, status: 'streaming' }),
    }),
    'run start',
  );

  const createResult = await withTimeout(
    channel.publish({
      name: 'ai-output',
      data: '',
      extras: aiExtras({ runId, streamId, status: 'streaming' }),
    }),
    'output create',
  );
  const serial = createResult.serials[0];
  assert.ok(serial, 'create returned a message serial');

  const appendOne = await withTimeout(
    channel.appendMessage({
      serial,
      name: 'ai-output',
      data: 'Hello',
      extras: aiExtras({ runId, streamId, status: 'streaming' }),
    }),
    'append one',
  );
  assert.ok(appendOne.versionSerial, 'first append returned a version serial');

  await withTimeout(
    channel.appendMessage({
      serial,
      name: 'ai-output',
      data: ' world',
      extras: aiExtras({ runId, streamId, status: 'streaming' }),
    }),
    'append two',
  );

  await withTimeout(
    channel.appendMessage({
      serial,
      name: 'ai-output',
      data: '',
      extras: aiExtras({ runId, streamId, status: 'complete' }),
    }),
    'terminal append',
  );

  await waitFor(
    delivered,
    (message) =>
      message.name === 'ai-output' &&
      messageAction(message) === 'message.append' &&
      message.extras?.ai?.codec?.status === 'complete',
    'terminal append delivery',
  );

  const latest = await withTimeout(channel.getMessage(serial), 'get latest');
  assert.equal(latest.serial, serial);
  assert.equal(latest.data, 'Hello world');
  assert.equal(messageAction(latest), 'message.update');
  assert.equal(latest.extras?.ai?.codec?.status, 'complete');

  const history = await withTimeout(channel.history({ limit: 1, untilAttach: true }), 'history');
  assert.equal(history.items[0].serial, serial);
  assert.equal(history.items[0].data, 'Hello world');
  assert.equal(messageAction(history.items[0]), 'message.update');

  const versions = await withTimeout(channel.getMessageVersions(serial, { limit: 10 }), 'versions');
  assert.ok(
    versions.items.some(
      (message) => messageAction(message) === 'message.append' && message.data === ' world',
    ),
    'version history includes append fragment',
  );
  assert.ok(
    versions.items.some((message) => messageAction(message) === 'message.append' && message.data === ''),
    'version history includes empty terminal append',
  );

  const recovered = await withTimeout(
    channel.updateMessage({
      serial,
      name: 'ai-output',
      data: 'Hello recovered world',
      extras: aiExtras({ runId, streamId, status: 'complete' }),
    }),
    'recovery update',
  );
  assert.ok(recovered.versionSerial, 'update returned a version serial');

  const deleteResult = await withTimeout(
    channel.deleteMessage({
      serial,
      name: 'ai-output',
      data: null,
      extras: aiExtras({
        runId,
        streamId,
        status: 'cancelled',
        extraTransport: { 'run-reason': 'cancelled' },
      }),
    }),
    'delete',
  );
  assert.ok(deleteResult.versionSerial, 'delete returned a version serial');

  await withTimeout(
    channel.publish({
      name: 'ai-run-end',
      data: null,
      extras: aiExtras({
        runId,
        streamId,
        status: 'complete',
        extraTransport: { 'run-reason': 'complete' },
      }),
    }),
    'run end',
  );

  console.log(
    JSON.stringify({
      ok: true,
      endpoint,
      port,
      tls,
      channel: channelName,
      serial,
      appendVersionSerial: appendOne.versionSerial,
      latest: latest.data,
      versionCount: versions.items.length,
    }),
  );
} finally {
  client.close();
}
