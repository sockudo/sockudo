import * as Ably from 'ably';
import assert from 'node:assert/strict';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const key = process.env.ABLY_KEY ?? 'app-key:app-secret';
const clientId = process.env.ABLY_CLIENT_ID ?? 'sockudo-ably-ait-recovery';
const publisherClientId = `${clientId}-publisher`;
const channelName = process.env.ABLY_CHANNEL ?? `private-ai-ably-recovery-${Date.now()}`;

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

function createClient(options = {}) {
  return new Ably.Realtime({
    key,
    endpoint,
    port,
    tls,
    useBinaryProtocol: false,
    autoConnect: false,
    ...options,
  });
}

function aiExtras({ runId, streamId, status, client = clientId }) {
  return {
    ai: {
      transport: {
        'run-id': runId,
        'run-client-id': client,
        role: 'assistant',
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

const source = createClient({ clientId });
const publisher = createClient({ clientId: publisherClientId });
let recovered;

try {
  source.connect();
  publisher.connect();
  await withTimeout(onceConnection(source, 'connected'), 'source connect');
  await withTimeout(onceConnection(publisher, 'connected'), 'publisher connect');

  const sourceChannel = source.channels.get(channelName);
  const publisherChannel = publisher.channels.get(channelName);
  await withTimeout(sourceChannel.attach(), 'source attach');
  await withTimeout(publisherChannel.attach(), 'publisher attach');

  const sourceDelivered = [];
  await sourceChannel.subscribe((message) => {
    sourceDelivered.push(message);
  });

  const runId = `run-${Date.now()}`;
  const streamId = `${runId}:stream`;
  const createResult = await withTimeout(
    sourceChannel.publish({
      name: 'ai-output',
      data: '',
      extras: aiExtras({ runId, streamId, status: 'streaming' }),
    }),
    'output create',
  );
  const serial = createResult.serials[0];
  assert.ok(serial, 'create returned a message serial');

  await withTimeout(
    sourceChannel.appendMessage({
      serial,
      name: 'ai-output',
      data: 'Hello',
      extras: aiExtras({ runId, streamId, status: 'streaming' }),
    }),
    'append before disconnect',
  );
  await waitFor(
    sourceDelivered,
    (message) => message.serial === serial && messageAction(message) === 'message.append',
    'source received first append',
  );

  const recoveryKey = source.connection.createRecoveryKey();
  assert.ok(recoveryKey, 'source produced a recovery key');
  const recoveryContext = JSON.parse(recoveryKey);
  assert.ok(
    recoveryContext.channelSerials?.[channelName],
    'recovery key includes the channel serial',
  );

  source.close();

  await withTimeout(
    publisherChannel.appendMessage({
      serial,
      name: 'ai-output',
      data: ' world',
      extras: aiExtras({
        runId,
        streamId,
        status: 'streaming',
        client: publisherClientId,
      }),
    }),
    'append while disconnected',
  );
  await withTimeout(
    publisherChannel.appendMessage({
      serial,
      name: 'ai-output',
      data: '',
      extras: aiExtras({
        runId,
        streamId,
        status: 'complete',
        client: publisherClientId,
      }),
    }),
    'terminal append while disconnected',
  );

  recovered = createClient({ clientId, recover: recoveryKey });
  recovered.connect();
  await withTimeout(onceConnection(recovered, 'connected'), 'recovered connect');

  const recoveredChannel = recovered.channels.get(channelName);
  const recoveredDelivered = [];
  await recoveredChannel.subscribe((message) => {
    recoveredDelivered.push(message);
  });
  await withTimeout(recoveredChannel.attach(), 'recovered attach');

  await waitFor(
    recoveredDelivered,
    (message) =>
      message.serial === serial &&
      messageAction(message) === 'message.append' &&
      message.extras?.ai?.codec?.status === 'complete',
    'terminal append recovered',
  );

  const recoveredAppends = recoveredDelivered.filter(
    (message) => message.serial === serial && messageAction(message) === 'message.append',
  );
  const recoveredFragments = recoveredAppends
    .map((message) => message.data)
    .filter((fragment) => fragment !== undefined && fragment !== null && fragment !== '');
  assert.deepEqual(recoveredFragments, [' world']);
  assert.ok(
    recoveredAppends.some((message) => message.extras?.ai?.codec?.status === 'complete'),
    'terminal append status was recovered',
  );

  const latest = await withTimeout(recoveredChannel.getMessage(serial), 'get recovered latest');
  assert.equal(latest.data, 'Hello world');
  assert.equal(latest.extras?.ai?.codec?.status, 'complete');

  console.log(
    JSON.stringify({
      ok: true,
      endpoint,
      port,
      tls,
      channel: channelName,
      serial,
      recoveredFragments,
      latest: latest.data,
    }),
  );
} finally {
  source.close();
  publisher.close();
  recovered?.close();
}
