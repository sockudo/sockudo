import * as Ably from 'ably';
import assert from 'node:assert/strict';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const key = process.env.ABLY_KEY ?? 'app-key:app-secret';
const channelName = process.env.ABLY_CHANNEL ?? `chat:ably-pubsub-${Date.now()}`;

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

function createRealtime(clientId) {
  return new Ably.Realtime({
    key,
    clientId,
    endpoint,
    port,
    tls,
    useBinaryProtocol: false,
    autoConnect: false,
  });
}

function createMessageWaiter(channel, eventName, predicate, label) {
  let listener;
  let rejectMessage;
  const message = withTimeout(
    new Promise((resolve, reject) => {
      rejectMessage = reject;
      listener = (message) => {
        if (predicate(message)) {
          channel.unsubscribe(eventName, listener);
          resolve(message);
        }
      };
    }),
    label,
  );
  const ready = withTimeout(
    Promise.resolve(channel.subscribe(eventName, listener)).catch((error) => {
      channel.unsubscribe(eventName, listener);
      rejectMessage(error);
      throw error;
    }),
    `${label} subscribe`,
  );
  return {
    ready,
    message,
  };
}

const alice = createRealtime('sockudo-ably-chat-alice');
const bob = createRealtime('sockudo-ably-chat-bob');

try {
  alice.connect();
  bob.connect();
  await withTimeout(onceConnection(alice, 'connected'), 'alice connect');
  await withTimeout(onceConnection(bob, 'connected'), 'bob connect');

  const aliceChannel = alice.channels.get(channelName);
  const bobChannel = bob.channels.get(channelName);
  await Promise.all([
    withTimeout(aliceChannel.attach(), 'alice attach'),
    withTimeout(bobChannel.attach(), 'bob attach'),
  ]);

  const aliceMessage = {
    id: `alice-${Date.now()}`,
    author: 'Alice',
    text: 'hello from plain Ably Pub/Sub',
    ts: Date.now(),
  };
  const bobReceivesAlice = createMessageWaiter(
    bobChannel,
    'chat-message',
    (message) => message.data?.id === aliceMessage.id,
    'bob receives alice message',
  );
  await bobReceivesAlice.ready;
  await withTimeout(aliceChannel.publish('chat-message', aliceMessage), 'alice publish');
  const messageFromAlice = await bobReceivesAlice.message;
  assert.deepEqual(messageFromAlice.data, aliceMessage);

  const bobMessage = {
    id: `bob-${Date.now()}`,
    author: 'Bob',
    text: 'reply from a second Ably client',
    ts: Date.now(),
  };
  const aliceReceivesBob = createMessageWaiter(
    aliceChannel,
    'chat-message',
    (message) => message.data?.id === bobMessage.id,
    'alice receives bob message',
  );
  await aliceReceivesBob.ready;
  await withTimeout(bobChannel.publish('chat-message', bobMessage), 'bob publish');
  const messageFromBob = await aliceReceivesBob.message;
  assert.deepEqual(messageFromBob.data, bobMessage);

  const historyPage = await withTimeout(aliceChannel.history({ limit: 10 }), 'history');
  const historyItems = historyPage.items ?? [];
  assert.ok(
    historyItems.some((message) => message.data?.id === aliceMessage.id),
    'history contains alice message',
  );
  assert.ok(
    historyItems.some((message) => message.data?.id === bobMessage.id),
    'history contains bob message',
  );

  await Promise.all([
    withTimeout(aliceChannel.detach(), 'alice detach'),
    withTimeout(bobChannel.detach(), 'bob detach'),
  ]);

  console.log(
    JSON.stringify({
      ok: true,
      endpoint,
      port,
      tls,
      channel: channelName,
      clients: ['sockudo-ably-chat-alice', 'sockudo-ably-chat-bob'],
      received: [messageFromAlice.data.text, messageFromBob.data.text],
      historyCount: historyItems.length,
    }),
  );
} finally {
  alice.close();
  bob.close();
}
