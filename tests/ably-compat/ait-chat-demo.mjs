import { vercelAdaptor, vercelDecoder } from '@ably/ai-transport/vercel';
import { createRealtimeClient } from '@ably/pubsub-server';
import assert from 'node:assert/strict';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const key = process.env.ABLY_KEY ?? 'app-key:app-secret';
const clientId = process.env.ABLY_CLIENT_ID ?? 'sockudo-ably-ait-client';
// Keep the agent connection unidentified unless asked otherwise, so this
// exercises the real unknown-owner protocol path.
const agentClientId = process.env.ABLY_AGENT_CLIENT_ID;
const channelName = process.env.ABLY_CHANNEL ?? `private-ai-ably-chat-${Date.now()}`;
const prompt = process.env.ABLY_DEMO_PROMPT ?? 'Say hello from Sockudo AI Transport.';

function withTimeout(promise, label, ms = 20_000) {
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

function createRealtime(clientIdValue) {
  const options = {
    key,
    endpoint,
    port,
    tls,
    useBinaryProtocol: false,
    autoConnect: false,
  };
  if (clientIdValue) options.clientId = clientIdValue;
  // The AI Transport adaptors and decoders publish on a Pub/Sub v3 channel.
  return createRealtimeClient(options);
}

async function teardownChannel(channel, decoder) {
  channel.unsubscribe(decoder.receive);
  // detach rejects once the connection is gone; closing the client is enough.
  await channel.detach().catch(() => {});
}

function streamChunks(chunks) {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
}

function textFromMessage(message) {
  return (message.parts ?? [])
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('');
}

const clientRealtime = createRealtime(clientId);
const agentRealtime = createRealtime(agentClientId);
// A decoder holds the per-serial state of the streams it reads, so the two
// sides of this one process need one each, and each reads its own channel.
// The 0.11 transport set echoMessages: false; in 1.0 that is a channel param.
const channelOptions = { params: { echo: 'false' } };
const clientChannel = clientRealtime.channels.get(channelName, channelOptions);
const agentChannel = agentRealtime.channels.get(channelName, channelOptions);
const clientAdaptor = vercelAdaptor(clientChannel);
const agentAdaptor = vercelAdaptor(agentChannel);
const clientDecoder = vercelDecoder();
const agentDecoder = vercelDecoder();
const decodeErrors = [];
clientDecoder.onError((error) => decodeErrors.push(error));
agentDecoder.onError((error) => decodeErrors.push(error));

try {
  clientRealtime.connect();
  agentRealtime.connect();
  await withTimeout(onceConnection(clientRealtime, 'connected'), 'client connect');
  await withTimeout(onceConnection(agentRealtime, 'connected'), 'agent connect');

  const seenClientEvents = [];
  const seenAgentEvents = [];
  const inbound = withTimeout(
    new Promise((resolve) => {
      agentDecoder.onEvent(({ event }) => {
        seenAgentEvents.push(event?.type);
        if (event?.type === 'user-message') resolve(event.message);
      });
    }),
    'agent observed the prompt',
  );
  const reply = withTimeout(
    new Promise((resolve) => {
      let text = '';
      clientDecoder.onEvent(({ event }) => {
        seenClientEvents.push(event?.type);
        if (event?.type === 'text-delta') text += event.delta;
        if (event?.type === 'finish') resolve(text);
      });
    }),
    'client observed the reply',
  );
  await withTimeout(clientChannel.subscribe(clientDecoder.receive), 'client subscribe');
  await withTimeout(agentChannel.subscribe(agentDecoder.receive), 'agent subscribe');
  await withTimeout(clientChannel.whenState('attached'), 'client attach');
  await withTimeout(agentChannel.whenState('attached'), 'agent attach');

  const requestId = `user-${Date.now()}`;
  const { serial } = await withTimeout(
    clientAdaptor.send(
      {
        type: 'user-message',
        message: { id: requestId, role: 'user', parts: [{ type: 'text', text: prompt }] },
      },
      { headers: { requestId } },
    ),
    'client send',
  );
  assert.equal(textFromMessage(await inbound), prompt);

  await withTimeout(
    agentAdaptor.pipe(
      streamChunks([
        { type: 'start', messageId: `assistant-${Date.now()}` },
        { type: 'text-start', id: 'answer' },
        { type: 'text-delta', id: 'answer', delta: 'Hello from ' },
        { type: 'text-delta', id: 'answer', delta: 'Sockudo via stock @ably/ai-transport.' },
        { type: 'text-end', id: 'answer' },
        { type: 'finish', finishReason: 'stop' },
      ]),
      { headers: { requestId } },
    ),
    'agent stream',
  );

  const text = await reply;
  assert.equal(text, 'Hello from Sockudo via stock @ably/ai-transport.');
  assert.deepEqual(
    decodeErrors.map((error) => error.message),
    [],
    'both decoders reported decode errors',
  );
  assert.deepEqual(
    seenAgentEvents,
    ['user-message'],
    'the agent decoded exactly the user message',
  );
  assert.deepEqual(
    seenClientEvents,
    ['start', 'text-start', 'text-delta', 'text-delta', 'text-end', 'finish'],
    'the client decoded the full assistant stream in order',
  );

  console.log(
    JSON.stringify({
      ok: true,
      endpoint,
      port,
      tls,
      channel: channelName,
      requestId,
      inputSerial: serial,
      assistantText: text,
    }),
  );
} finally {
  await teardownChannel(clientChannel, clientDecoder);
  await teardownChannel(agentChannel, agentDecoder);
  clientRealtime.close();
  agentRealtime.close();
}
