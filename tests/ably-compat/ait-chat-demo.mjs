import * as Ably from 'ably';
import { createAgentSession, createClientSession, UIMessageCodec } from '@ably/ai-transport/vercel';
import { Invocation } from '@ably/ai-transport';
import assert from 'node:assert/strict';

const endpoint = process.env.ABLY_ENDPOINT ?? '127.0.0.1';
const port = Number(process.env.ABLY_PORT ?? '6001');
const tls = process.env.ABLY_TLS === 'true';
const key = process.env.ABLY_KEY ?? 'app-key:app-secret';
const clientId = process.env.ABLY_CLIENT_ID ?? 'sockudo-ably-ait-client';
const agentClientId = process.env.ABLY_AGENT_CLIENT_ID ?? 'sockudo-ably-ait-agent';
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
  return new Ably.Realtime({
    key,
    clientId: clientIdValue,
    endpoint,
    port,
    tls,
    useBinaryProtocol: false,
    autoConnect: false,
  });
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

function waitForView(view, predicate, label) {
  const initial = view.getMessages();
  if (predicate(initial)) {
    return Promise.resolve(initial);
  }
  return withTimeout(
    new Promise((resolve) => {
      const unsubscribe = view.on('update', () => {
        const messages = view.getMessages();
        if (predicate(messages)) {
          unsubscribe();
          resolve(messages);
        }
      });
    }),
    label,
  );
}

const clientRealtime = createRealtime(clientId);
const agentRealtime = createRealtime(agentClientId);
let clientSession;
let agentSession;

try {
  clientRealtime.connect();
  agentRealtime.connect();
  await withTimeout(onceConnection(clientRealtime, 'connected'), 'client connect');
  await withTimeout(onceConnection(agentRealtime, 'connected'), 'agent connect');

  clientSession = createClientSession({
    client: clientRealtime,
    channelName,
  });
  agentSession = createAgentSession({
    client: agentRealtime,
    channelName,
  });

  await withTimeout(clientSession.connect(), 'client session connect');
  await withTimeout(agentSession.connect(), 'agent session connect');

  const input = UIMessageCodec.createUserMessage({
    id: `user-${Date.now()}`,
    role: 'user',
    parts: [{ type: 'text', text: prompt }],
  });
  const clientRun = await withTimeout(clientSession.view.send(input), 'client send');
  const invocation = Invocation.fromJSON(clientRun.toInvocation().toJSON());
  const agentRun = agentSession.createRun(invocation, {
    runId: `run-${Date.now()}`,
    invocationId: `invoke-${Date.now()}`,
  });

  await withTimeout(agentRun.start(), 'agent run start');
  await withTimeout(clientRun.started, 'client observed run start');

  const streamResult = await withTimeout(
    agentRun.pipe(
      streamChunks([
        { type: 'start', messageId: `assistant-${Date.now()}` },
        { type: 'text-start', id: 'answer' },
        { type: 'text-delta', id: 'answer', delta: 'Hello from ' },
        { type: 'text-delta', id: 'answer', delta: 'Sockudo via stock @ably/ai-transport.' },
        { type: 'text-end', id: 'answer' },
        { type: 'finish', finishReason: 'stop' },
      ]),
    ),
    'agent stream',
  );
  assert.equal(streamResult.reason, 'complete');
  await withTimeout(agentRun.end({ reason: 'complete' }), 'agent run end');

  const messages = await waitForView(
    clientSession.view,
    (items) =>
      items.some(
        (item) =>
          item.message.role === 'assistant' &&
          textFromMessage(item.message).includes('Sockudo via stock @ably/ai-transport'),
      ),
    'assistant message projection',
  );
  const assistant = messages
    .map((item) => item.message)
    .find((message) => message.role === 'assistant');

  assert.ok(assistant, 'assistant message projected into the client view');
  const text = textFromMessage(assistant);
  assert.match(text, /Sockudo via stock @ably\/ai-transport/);

  console.log(
    JSON.stringify({
      ok: true,
      endpoint,
      port,
      tls,
      channel: channelName,
      runId: agentRun.runId,
      inputEventId: clientRun.inputEventId,
      assistantText: text,
    }),
  );
} finally {
  await clientSession?.close().catch(() => {});
  await agentSession?.close().catch(() => {});
  clientRealtime.close();
  agentRealtime.close();
}
