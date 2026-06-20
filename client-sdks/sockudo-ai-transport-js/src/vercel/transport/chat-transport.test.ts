import { describe, expect, it, vi } from "vitest";

import {
  EVENT_AI_OUTPUT,
  EVENT_AI_TURN_END,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_INVOCATION_ID,
  HEADER_TURN_ID,
  HEADER_TURN_REASON,
} from "../../constants.js";
import type { InvocationIdProvider } from "../../core/transport/index.js";
import { createMockClient, type MockChannel } from "../../realtime/mocks.js";
import type { PublishMessage } from "../../realtime/types.js";
import type { SockudoRawMessage } from "../../realtime/adapter.js";
import {
  createChatTransport,
  deriveContinuationInputs,
  type ChatTransportOptions,
} from "./chat-transport.js";
import { createClientTransport } from "../index.js";
import type { AI } from "../codec/index.js";

describe("Vercel ChatTransport", () => {
  it("submits a fresh user message with split history and default api", async () => {
    const { chat, fetch, published } = setup();

    await chat.sendMessages({
      trigger: "submit-message",
      chatId: "chat-1",
      messages: [user("u1", "hello")],
    });

    expect(fetch.mock.calls[0]?.[0]).toBe("/api/chat");
    expect(published).toHaveLength(1);
    expect(published[0]).toMatchObject({
      name: "ai-input",
      messageSerial: "u1",
      messageId: "u1",
    });
    expect(bodyOf(fetch)).toMatchObject({
      id: "chat-1",
      messages: [user("u1", "hello")],
      history: [],
      trigger: "submit-message",
      turnId: "turn-1",
      invocationId: "inv-1",
      inputEventId: "evt-1",
      sessionName: "chat",
    });
  });

  it("lets prepareSendMessagesRequest override default body and headers", async () => {
    const { chat, fetch } = setup({
      prepareSendMessagesRequest: (ctx) => ({
        body: { history: ["custom"], parent: ctx.parent, custom: true },
        headers: { "x-custom": "yes" },
      }),
    });

    await chat.sendMessages({
      trigger: "submit-message",
      chatId: "chat-1",
      messages: [user("u1", "hello")],
      body: { fromUseChat: true },
      headers: { "x-use-chat": "yes" },
    });

    expect(bodyOf(fetch)).toMatchObject({
      history: ["custom"],
      custom: true,
      fromUseChat: true,
      turnId: "turn-1",
    });
    const request = fetch.mock.calls[0]?.[1] as RequestInit | undefined;
    expect(request?.headers).toMatchObject({
      "x-custom": "yes",
      "x-use-chat": "yes",
    });
  });

  it("sends edit metadata as a fork from the edited message", async () => {
    const { chat, fetch, published } = setup();

    await chat.sendMessages({
      trigger: "submit-message",
      chatId: "chat-1",
      messageId: "u1",
      messages: [user("u0", "before"), user("u1", "edited")],
    });

    expect(bodyOf(fetch)).toMatchObject({
      messageId: "u1",
      forkOf: "u1",
      parent: "u0",
      messages: [user("u1", "edited")],
      history: [user("u0", "before")],
    });
    expect(transportHeaders(published[0])).toMatchObject({
      "fork-of": "u1",
      parent: "u0",
    });
  });

  it.each(["input-streaming", "input-available", "approval-requested"] as const)(
    "forks a user submit away from unresolved %s tool state",
    async (state) => {
      const { chat, fetch } = setup();
      const assistant = assistantWithTool("a1", state);

      await chat.sendMessages({
        trigger: "submit-message",
        chatId: "chat-1",
        messages: [user("u1", "ask"), assistant, user("u2", "next")],
      });

      expect(bodyOf(fetch)).toMatchObject({
        forkOf: "a1",
        parent: "u1",
        history: [user("u1", "ask")],
        messages: [user("u2", "next")],
      });
    },
  );

  it.each(["approval-responded", "output-available", "output-error", "output-denied"] as const)(
    "does not fork resolved %s tool state",
    async (state) => {
      const { chat, fetch } = setup();
      const assistant = assistantWithTool("a1", state);

      await chat.sendMessages({
        trigger: "submit-message",
        chatId: "chat-1",
        messages: [user("u1", "ask"), assistant, user("u2", "next")],
      });

      expect(bodyOf(fetch)).toMatchObject({
        history: [user("u1", "ask"), assistant],
        messages: [user("u2", "next")],
      });
      expect(bodyOf(fetch)).not.toMatchObject({ forkOf: "a1" });
    },
  );

  it("regenerates the target assistant message as a sibling", async () => {
    const { chat, fetch, published } = setup();

    await chat.sendMessages({
      trigger: "regenerate-message",
      chatId: "chat-1",
      messageId: "a1",
      messages: [user("u1", "ask"), assistant("a1")],
    });

    expect(bodyOf(fetch)).toMatchObject({
      messages: [],
      history: [user("u1", "ask"), assistant("a1")],
      messageId: "a1",
      forkOf: "a1",
      parent: "u1",
    });
    expect(transportHeaders(published[0])).toMatchObject({
      "msg-regenerate": "true",
      "fork-of": "a1",
      parent: "u1",
    });
  });

  it("continues a suspended assistant turn with derived tool inputs", async () => {
    const treeAssistant = assistantWithTool("a1", "approval-requested");
    const overlay = assistantWithTool("a1", "approval-responded", {
      approval: { approvalId: "approval-1", approved: true },
    });
    const { chat, fetch, published } = setup({
      messages: [treeAssistant],
    });

    await chat.sendMessages({
      trigger: "submit-message",
      chatId: "chat-1",
      messages: [overlay],
    });

    expect(published[0]?.data).toEqual({
      type: "tool-approval-response",
      toolCallId: "tool-1",
      approvalId: "approval-1",
      approved: true,
    });
    expect(transportHeaders(published[0])).toMatchObject({
      "turn-id": "turn-1",
      "turn-continue": "true",
      "codec-message-id": "a1",
    });
    expect(bodyOf(fetch)).toMatchObject({
      messages: [],
      history: [overlay],
      messageId: "a1",
      parent: "a1",
      forkOf: "a1",
    });
  });

  it("derives tool result and error continuation inputs", () => {
    const tree = assistantWithTool("a1", "input-available");
    const overlay: AI.UIMessage = {
      ...tree,
      parts: [
        {
          type: "dynamic-tool",
          toolName: "lookup",
          toolCallId: "tool-1",
          state: "output-available",
          output: { ok: true },
        },
        {
          type: "dynamic-tool",
          toolName: "lookup",
          toolCallId: "tool-2",
          state: "output-error",
          errorText: "failed",
        },
      ],
    };

    expect(deriveContinuationInputs(overlay, tree)).toEqual([
      { type: "tool-result", toolCallId: "tool-1", output: { ok: true } },
      { type: "tool-result-error", toolCallId: "tool-2", message: "failed" },
    ]);
  });

  it("publishes cancel for an already returned active turn when aborted", async () => {
    const { chat, published } = setup();
    const controller = new AbortController();
    const stream = await chat.sendMessages({
      trigger: "submit-message",
      chatId: "chat-1",
      messages: [user("u1", "hello")],
      abortSignal: controller.signal,
    });

    controller.abort();
    await stream.cancel();

    expect(published.some((message) => message.name === "ai-cancel")).toBe(true);
  });

  it("rejects already-aborted sends before publishing", async () => {
    const { chat, published } = setup();
    const controller = new AbortController();
    controller.abort();

    await expect(
      chat.sendMessages({
        trigger: "submit-message",
        chatId: "chat-1",
        messages: [user("u1", "hello")],
        abortSignal: controller.signal,
      }),
    ).rejects.toMatchObject({ statusCode: 499 });
    expect(published).toHaveLength(0);
  });

  it("notifies streaming transitions on complete and error", async () => {
    const { chat, channel } = setup();
    const changes: boolean[] = [];
    chat.onStreamingChange((streaming) => changes.push(streaming));

    const stream = await chat.sendMessages({
      trigger: "submit-message",
      chatId: "chat-1",
      messages: [user("u1", "hello")],
    });
    const reader = stream.getReader();
    channel.inject(output("turn-1", "inv-1", "a1", "hello", 10));
    await expect(reader.read()).resolves.toMatchObject({
      done: false,
      value: { type: "start", messageId: "a1" },
    });
    channel.inject(turnEnd("turn-1", "inv-1", 11, "complete"));
    await expect(reader.read()).resolves.toEqual({
      done: true,
      value: undefined,
    });

    expect(changes).toEqual([true, false]);
  });

  it("returns null for reconnect because channel observation owns recovery", async () => {
    const { chat } = setup();

    await expect(chat.reconnectToStream({ chatId: "chat-1" })).resolves.toBe(null);
  });
});

function setup(
  options: {
    messages?: readonly AI.UIMessage[];
    prepareSendMessagesRequest?: ChatTransportOptions["prepareSendMessagesRequest"];
  } = {},
): {
  chat: ReturnType<typeof createChatTransport>;
  channel: MockChannel;
  fetch: ReturnType<typeof okFetch>;
  published: PublishMessage[];
} {
  const client = createMockClient({ clientId: "client-1" });
  const channel = client.getMockChannel("chat");
  const published: PublishMessage[] = [];
  const originalPublish = channel.publish.bind(channel);
  vi.spyOn(channel, "publish").mockImplementation((message) => {
    published.push(message);
    return originalPublish(message);
  });
  const fetch = okFetch();
  const transport = createClientTransport({
    channel,
    fetch,
    idProvider: fixedIds(),
    turnStartDeadlineMs: 0,
    messages: options.messages ?? [],
  });
  const chatOptions: ChatTransportOptions = {};
  if (options.prepareSendMessagesRequest !== undefined) {
    chatOptions.prepareSendMessagesRequest = options.prepareSendMessagesRequest;
  }
  return {
    chat: createChatTransport(transport, chatOptions),
    channel,
    fetch,
    published,
  };
}

function okFetch(): ReturnType<typeof vi.fn> & typeof fetch {
  return vi.fn(() => Promise.resolve(new Response(null, { status: 200 }))) as ReturnType<
    typeof vi.fn
  > &
    typeof fetch;
}

function bodyOf(fetch: ReturnType<typeof okFetch>): Record<string, unknown> {
  const request = fetch.mock.calls[0]?.[1] as RequestInit | undefined;
  if (typeof request?.body !== "string") {
    throw new TypeError("expected string request body");
  }
  return JSON.parse(request.body) as Record<string, unknown>;
}

function transportHeaders(message: PublishMessage | undefined): Record<string, string> {
  const extras = message?.extras as { ai?: { transport?: Record<string, string> } } | undefined;
  return extras?.ai?.transport ?? {};
}

function fixedIds(): InvocationIdProvider {
  let turn = 0;
  let invocation = 0;
  let event = 0;
  let message = 0;
  return {
    turnId: () => `turn-${String(++turn)}`,
    invocationId: () => `inv-${String(++invocation)}`,
    inputEventId: () => `evt-${String(++event)}`,
    messageId: () => `msg-${String(++message)}`,
  };
}

function user(id: string, text: string): AI.UIMessage {
  return { id, role: "user", parts: [{ type: "text", text }] };
}

function assistant(id: string): AI.UIMessage {
  return { id, role: "assistant", parts: [{ type: "text", text: "answer" }] };
}

function assistantWithTool(
  id: string,
  state: AI.DynamicToolState,
  overrides: Partial<Extract<AI.UIMessagePart, { type: "dynamic-tool" }>> = {},
): AI.UIMessage {
  return {
    id,
    role: "assistant",
    parts: [
      {
        type: "dynamic-tool",
        toolName: "lookup",
        toolCallId: "tool-1",
        state,
        input: { q: "sockudo" },
        ...overrides,
      },
    ],
  };
}

function output(
  turnId: string,
  invocationId: string,
  messageId: string,
  _delta: string,
  serial: number,
): SockudoRawMessage {
  return {
    event: "sockudo:message.create",
    name: EVENT_AI_OUTPUT,
    channel: "chat",
    data: {},
    message_serial: messageId,
    history_serial: serial,
    delivery_serial: serial,
    extras: {
      ai: {
        transport: {
          [HEADER_TURN_ID]: turnId,
          [HEADER_INVOCATION_ID]: invocationId,
          [HEADER_CODEC_MESSAGE_ID]: messageId,
        },
        codec: {
          type: "start",
          messageId,
        },
      },
    },
  };
}

function turnEnd(
  turnId: string,
  invocationId: string,
  serial: number,
  reason: "complete" | "cancelled" | "error" | "suspended",
): SockudoRawMessage {
  return {
    event: "sockudo:message.create",
    name: EVENT_AI_TURN_END,
    channel: "chat",
    data: {},
    message_serial: `end-${String(serial)}`,
    history_serial: serial,
    delivery_serial: serial,
    extras: {
      ai: {
        transport: {
          [HEADER_TURN_ID]: turnId,
          [HEADER_INVOCATION_ID]: invocationId,
          [HEADER_TURN_REASON]: reason,
        },
      },
    },
  };
}
