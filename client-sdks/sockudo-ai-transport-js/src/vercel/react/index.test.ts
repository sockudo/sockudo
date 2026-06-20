// @vitest-environment happy-dom

import { createElement, type ReactNode } from "react";
import { SockudoProvider } from "@sockudo/client/react";
import { useChat } from "@ai-sdk/react";
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ChatTransport as AiSdkChatTransport, UIMessage as AiSdkUIMessage } from "ai";

import {
  EVENT_AI_OUTPUT,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_INVOCATION_ID,
  HEADER_TURN_ID,
} from "../../constants.js";
import { ErrorCode, ErrorInfo } from "../../errors.js";
import { createMockClient, type MockChannel } from "../../realtime/mocks.js";
import type { SockudoRawMessage } from "../../realtime/adapter.js";
import type { ClientLike } from "../../realtime/index.js";
import type { AI } from "../codec/index.js";
import {
  ChatTransportProvider,
  mergeMessages,
  useChatTransport,
  useClientTransport,
  useMessageSync,
  useView,
} from "./index.js";

afterEach(() => {
  cleanup();
});

describe("Vercel React transport hooks", () => {
  it("resolves nested nearest and named chat transports", () => {
    const outer = createMockClient({ clientId: "outer" });
    const inner = createMockClient({ clientId: "inner" });
    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo(outer) },
        createElement(
          ChatTransportProvider,
          {
            channelName: "outer",
            fetch: okFetch(),
          },
          createElement(
            SockudoProvider,
            { client: asSockudo(inner) },
            createElement(
              ChatTransportProvider,
              {
                channelName: "inner",
                fetch: okFetch(),
              },
              children,
            ),
          ),
        ),
      );

    const { result } = renderHook(
      () => ({
        nearest: useChatTransport(),
        outer: useChatTransport({ channelName: "outer" }),
        view: useView({ limit: 30 }),
        core: useClientTransport(),
      }),
      { wrapper },
    );

    expect(result.current.nearest.chatTransportError).toBeUndefined();
    expect(result.current.outer.chatTransportError).toBeUndefined();
    expect(result.current.view.messages).toEqual([]);
    expect(result.current.core.transportError).toBeUndefined();
    expect(result.current.nearest.chatTransport).not.toBe(result.current.outer.chatTransport);
  });

  it("returns dual throwing stubs for missing, skipped, and failed providers", () => {
    const missing = renderHook(() => useChatTransport());
    expect(missing.result.current.chatTransportError).toMatchObject({
      code: ErrorCode.InvalidArgument,
    });
    expect(missing.result.current.transportError).toMatchObject({
      code: ErrorCode.InvalidArgument,
    });
    expect(() => missing.result.current.chatTransport.streaming).toThrow(ErrorInfo);
    expect(() => missing.result.current.transport.view).toThrow(ErrorInfo);

    const skipped = renderHook(() => useChatTransport({ skip: true }));
    expect(skipped.result.current.chatTransportError).toBeUndefined();
    expect(skipped.result.current.transportError).toBeUndefined();
    expect(() => skipped.result.current.chatTransport.streaming).toThrow(ErrorInfo);

    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo({}) },
        createElement(
          ChatTransportProvider,
          {
            channelName: "broken",
            fetch: okFetch(),
          },
          children,
        ),
      );
    const failed = renderHook(() => useChatTransport(), { wrapper });
    expect(failed.result.current.chatTransportError).toMatchObject({
      code: ErrorCode.InvalidArgument,
    });
    expect(failed.result.current.transportError).toMatchObject({
      code: ErrorCode.InvalidArgument,
    });
  });

  it("gates message sync while own stream is active and syncs on stream end", async () => {
    const client = createMockClient({ clientId: "client-1" });
    let overlay: readonly AI.UIMessage[] = [
      assistantTool("a1", "output-available", { output: "local" }),
    ];
    const setMessages = vi.fn(
      (
        value:
          | readonly AI.UIMessage[]
          | ((messages: readonly AI.UIMessage[]) => readonly AI.UIMessage[]),
      ) => {
        overlay = typeof value === "function" ? value(overlay) : value;
      },
    );
    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo(client) },
        createElement(
          ChatTransportProvider,
          {
            channelName: "chat",
            fetch: okFetch(),
            messages: [assistantTool("a1", "input-available")],
          },
          children,
        ),
      );
    const { result } = renderHook(
      () => {
        useMessageSync({ setMessages });
        return useChatTransport();
      },
      { wrapper },
    );
    setMessages.mockClear();

    let stream: ReadableStream<AI.UIMessageChunk> | undefined;
    await act(async () => {
      stream = await result.current.chatTransport.sendMessages({
        trigger: "submit-message",
        chatId: "chat-1",
        messages: [...overlay, user("u1", "hello")],
      });
    });
    expect(result.current.chatTransport.streaming).toBe(true);

    act(() => {
      result.current.transport.stageMessage("a1", assistantText("a1", "tree changed"));
    });
    expect(setMessages).not.toHaveBeenCalled();

    await act(async () => {
      await stream?.cancel();
    });

    expect(result.current.chatTransport.streaming).toBe(false);
    expect(setMessages).toHaveBeenCalled();
    expect(overlay.map((message) => message.id)).toContain("a1");
  });

  it("skips message sync subscriptions when requested", () => {
    const client = createMockClient({ clientId: "client-1" });
    const setMessages = vi.fn();
    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo(client) },
        createElement(
          ChatTransportProvider,
          {
            channelName: "chat",
            fetch: okFetch(),
            messages: [assistantText("a1", "tree")],
          },
          children,
        ),
      );

    renderHook(
      () => {
        useMessageSync({ setMessages, skip: true });
      },
      { wrapper },
    );

    expect(setMessages).not.toHaveBeenCalled();
  });

  it("syncs remote channel updates into observer message state", () => {
    const channel = createMockClient({ clientId: "source" }).getMockChannel("chat");
    const clientA = sharedChannelClient("client-a", channel);
    let overlay: readonly AI.UIMessage[] = [];
    const setMessages = vi.fn(
      (
        value:
          | readonly AI.UIMessage[]
          | ((messages: readonly AI.UIMessage[]) => readonly AI.UIMessage[]),
      ) => {
        overlay = typeof value === "function" ? value(overlay) : value;
      },
    );
    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo(clientA) },
        createElement(
          ChatTransportProvider,
          {
            channelName: "chat",
            fetch: okFetch(),
          },
          children,
        ),
      );

    renderHook(
      () => {
        useMessageSync({ setMessages });
      },
      { wrapper },
    );
    setMessages.mockClear();

    act(() => {
      channel.inject(outputStart("turn-b", "inv-b", "assistant-b", 2));
    });

    expect(setMessages).toHaveBeenCalled();
    expect(overlay.map((message) => message.id)).toEqual(["assistant-b"]);
  });

  it("bridges observer updates into real useChat state", () => {
    const channel = createMockClient({ clientId: "source" }).getMockChannel("chat");
    const clientA = sharedChannelClient("client-a", channel);
    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo(clientA) },
        createElement(
          ChatTransportProvider,
          {
            channelName: "chat",
            fetch: okFetch(),
          },
          children,
        ),
      );

    const { result } = renderHook(
      () => {
        const { chatTransport } = useChatTransport();
        const chat = useChat({
          id: "chat-1",
          transport: chatTransport as unknown as AiSdkChatTransport<AiSdkUIMessage>,
        });
        useMessageSync({
          setMessages: chat.setMessages as unknown as Parameters<
            typeof useMessageSync
          >[0]["setMessages"],
        });
        return chat;
      },
      { wrapper },
    );

    act(() => {
      channel.inject(outputStart("turn-b", "inv-b", "assistant-b", 2));
    });

    expect(result.current.messages.map((message) => message.id)).toEqual(["assistant-b"]);
  });
});

describe("mergeMessages", () => {
  it.each([
    ["output-available", { output: { ok: true } }],
    ["output-error", { errorText: "failed" }],
    ["approval-responded", { approval: { approved: false, reason: "no" } }],
    ["output-denied", { errorText: "denied" }],
  ] as const)(
    "keeps local assistant tool resolution for %s over unresolved tree parts",
    (state, patch) => {
      const merged = mergeMessages(
        [assistantTool("a1", "input-available")],
        [assistantTool("a1", state, patch)],
      );
      expect(dynamicTool(merged[0]).state).toBe(state);
    },
  );

  it("preserves the tree tool part type when overlay and tree tool shapes differ", () => {
    const tree = assistantWithPart("a1", {
      type: "tool-weather",
      toolCallId: "tool-1",
      state: "input-available",
    });
    const overlay = assistantTool("a1", "output-available", {
      output: "sunny",
    });

    const merged = mergeMessages([tree], [overlay]);

    expect(partRecord(merged[0]?.parts[0]).type).toBe("tool-weather");
    expect(partRecord(merged[0]?.parts[0]).state).toBe("output-available");
  });

  it("passes non-assistant tree messages through and appends unknown overlay messages", () => {
    const treeUser = user("u1", "tree");
    const overlayUser = user("u1", "overlay");
    const unknown = assistantText("a2", "local");

    const merged = mergeMessages([treeUser], [overlayUser, unknown]);

    expect(merged).toEqual([treeUser, unknown]);
  });

  it("does not overwrite already resolved tree tool parts", () => {
    const tree = assistantTool("a1", "output-available", { output: "tree" });
    const overlay = assistantTool("a1", "output-error", {
      errorText: "local",
    });

    const merged = mergeMessages([tree], [overlay]);

    expect(dynamicTool(merged[0]).state).toBe("output-available");
    expect(dynamicTool(merged[0]).output).toBe("tree");
  });
});

function user(id: string, text: string): AI.UIMessage {
  return {
    id,
    role: "user",
    parts: [{ type: "text", text }],
  };
}

function assistantText(id: string, text: string): AI.UIMessage {
  return {
    id,
    role: "assistant",
    parts: [{ type: "text", text }],
  };
}

function assistantTool(
  id: string,
  state: AI.DynamicToolState,
  patch: Partial<Extract<AI.UIMessagePart, { type: "dynamic-tool" }>> = {},
): AI.UIMessage {
  return {
    id,
    role: "assistant",
    parts: [
      {
        type: "dynamic-tool",
        toolName: "weather",
        toolCallId: "tool-1",
        state,
        ...patch,
      },
    ],
  };
}

function assistantWithPart(id: string, part: Record<string, unknown>): AI.UIMessage {
  return {
    id,
    role: "assistant",
    parts: [part as AI.UIMessagePart],
  };
}

function dynamicTool(
  message: AI.UIMessage | undefined,
): Extract<AI.UIMessagePart, { type: "dynamic-tool" }> {
  const part = message?.parts[0];
  if (part?.type !== "dynamic-tool") {
    throw new Error("expected dynamic tool part");
  }
  return part;
}

function partRecord(part: AI.UIMessagePart | undefined): Record<string, unknown> {
  if (part === undefined) {
    throw new Error("expected part");
  }
  return part;
}

function outputStart(
  turnId: string,
  invocationId: string,
  messageId: string,
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

function sharedChannelClient(clientId: string, channel: MockChannel): ClientLike {
  return {
    channels: {
      get: () => channel,
    },
    connection: {
      state: "connected",
      clientId,
    },
    close: () => undefined,
  };
}

function okFetch(): typeof globalThis.fetch {
  return vi.fn(() => Promise.resolve(new Response(null, { status: 200 })));
}

function asSockudo(client: unknown): Parameters<typeof SockudoProvider>[0]["client"] {
  return client as Parameters<typeof SockudoProvider>[0]["client"];
}
