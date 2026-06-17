// @vitest-environment happy-dom

import { StrictMode, createElement, type ReactNode } from "react";
import { renderToString } from "react-dom/server";
import { SockudoProvider } from "@sockudo/client/react";
import { act, cleanup, render, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  EVENT_AI_OUTPUT,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_INVOCATION_ID,
  HEADER_TURN_CLIENT_ID,
  HEADER_TURN_ID,
} from "../constants.js";
import { ErrorCode, ErrorInfo } from "../errors.js";
import { EventEmitter } from "../event-emitter.js";
import { createMockClient, type MockChannel } from "../realtime/mocks.js";
import type { SockudoRawMessage } from "../realtime/adapter.js";
import type {
  Codec,
  DecodedBatch,
  Decoder,
  UserMessage,
} from "../core/codec/index.js";
import {
  createClientTransport,
  type ClientTransport,
  type ConversationTree,
  type TurnNode,
  type View,
  type ViewEvents,
} from "../core/transport/index.js";
import {
  TransportProvider,
  createTransportHooks,
  useActiveTurns,
  useClientTransport,
  useCreateView,
  useSockudoMessages,
  useTree,
  useView,
} from "./index.js";

afterEach(() => {
  cleanup();
});

describe("React transport hooks", () => {
  it("resolves nearest and named provider registry entries", () => {
    const outer = createMockClient({ clientId: "outer" });
    const inner = createMockClient({ clientId: "inner" });
    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo(outer) },
        createElement(
          TransportProvider,
          {
            channelName: "outer",
            codec: testCodec(),
            api: "/api/chat",
          },
          createElement(
            SockudoProvider,
            { client: asSockudo(inner) },
            createElement(
              TransportProvider,
              {
                channelName: "inner",
                codec: testCodec(),
                api: "/api/chat",
              },
              children,
            ),
          ),
        ),
      );

    const { result } = renderHook(
      () => ({
        nearest: useClientTransport(),
        outer: useClientTransport({ channelName: "outer" }),
      }),
      { wrapper },
    );

    expect(result.current.nearest.transportError).toBeUndefined();
    expect(result.current.outer.transportError).toBeUndefined();
    expect(result.current.nearest.transport).not.toBe(
      result.current.outer.transport,
    );
  });

  it("returns throwing stubs for missing, skipped, and failed providers", () => {
    const missing = renderHook(() => useClientTransport());
    expect(missing.result.current.transportError).toMatchObject({
      code: ErrorCode.InvalidArgument,
    });
    expect(() => missing.result.current.transport.view).toThrow(ErrorInfo);

    const skipped = renderHook(() => useClientTransport({ skip: true }));
    expect(skipped.result.current.transportError).toBeUndefined();
    expect(() => skipped.result.current.transport.view).toThrow(ErrorInfo);

    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo({}) },
        createElement(
          TransportProvider,
          {
            channelName: "broken",
            codec: testCodec(),
            api: "/api/chat",
          },
          children,
        ),
      );
    const failed = renderHook(() => useClientTransport(), { wrapper });
    expect(failed.result.current.transportError).toMatchObject({
      code: ErrorCode.InvalidArgument,
    });
    expect(() => failed.result.current.transport.view).toThrow(ErrorInfo);
  });

  it("non-client hooks are stable without a provider", () => {
    const view = renderHook(() => useView());
    expect(view.result.current.messages).toEqual([]);
    expect(() => view.result.current.send({ id: "m1", text: "x" })).toThrow(
      ErrorInfo,
    );

    const created = renderHook(() => useCreateView());
    expect(created.result.current.nodes).toEqual([]);

    const tree = renderHook(() => useTree());
    expect(() => tree.result.current.getNode("missing")).toThrow(ErrorInfo);

    const active = renderHook(() => useActiveTurns());
    expect(active.result.current.size).toBe(0);

    const raw = renderHook(() => useSockudoMessages());
    expect(raw.result.current).toEqual([]);
  });

  it("defers provider close until after unmount microtask", async () => {
    let captured:
      | ClientTransport<Message, Message, Projection, Message>
      | undefined;
    const client = createMockClient({ clientId: "client-1" });
    function Capture(): null {
      captured = useClientTransport().transport as ClientTransport<
        Message,
        Message,
        Projection,
        Message
      >;
      return null;
    }
    const tree = createElement(
      StrictMode,
      {},
      createElement(
        SockudoProvider,
        { client: asSockudo(client) },
        createElement(
          TransportProvider,
          {
            channelName: "chat",
            codec: testCodec(),
            api: "/api/chat",
          },
          createElement(Capture),
        ),
      ),
    );

    const rendered = render(tree);
    expect(captured).toBeDefined();
    if (!captured) {
      throw new Error("expected captured transport");
    }
    const close = vi.spyOn(captured, "close");
    rendered.unmount();
    expect(close).not.toHaveBeenCalled();
    await Promise.resolve();
    expect(close).toHaveBeenCalled();
  });

  it("useView prefers explicit views, auto-loads once, and re-renders only on view updates", () => {
    const view = new FakeView<Message>();
    view.messages = [{ id: "m1", text: "one" }];
    view.hasOlderValue = true;
    let renders = 0;

    const { result, rerender } = renderHook(() => {
      renders += 1;
      return useView({ view, limit: 10 });
    });

    expect(result.current.messages).toEqual([{ id: "m1", text: "one" }]);
    expect(view.loadOlder).toHaveBeenCalledWith(10);
    rerender();
    expect(view.loadOlder).toHaveBeenCalledTimes(1);
    const settledRenders = renders;

    act(() => {
      view.messages = [{ id: "m2", text: "two" }];
      view.emitUpdate();
    });

    expect(result.current.messages).toEqual([{ id: "m2", text: "two" }]);
    expect(renders).toBe(settledRenders + 1);
  });

  it("useCreateView owns and closes the created view", () => {
    const view = new FakeView<Message>();
    view.hasOlderValue = true;
    const transport = fakeTransport(view);

    const { result, unmount } = renderHook(() =>
      useCreateView({ transport, limit: 5 }),
    );

    expect(transport.createViewMock.mock.calls).toHaveLength(1);
    expect(view.loadOlder.mock.calls).toContainEqual([5]);
    expect(result.current.messages).toEqual([]);
    unmount();
    expect(view.close).toHaveBeenCalledTimes(1);
  });

  it("useTree exposes stable callbacks without subscribing to tree changes", () => {
    const transport = createRealTransport();
    let renders = 0;
    const { result } = renderHook(() => {
      renders += 1;
      return useTree({ transport });
    });
    const firstHandle = result.current;

    act(() => {
      applyTurnStart(transport.tree, "turn-1", "client-1");
    });

    expect(renders).toBe(1);
    expect(result.current).toBe(firstHandle);
  });

  it("useClientTransport does not re-render during message delivery", () => {
    const client = createMockClient({ clientId: "client-1" });
    const channel = client.getMockChannel("chat");
    let renders = 0;
    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo(client) },
        createElement(
          TransportProvider,
          {
            channelName: "chat",
            codec: testCodec(),
            api: "/api/chat",
          },
          children,
        ),
      );
    const { result } = renderHook(
      () => {
        renders += 1;
        return useClientTransport();
      },
      { wrapper },
    );
    result.current.transport.on("message", () => undefined);

    act(() => {
      channel.inject(output("turn-1", "inv-1", "m1", 1));
    });

    expect(renders).toBe(1);
  });

  it("useActiveTurns publishes new map identities on turn changes", () => {
    const transport = createRealTransport();
    const { result } = renderHook(() => useActiveTurns({ transport }));
    const first = result.current;

    act(() => {
      applyTurnStart(transport.tree, "turn-1", "client-1");
    });

    expect(result.current).not.toBe(first);
    expect(Array.from(result.current.get("client-1") ?? [])).toEqual([
      "turn-1",
    ]);
  });

  it("useSockudoMessages subscribes to the raw normalized firehose", () => {
    const client = createMockClient({ clientId: "client-1" });
    const channel = client.getMockChannel("chat");
    const transport = createRealTransport(channel);
    const { result, rerender } = renderHook(
      ({ current }) => useSockudoMessages({ transport: current }),
      { initialProps: { current: transport } },
    );

    act(() => {
      channel.inject(output("turn-1", "inv-1", "m1", 1));
    });

    expect(result.current.map((message) => message.messageSerial)).toEqual([
      "m1",
    ]);

    const next = createRealTransport(createMockClient().getMockChannel("next"));
    rerender({ current: next });
    expect(result.current).toEqual([]);
  });

  it("useClientTransport onError uses the latest callback ref", () => {
    const client = createMockClient({ clientId: "client-1" });
    const channel = client.getMockChannel("chat");
    const first = vi.fn();
    const second = vi.fn();
    const wrapper = ({ children }: { children?: ReactNode }) =>
      createElement(
        SockudoProvider,
        { client: asSockudo(client) },
        createElement(
          TransportProvider,
          {
            channelName: "chat",
            codec: testCodec(),
            api: "/api/chat",
            fetch: vi.fn(() =>
              Promise.resolve(new Response(null, { status: 200 })),
            ),
            turnStartDeadlineMs: 0,
          },
          children,
        ),
      );
    const { result, rerender } = renderHook(
      ({ onError }) => useClientTransport({ onError }),
      {
        initialProps: { onError: first },
        wrapper,
      },
    );

    rerender({ onError: second });
    act(() => {
      void result.current.transport.view.send({ id: "u1", text: "hello" });
    });
    act(() => {
      channel.injectContinuityLost("position_expired");
    });

    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalled();
  });

  it("createTransportHooks returns an isolated generic hook bundle", () => {
    const hooks = createTransportHooks<Message, Message, Projection, Message>();
    expect(Object.hasOwn(hooks, "TransportProvider")).toBe(true);
    expect(Object.hasOwn(hooks, "useView")).toBe(true);
  });

  it("SSR render is crash-free and returns stable empty hook values", () => {
    function Component(): ReturnType<typeof createElement> {
      const handle = useView({ skip: true });
      expect(handle.messages).toEqual([]);
      return createElement("span", {}, "ok");
    }

    expect(renderToString(createElement(Component))).toContain("ok");
  });
});

class FakeView<TMessage> implements View<unknown, TMessage> {
  public messages: readonly TMessage[] = [];
  public nodes: readonly TurnNode<unknown>[] = [];
  public hasOlderValue = false;
  public loading = false;
  public loadError: ErrorInfo | undefined = undefined;
  public readonly loadOlder = vi.fn(() => Promise.resolve(undefined));
  public readonly close = vi.fn();
  public readonly send = vi.fn(() => Promise.resolve(undefined));
  public readonly sendInput = vi.fn(() => Promise.resolve(undefined));
  public readonly regenerate = vi.fn(() => Promise.resolve(undefined));
  public readonly edit = vi.fn(() => Promise.resolve(undefined));
  public readonly update = vi.fn(() => Promise.resolve(undefined));
  private readonly emitter = new EventEmitter<ViewEvents<TMessage>>();

  public getMessages(): readonly TMessage[] {
    return this.messages;
  }

  public flattenNodes(): readonly TurnNode<unknown>[] {
    return this.nodes;
  }

  public hasOlder(): boolean {
    return this.hasOlderValue;
  }

  public select(): void {
    return undefined;
  }

  public getSelectedIndex(): number {
    return 0;
  }

  public getSiblings(): readonly TurnNode<unknown>[] {
    return [];
  }

  public hasSiblings(): boolean {
    return false;
  }

  public getNode(): TurnNode<unknown> | undefined {
    return undefined;
  }

  public getMessageMetadata(): undefined {
    return undefined;
  }

  public hasMessageSiblings(): boolean {
    return false;
  }

  public getMessageSiblings(): readonly TMessage[] {
    return [];
  }

  public getSelectedMessageSiblingIndex(): number {
    return 0;
  }

  public selectMessageSibling(): void {
    return undefined;
  }

  public on<K extends keyof ViewEvents<TMessage>>(
    event: K,
    handler: (payload: ViewEvents<TMessage>[K]) => void,
  ): () => void {
    return this.emitter.on(event, handler);
  }

  public emitUpdate(): void {
    this.emitter.emit("update", this.messages);
  }
}

type FakeTransport = ClientTransport<unknown, unknown, unknown, Message> & {
  readonly createViewMock: ReturnType<
    typeof vi.fn<() => View<unknown, Message>>
  >;
};

function fakeTransport(view: View<unknown, Message>): FakeTransport {
  const createViewMock = vi.fn(() => view);
  return {
    tree: createRealTransport().tree,
    view,
    createView: createViewMock,
    createViewMock,
    cancel: vi.fn(() => Promise.resolve(undefined)),
    waitForTurn: vi.fn(() => Promise.resolve(undefined)),
    stageEvents: vi.fn(),
    stageMessage: vi.fn(),
    on: vi.fn(() => () => undefined),
    close: vi.fn(() => Promise.resolve(undefined)),
  };
}

function createRealTransport(
  channel: MockChannel = createMockClient({
    clientId: "client-1",
  }).getMockChannel("chat"),
): ClientTransport<Message, Message, Projection, Message> {
  return createClientTransport({
    channel,
    codec: testCodec(),
    api: "/api/chat",
    fetch: vi.fn(() => Promise.resolve(new Response(null, { status: 200 }))),
    turnStartDeadlineMs: 0,
  });
}

function asSockudo(
  client: unknown,
): Parameters<typeof SockudoProvider>[0]["client"] {
  return client as Parameters<typeof SockudoProvider>[0]["client"];
}

function applyTurnStart(
  tree: ConversationTree<Message, Projection>,
  turnId: string,
  clientId: string,
): void {
  tree.applyTurnLifecycle({
    type: "turn-start",
    headers: {
      [HEADER_TURN_ID]: turnId,
      [HEADER_TURN_CLIENT_ID]: clientId,
      [HEADER_INVOCATION_ID]: "inv-1",
    },
    serial: 1,
  });
}

function output(
  turnId: string,
  invocationId: string,
  messageId: string,
  serial: number,
): SockudoRawMessage {
  return {
    event: "sockudo:message.create",
    name: EVENT_AI_OUTPUT,
    channel: "chat",
    data: { id: messageId, text: "hello" },
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
      },
    },
  };
}

function testCodec(): Codec<Message, Message, Projection, Message> {
  return {
    init: () => ({ messages: [] }),
    fold(projection, event) {
      const index = projection.messages.findIndex(
        (message) => message.id === event.id,
      );
      if (index === -1) {
        projection.messages.push(event);
      } else {
        projection.messages[index] = event;
      }
      return projection;
    },
    getMessages: (projection) => projection.messages,
    createUserMessage(message): UserMessage<Message> {
      return { message };
    },
    createRegenerate(target, parent) {
      return { target, parent };
    },
    resolveToolTarget: () => undefined,
    isTerminal: (outputMessage) => outputMessage.done === true,
    createEncoder() {
      throw new Error("not used");
    },
    createDecoder(): Decoder<Message, Message> {
      return {
        decode(message): DecodedBatch<Message, Message> {
          const data = message.data as Message;
          const decoded = {
            event: data,
            messageId: message.messageSerial,
            meta: {
              serial: message.deliverySerial ?? message.historySerial,
              messageId: message.messageSerial,
            },
          };
          return message.name === EVENT_AI_OUTPUT
            ? { inputs: [], outputs: [decoded] }
            : { inputs: [decoded], outputs: [] };
        },
      };
    },
  };
}

interface Message {
  id: string;
  text: string;
  done?: boolean;
}

interface Projection {
  messages: Message[];
}
