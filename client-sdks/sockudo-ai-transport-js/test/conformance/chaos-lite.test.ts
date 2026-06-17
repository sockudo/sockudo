import { describe, expect, it, vi } from "vitest";

import {
  EVENT_AI_INPUT,
  EVENT_AI_OUTPUT,
  EVENT_AI_TURN_END,
  EVENT_AI_TURN_START,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_INVOCATION_ID,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_TURN_CLIENT_ID,
  HEADER_TURN_ID,
  HEADER_TURN_REASON,
} from "../../src/constants.js";
import { ErrorCode, ErrorInfo } from "../../src/errors.js";
import { normalizeInboundMessage } from "../../src/realtime/adapter.js";
import type { SockudoRawMessage } from "../../src/realtime/adapter.js";
import type {
  ChannelEvents,
  ChannelLike,
  InboundMessage,
  MessageAck,
  MessageListener,
  MessageMutation,
  PaginatedResult,
  PresenceLike,
  PublishMessage,
  Serial,
  SubscribeOptions,
  Unsubscribe,
} from "../../src/realtime/types.js";
import type {
  Codec,
  DecodedBatch,
  Decoder,
  ReducerMeta,
  UserMessage,
} from "../../src/core/codec/index.js";
import { createClientTransport } from "../../src/core/transport/client-transport.js";
import type {
  ActiveTurn,
  ClientTransport,
} from "../../src/core/transport/client-transport.js";
import type { InvocationIdProvider } from "../../src/core/transport/invocation.js";

const stages = [
  "pre-publish",
  "post-publish-pre-post",
  "mid-stream",
  "pre-turn-end",
] as const;

describe("chaos-lite deterministic matrix", () => {
  it("defines every websocket drop stage required by the release gate", () => {
    expect(stages).toEqual([
      "pre-publish",
      "post-publish-pre-post",
      "mid-stream",
      "pre-turn-end",
    ]);
  });

  it("drops before input publish and rolls back optimistic state", async () => {
    const { channel, transport } = setup();
    channel.failNextPublish(new Error("socket closed before publish"));

    await expect(
      transport.view.send({ id: "user-1", text: "hello" }),
    ).rejects.toMatchObject({ code: ErrorCode.TransportSendFailed });
    expect(transport.view.getMessages()).toEqual([]);
    expect(channel.historySize()).toBe(0);
  });

  it("drops after input publish but before POST and leaves observable send failure", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(() =>
      Promise.reject(new TypeError("socket closed before POST")),
    );
    const { channel, transport } = setup({ fetch });
    const errors: ErrorInfo[] = [];
    transport.on("error", (error) => errors.push(error));

    const active = (await transport.view.send(
      { id: "user-1", text: "hello" },
      { waitForTurnStart: false },
    )) as ActiveTurn<Message>;
    await Promise.resolve();

    expect(channel.historySize()).toBe(1);
    expect(transport.view.getMessages()).toEqual([
      { id: "user-1", text: "hello" },
    ]);
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ code: ErrorCode.TransportSendFailed });
    await expect(active.stream.getReader().read()).rejects.toMatchObject({
      code: ErrorCode.TransportSendFailed,
    });
  });

  it("drops mid-stream, preserves rendered content, and errors the active stream", async () => {
    const { channel, transport } = setup();
    const active = (await transport.view.send(
      { id: "user-1", text: "hello" },
      { waitForTurnStart: false },
    )) as ActiveTurn<Message>;
    const reader = active.stream.getReader();

    channel.inject(lifecycle(EVENT_AI_TURN_START, active, 2));
    channel.inject(output(active, "assistant-1", "one", 3));

    await expect(reader.read()).resolves.toEqual({
      done: false,
      value: { id: "assistant-1", text: "one" },
    });
    channel.injectContinuityLost("mid-stream");

    await expect(reader.read()).rejects.toMatchObject({
      code: ErrorCode.ChannelContinuityLost,
    });
    expect(transport.view.getMessages()).toContainEqual({
      id: "assistant-1",
      text: "one",
    });
  });

  it("drops before turn-end and converges through history replay", async () => {
    const { channel, transport } = setup();
    const active = (await transport.view.send(
      { id: "user-1", text: "hello" },
      { waitForTurnStart: false },
    )) as ActiveTurn<Message>;
    const reader = active.stream.getReader();

    channel.inject(lifecycle(EVENT_AI_TURN_START, active, 2));
    channel.inject(output(active, "assistant-1", "partial", 3));
    await expect(reader.read()).resolves.toMatchObject({
      done: false,
      value: { id: "assistant-1", text: "partial" },
    });

    const waiting = transport.waitForTurn({ turnId: active.turnId });
    channel.inject(lifecycle(EVENT_AI_TURN_END, active, 4, "complete"), {
      deliver: false,
    });
    channel.injectContinuityLost("pre-turn-end");

    await expect(reader.read()).rejects.toMatchObject({
      code: ErrorCode.ChannelContinuityLost,
    });
    expect(transport.tree.getTurnNode(active.turnId)?.status).toBe("active");

    await transport.view.loadOlder(10);
    await expect(waiting).resolves.toBeUndefined();
    expect(transport.tree.getTurnNode(active.turnId)?.status).toBe("complete");
  });

  it("orders turns by server serial, not skewed local timestamps", async () => {
    const { channel, transport } = setup();
    await transport.view.send(
      { id: "user-1", text: "hello" },
      { waitForTurnStart: false },
    );

    channel.inject(lifecycleFromIds("turn-late", "inv-late", 20, 100));
    channel.inject(
      outputFromIds("turn-late", "inv-late", "assistant-late", 21, 50),
    );
    channel.inject(lifecycleFromIds("turn-early", "inv-early", 10, 10_000));
    channel.inject(
      outputFromIds("turn-early", "inv-early", "assistant-early", 11, 9_000),
    );

    const assistantMessages = transport.view
      .getMessages()
      .filter((message) => message.id.startsWith("assistant-"));
    expect(assistantMessages).toEqual([
      { id: "assistant-early", text: "assistant-early" },
      { id: "assistant-late", text: "assistant-late" },
    ]);
  });

  it("fails slow consumers through bounded stream backpressure", async () => {
    const { channel, transport } = setup({ streamQueueLimit: 1 });
    const active = (await transport.view.send(
      { id: "user-1", text: "hello" },
      { waitForTurnStart: false },
    )) as ActiveTurn<Message>;

    channel.inject(lifecycle(EVENT_AI_TURN_START, active, 2));
    channel.inject(output(active, "assistant-1", "one", 3));
    channel.inject(output(active, "assistant-2", "two", 4));

    await expect(active.stream.getReader().read()).rejects.toMatchObject({
      code: ErrorCode.StreamError,
      statusCode: 507,
    });
  });
});

function setup(
  options: {
    fetch?: typeof globalThis.fetch;
    streamQueueLimit?: number;
  } = {},
): {
  channel: ChaosChannel;
  transport: ClientTransport<Message, Message, Projection, Message>;
} {
  const channel = new ChaosChannel("chat");
  const transport = createClientTransport({
    channel,
    codec: testCodec(),
    api: "https://agent.test/run",
    clientId: "client-1",
    idProvider: fixedIds(),
    fetch: options.fetch ?? okFetch(),
    turnStartDeadlineMs: 0,
    ...(options.streamQueueLimit !== undefined
      ? { streamQueueLimit: options.streamQueueLimit }
      : {}),
  });
  return { channel, transport };
}

class ChaosChannel implements ChannelLike {
  private readonly messages: SockudoRawMessage[] = [];
  private readonly listeners = new Set<MessageListener>();
  private readonly continuityListeners = new Set<
    (payload: ErrorInfo) => void
  >();
  private nextPublishError: Error | undefined;
  private serial = 0;

  public readonly presence: PresenceLike = noopPresence;
  public attachSerial: Serial | undefined = undefined;

  public constructor(public readonly name: string) {}

  public failNextPublish(error: Error): void {
    this.nextPublishError = error;
  }

  public historySize(): number {
    return this.messages.length;
  }

  public publish(message: PublishMessage): Promise<MessageAck> {
    if (this.nextPublishError !== undefined) {
      const error = this.nextPublishError;
      this.nextPublishError = undefined;
      return Promise.reject(error);
    }
    const ack = this.nextAck(message.messageSerial);
    const raw = this.raw(
      message.name ?? EVENT_AI_INPUT,
      message.data,
      "message.create",
      ack,
      message.extras,
      message.messageId,
    );
    this.messages.push(raw);
    this.dispatch(raw);
    return Promise.resolve(ack);
  }

  public appendMessage(
    messageSerial: string,
    data: string,
    options: Omit<MessageMutation, "data"> = {},
  ): Promise<MessageAck> {
    return this.mutate(messageSerial, "message.append", data, options.extras);
  }

  public updateMessage(
    messageSerial: string,
    options: MessageMutation = {},
  ): Promise<MessageAck> {
    return this.mutate(
      messageSerial,
      "message.update",
      options.data,
      options.extras,
    );
  }

  public deleteMessage(
    messageSerial: string,
    options: MessageMutation = {},
  ): Promise<MessageAck> {
    return this.mutate(
      messageSerial,
      "message.delete",
      options.data,
      options.extras,
    );
  }

  public subscribe(
    listener: MessageListener,
    options?: SubscribeOptions,
  ): Unsubscribe {
    const names = options?.names ? new Set(options.names) : undefined;
    const wrapped = (message: InboundMessage): void => {
      if (!names || names.has(message.name)) {
        listener(message);
      }
    };
    this.listeners.add(wrapped);
    return () => {
      this.listeners.delete(wrapped);
    };
  }

  public history(): Promise<PaginatedResult<InboundMessage>> {
    const items = this.messages.map((message) =>
      normalizeInboundMessage(message),
    );
    return Promise.resolve({
      items,
      hasNext: () => false,
      next: () =>
        Promise.resolve({
          items: [],
          hasNext: () => false,
          next: () => Promise.reject(new Error("no next page")),
        }),
    });
  }

  public on<K extends keyof ChannelEvents>(
    event: K,
    listener: (payload: ChannelEvents[K]) => void,
  ): Unsubscribe {
    if (event !== "continuity_lost") {
      return noopUnsubscribe;
    }
    const continuityListener = listener as (payload: ErrorInfo) => void;
    this.continuityListeners.add(continuityListener);
    return () => {
      this.continuityListeners.delete(continuityListener);
    };
  }

  public inject(
    raw: SockudoRawMessage,
    options: { deliver?: boolean } = {},
  ): void {
    this.messages.push(raw);
    if (options.deliver !== false) {
      this.dispatch(raw);
    }
  }

  public injectContinuityLost(stage: string): void {
    this.emitContinuityLost(
      new ErrorInfo({
        code: ErrorCode.ChannelContinuityLost,
        statusCode: 409,
        message: `unable to maintain channel continuity; dropped ${stage}`,
      }),
    );
  }

  private mutate(
    messageSerial: string,
    action: "message.append" | "message.update" | "message.delete",
    data: unknown,
    extras: unknown,
  ): Promise<MessageAck> {
    const ack = this.nextAck(messageSerial);
    const raw = this.raw(action, data, action, ack, extras);
    this.messages.push(raw);
    this.dispatch(raw);
    return Promise.resolve(ack);
  }

  private nextAck(messageSerial?: string): MessageAck {
    this.serial += 1;
    return {
      messageSerial: messageSerial ?? `msg-${String(this.serial)}`,
      historySerial: this.serial,
      deliverySerial: this.serial,
      versionSerial: `ver-${String(this.serial)}`,
    };
  }

  private raw(
    name: string,
    data: unknown,
    action: string,
    ack: MessageAck,
    extras: unknown,
    messageId?: string,
  ): SockudoRawMessage {
    const raw: SockudoRawMessage = {
      event: `sockudo:${action}`,
      channel: this.name,
      name,
      data,
      extras,
      message_serial: ack.messageSerial,
      history_serial: ack.historySerial,
      version: {
        serial: ack.versionSerial,
        timestamp_ms: 0,
      },
    };
    if (ack.deliverySerial !== undefined) {
      raw.delivery_serial = ack.deliverySerial;
      raw.serial = ack.deliverySerial;
    }
    if (messageId !== undefined) {
      raw.message_id = messageId;
    }
    return raw;
  }

  private dispatch(raw: SockudoRawMessage): void {
    const message = normalizeInboundMessage(raw);
    for (const listener of Array.from(this.listeners)) {
      listener(message);
    }
  }

  private emitContinuityLost(payload: ErrorInfo): void {
    for (const listener of this.continuityListeners) {
      listener(payload);
    }
  }
}

const noopPresence: PresenceLike = {
  enter: () => Promise.resolve(),
  update: () => Promise.resolve(),
  leave: () => Promise.resolve(),
  get: () => Promise.resolve([]),
  subscribe: () => noopUnsubscribe,
};

function okFetch(): typeof globalThis.fetch {
  return vi.fn<typeof globalThis.fetch>(() =>
    Promise.resolve(new Response(null, { status: 200 })),
  );
}

function noopUnsubscribe(): void {
  return undefined;
}

function fixedIds(): InvocationIdProvider {
  let message = 0;
  return {
    turnId: () => "turn-1",
    invocationId: () => "inv-1",
    inputEventId: () => "evt-1",
    messageId: () => `msg-${String(++message)}`,
  };
}

function lifecycle(
  name: typeof EVENT_AI_TURN_START | typeof EVENT_AI_TURN_END,
  turn: ActiveTurn<Message>,
  serial: number,
  reason?: "complete" | "cancelled" | "error" | "suspended",
): SockudoRawMessage {
  return lifecycleFromIds(
    turn.turnId,
    turn.invocationId,
    serial,
    0,
    name,
    reason,
  );
}

function lifecycleFromIds(
  turnId: string,
  invocationId: string,
  serial: number,
  timestampMs: number,
  name:
    | typeof EVENT_AI_TURN_START
    | typeof EVENT_AI_TURN_END = EVENT_AI_TURN_START,
  reason?: "complete" | "cancelled" | "error" | "suspended",
): SockudoRawMessage {
  return {
    event: "sockudo:message.create",
    name,
    channel: "chat",
    data: {},
    message_serial: `${name}-${String(serial)}`,
    history_serial: serial,
    delivery_serial: serial,
    serial,
    version: {
      serial: `ver-${String(serial)}`,
      timestamp_ms: timestampMs,
    },
    extras: {
      ai: {
        transport: {
          [HEADER_TURN_ID]: turnId,
          [HEADER_INVOCATION_ID]: invocationId,
          [HEADER_TURN_CLIENT_ID]: "client-1",
          ...(reason !== undefined ? { [HEADER_TURN_REASON]: reason } : {}),
        },
      },
    },
  };
}

function output(
  turn: ActiveTurn<Message>,
  messageId: string,
  text: string,
  serial: number,
): SockudoRawMessage {
  return outputFromIds(
    turn.turnId,
    turn.invocationId,
    messageId,
    serial,
    0,
    text,
  );
}

function outputFromIds(
  turnId: string,
  invocationId: string,
  messageId: string,
  serial: number,
  timestampMs: number,
  text = messageId,
): SockudoRawMessage {
  return {
    event: "sockudo:message.create",
    name: EVENT_AI_OUTPUT,
    channel: "chat",
    data: { id: messageId, text },
    message_serial: messageId,
    history_serial: serial,
    delivery_serial: serial,
    serial,
    version: {
      serial: `ver-${String(serial)}`,
      timestamp_ms: timestampMs,
    },
    extras: {
      ai: {
        transport: {
          [HEADER_TURN_ID]: turnId,
          [HEADER_INVOCATION_ID]: invocationId,
          [HEADER_TURN_CLIENT_ID]: "client-1",
          [HEADER_CODEC_MESSAGE_ID]: messageId,
          [HEADER_STREAM]: "true",
          [HEADER_STATUS]: "streaming",
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
    isTerminal: (output) => output.done === true,
    createEncoder() {
      throw new Error("not used");
    },
    createDecoder(): Decoder<Message, Message> {
      return {
        decode(message): DecodedBatch<Message, Message> {
          const data = message.data as Message;
          const messageId =
            message.getTransportHeaders()[HEADER_CODEC_MESSAGE_ID] ??
            message.messageSerial;
          const decoded = {
            event: data,
            messageId,
            meta: {
              serial: message.deliverySerial ?? message.historySerial,
              messageId,
            } satisfies ReducerMeta,
          };
          if (message.name === EVENT_AI_OUTPUT) {
            return { inputs: [], outputs: [decoded] };
          }
          return { inputs: [decoded], outputs: [] };
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
