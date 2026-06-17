import { describe, expect, it, vi } from "vitest";

import {
  EVENT_AI_CANCEL,
  EVENT_AI_INPUT,
  EVENT_AI_OUTPUT,
  EVENT_AI_TURN_END,
  EVENT_AI_TURN_START,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_INPUT_CLIENT_ID,
  HEADER_INVOCATION_ID,
  HEADER_TURN_ID,
} from "../../constants.js";
import { ErrorCode } from "../../errors.js";
import { createMockClient, type MockChannel } from "../../realtime/mocks.js";
import type { SockudoRawMessage } from "../../realtime/adapter.js";
import { getTransportHeaders } from "../../utils.js";
import type {
  ChannelWriter,
  Codec,
  DecodedBatch,
  Decoder,
  Encoder,
  EncoderOptions,
  ReducerMeta,
  UserMessage,
} from "../codec/index.js";
import { createServerTransport } from "./agent-transport.js";

describe("server transport", () => {
  it("starts from a buffered input event and publishes turn-start once", async () => {
    const { channel, published } = setupChannel();
    const transport = createServerTransport({
      channel,
      codec: testCodec(),
      inputEventLookupTimeoutMs: 10,
    });
    channel.inject(input("turn-1", "inv-1", "input-1", "client-1", 1));
    const turn = transport.newTurn({
      turnId: "turn-1",
      clientId: "client-1",
      invocationId: "inv-1",
      inputEventId: "input-1",
    });

    await turn.start();
    await turn.start();

    expect(turn.messages).toEqual([{ id: "input-1", text: "hello" }]);
    expect(
      published.filter((message) => message.name === EVENT_AI_TURN_START),
    ).toHaveLength(1);
  });

  it("times out input lookup without publishing turn-start", async () => {
    const { channel, published } = setupChannel();
    const transport = createServerTransport({
      channel,
      codec: testCodec(),
      inputEventLookupTimeoutMs: 1,
    });
    const turn = transport.newTurn({
      turnId: "turn-1",
      invocationId: "missing",
      inputEventId: "input-1",
    });

    await expect(turn.start()).rejects.toMatchObject({
      code: ErrorCode.InputEventNotFound,
      statusCode: 504,
    });
    expect(
      published.some((message) => message.name === EVENT_AI_TURN_START),
    ).toBe(false);
  });

  it("routes own cancel through onCancel and aborts registered turns", async () => {
    const { channel } = setupChannel();
    const onCancel = vi.fn(() => true);
    const turn = createServerTransport({ channel, codec: testCodec() }).newTurn(
      {
        turnId: "turn-1",
        clientId: "client-1",
        onCancel,
      },
    );

    channel.inject(cancel("client-1"));
    await Promise.resolve();

    expect(onCancel).toHaveBeenCalledWith(
      expect.objectContaining({
        matchedTurnIds: ["turn-1"],
      }),
    );
    expect(turn.abortSignal.aborted).toBe(true);
  });

  it("publishes discrete messages with override ids in order", async () => {
    const { channel, published } = setupChannel();
    const turn = createServerTransport({ channel, codec: testCodec() }).newTurn(
      {
        turnId: "turn-1",
        clientId: "client-1",
      },
    );

    await expect(
      turn.addMessages([
        { message: { id: "m1", text: "one" }, msgId: "m1" },
        { message: { id: "m2", text: "two" }, msgId: "m2", parentId: "m1" },
      ]),
    ).resolves.toEqual({ msgIds: ["m1", "m2"] });

    expect(published.map((message) => message.name)).toEqual([
      EVENT_AI_INPUT,
      EVENT_AI_INPUT,
    ]);
  });

  it("pipes response chunks and returns complete without ending the turn", async () => {
    const { channel, published } = setupChannel();
    const turn = createServerTransport({ channel, codec: testCodec() }).newTurn(
      {
        turnId: "turn-1",
        clientId: "client-1",
      },
    );

    await expect(
      turn.streamResponse(
        new ReadableStream<Message>({
          start(controller) {
            controller.enqueue({ id: "a1", text: "A" });
            controller.enqueue({ id: "a1", text: "B" });
            controller.close();
          },
        }),
      ),
    ).resolves.toEqual({ reason: "complete" });

    expect(published.map((message) => message.name)).toEqual([
      EVENT_AI_OUTPUT,
      EVENT_AI_OUTPUT,
    ]);
  });

  it("stamps one generated assistant message id onto streamed responses", async () => {
    const { channel, published } = setupChannel();
    const turn = createServerTransport({
      channel,
      codec: testCodec(),
      idProvider: {
        turnId: () => "turn-generated",
        invocationId: () => "inv-generated",
        inputEventId: () => "evt-generated",
        messageId: () => "assistant-generated",
      },
    }).newTurn({
      turnId: "turn-1",
      clientId: "client-1",
    });

    await turn.streamResponse(
      new ReadableStream<Message>({
        start(controller) {
          controller.enqueue({ id: "ignored-by-codec", text: "A" });
          controller.enqueue({ id: "ignored-by-codec", text: "B" });
          controller.close();
        },
      }),
    );

    expect(
      published.map(
        (message) =>
          getTransportHeaders(message.extras)[HEADER_CODEC_MESSAGE_ID],
      ),
    ).toEqual(["assistant-generated", "assistant-generated"]);
  });

  it("publishes turn-end before deregistering", async () => {
    const { channel, published } = setupChannel();
    const turn = createServerTransport({ channel, codec: testCodec() }).newTurn(
      {
        turnId: "turn-1",
        clientId: "client-1",
      },
    );

    await turn.end("complete");
    channel.inject(cancel("client-1"));
    await Promise.resolve();

    expect(published.map((message) => message.name)).toEqual([
      EVENT_AI_TURN_END,
    ]);
    expect(turn.abortSignal.aborted).toBe(false);
  });
});

function setupChannel(): {
  channel: MockChannel;
  published: Parameters<MockChannel["publish"]>[0][];
} {
  const client = createMockClient({ clientId: "agent-1" });
  const channel = client.getMockChannel("chat");
  const published: Parameters<MockChannel["publish"]>[0][] = [];
  const originalPublish = channel.publish.bind(channel);
  vi.spyOn(channel, "publish").mockImplementation((message) => {
    published.push(message);
    return originalPublish(message);
  });
  return { channel, published };
}

function input(
  turnId: string,
  invocationId: string,
  messageId: string,
  clientId: string,
  serial: number,
): SockudoRawMessage {
  return {
    event: "sockudo:message.create",
    name: EVENT_AI_INPUT,
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
          [HEADER_INPUT_CLIENT_ID]: clientId,
        },
      },
    },
  };
}

function cancel(clientId: string): SockudoRawMessage {
  return {
    event: "sockudo:message.create",
    name: EVENT_AI_CANCEL,
    channel: "chat",
    data: { own: true },
    message_serial: "cancel-1",
    history_serial: 9,
    delivery_serial: 9,
    extras: {
      ai: {
        transport: {
          [HEADER_INPUT_CLIENT_ID]: clientId,
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
    createEncoder(channel, options) {
      return createTestEncoder(channel, options);
    },
    createDecoder(): Decoder<Message, Message> {
      return {
        decode(message): DecodedBatch<Message, Message> {
          const data = message.data as Message;
          const decoded = {
            event: data,
            messageId:
              message.getTransportHeaders()[HEADER_CODEC_MESSAGE_ID] ??
              message.messageSerial,
            meta: {
              serial: message.deliverySerial ?? message.historySerial,
              messageId:
                message.getTransportHeaders()[HEADER_CODEC_MESSAGE_ID] ??
                message.messageSerial,
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

function createTestEncoder(
  channel: ChannelWriter,
  options: EncoderOptions = {},
): Encoder<Message, Message> {
  return {
    publishInput(input, writeOptions = {}) {
      return channel.publish({
        name: EVENT_AI_INPUT,
        data: input,
        extras: mergeExtras(options.extras, writeOptions.extras),
        ...(writeOptions.messageId !== undefined
          ? { messageId: writeOptions.messageId }
          : {}),
        ...((writeOptions.clientId ?? options.clientId)
          ? { clientId: writeOptions.clientId ?? options.clientId }
          : {}),
      });
    },
    publishOutput(output, writeOptions = {}) {
      return channel.publish({
        name: EVENT_AI_OUTPUT,
        data: output,
        extras: mergeExtras(options.extras, writeOptions.extras),
        ...(writeOptions.messageId !== undefined
          ? { messageId: writeOptions.messageId }
          : {}),
        ...((writeOptions.clientId ?? options.clientId)
          ? { clientId: writeOptions.clientId ?? options.clientId }
          : {}),
      });
    },
    cancel() {
      return Promise.resolve();
    },
    close() {
      return Promise.resolve();
    },
  };
}

function mergeExtras(left: unknown, right: unknown): unknown {
  return {
    ...record(left),
    ...record(right),
    ai: {
      ...record(record(left).ai),
      ...record(record(right).ai),
      transport: {
        ...record(record(record(left).ai).transport),
        ...record(record(record(right).ai).transport),
      },
    },
  };
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : {};
}

interface Message {
  id: string;
  text: string;
  done?: boolean;
}

interface Projection {
  messages: Message[];
}
