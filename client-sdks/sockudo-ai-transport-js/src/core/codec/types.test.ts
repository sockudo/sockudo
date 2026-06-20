import { describe, expect, expectTypeOf, it } from "vitest";

import { createAccumulator } from "./types.js";
import type { AssertChannelWriter, ChannelWriter, Codec, DecodedEvent } from "./types.js";
import type { ChannelLike } from "../../realtime/types.js";

describe("codec types", () => {
  it("treats ChannelLike as a structural ChannelWriter", () => {
    expectTypeOf<ChannelLike>().toExtend<ChannelWriter>();
    expectTypeOf<AssertChannelWriter<ChannelLike>>().toExtend<ChannelLike>();
  });

  it("adapts codecs to the documented accumulator shape", () => {
    const codec = createMessageCodec();
    const accumulator = createAccumulator(codec);

    accumulator.initMessage({ id: "manual", text: "optimistic" });
    accumulator.initMessage({ id: "manual", text: "ignored" });
    expect(accumulator.messages).toContainEqual({
      id: "manual",
      text: "optimistic",
    });
    expect(accumulator.hasActiveStream).toBe(true);

    accumulator.processOutputs([
      decoded({ id: "server", text: "hello" }, "server", 1),
      decoded({ id: "server", text: "hello!", terminal: true }, "server", 2),
    ]);

    expect(accumulator.messages).toEqual([
      { id: "server", text: "hello!" },
      { id: "manual", text: "optimistic" },
    ]);
    expect(accumulator.completedMessages).toEqual([{ id: "server", text: "hello!" }]);
    expect(accumulator.hasActiveStream).toBe(true);

    accumulator.completeMessage({ id: "manual", text: "optimistic" });
    expect(accumulator.hasActiveStream).toBe(false);
    accumulator.completeMessage({ id: "missing", text: "" });
    expect(accumulator.hasActiveStream).toBe(false);
  });
});

interface Message {
  id: string;
  text: string;
  terminal?: boolean;
}

interface Projection {
  messages: Message[];
}

function createMessageCodec(): Codec<Message, Message, Projection, Message> {
  return {
    init() {
      return { messages: [] };
    },
    fold(state, event) {
      const index = state.messages.findIndex((message) => message.id === event.id);
      const next = { id: event.id, text: event.text };
      if (index === -1) {
        state.messages.push(next);
      } else {
        state.messages[index] = next;
      }
      return state;
    },
    createEncoder() {
      throw new Error("not used");
    },
    createDecoder() {
      throw new Error("not used");
    },
    getMessages(projection) {
      return projection.messages.slice();
    },
    createUserMessage(message) {
      return { message };
    },
    createRegenerate(target, parent) {
      return { target, parent };
    },
    resolveToolTarget() {
      return undefined;
    },
    isTerminal(output) {
      return output.terminal === true;
    },
  };
}

function decoded(event: Message, messageId: string, serial: number): DecodedEvent<Message> {
  return {
    event,
    messageId,
    meta: {
      serial,
      messageId,
    },
  };
}
