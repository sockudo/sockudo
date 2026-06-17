import fc from "fast-check";
import { describe, expect, it } from "vitest";

import {
  HEADER_CODEC_MESSAGE_ID,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_STREAM_ID,
} from "../../constants.js";
import { createDecoderCore } from "./decoder.js";
import type { DecoderCoreHooks } from "./decoder.js";
import type { DecodedEvent } from "./types.js";
import type {
  InboundMessage,
  InboundMessageAction,
  Serial,
} from "../../realtime/types.js";

describe("decoder core", () => {
  it("decodes non-stream creates as discrete events", () => {
    const decoder = createDecoderCore(hooks());

    expect(
      decoder.decode(
        message({
          action: "create",
          data: "hello",
          transport: { [HEADER_STREAM]: "false" },
        }),
      ),
    ).toEqual([event("discrete:hello", "msg-1", 1)]);
  });

  it("creates trackers, applies append deltas, and closes terminal streams", () => {
    const closed: string[] = [];
    const decoder = createDecoderCore(hooks(), {
      onTrackerClosed: (messageId) => closed.push(messageId),
    });

    expect(
      decoder.decode(
        message({
          action: "create",
          data: "a",
          transport: { [HEADER_STREAM]: "true" },
        }),
      ),
    ).toEqual([event("start:a", "msg-1", 1)]);
    expect(
      decoder.decode(
        message({
          action: "append",
          data: "b",
          transport: {
            [HEADER_STREAM]: "true",
            [HEADER_STATUS]: "complete",
          },
          serial: 2,
        }),
      ),
    ).toEqual([event("delta:b:ab", "msg-1", 2), event("end:ab", "msg-1", 2)]);
    expect(closed).toEqual(["msg-1"]);
  });

  it("treats prefix-match append frames as aggregate server state", () => {
    const decoder = createDecoderCore(hooks());

    expect(
      decoder.decode(
        message({
          action: "create",
          data: "hello",
          transport: { [HEADER_STREAM]: "true" },
        }),
      ),
    ).toEqual([event("start:hello", "msg-1", 1)]);
    expect(
      decoder.decode(
        message({
          action: "append",
          data: "hello world",
          transport: {
            [HEADER_STREAM]: "true",
            [HEADER_STATUS]: "complete",
          },
          serial: 2,
        }),
      ),
    ).toEqual([
      event("delta: world:hello world", "msg-1", 2),
      event("end:hello world", "msg-1", 2),
    ]);
  });

  it("synthesizes first-contact start, delta, and end for terminal append", () => {
    const firstContact: string[] = [];
    const decoder = createDecoderCore(hooks(), {
      onFirstContact: (messageId) => firstContact.push(messageId),
    });

    expect(
      decoder.decode(
        message({
          action: "append",
          data: "late",
          transport: {
            [HEADER_STREAM]: "true",
            [HEADER_STATUS]: "cancelled",
          },
        }),
      ),
    ).toEqual([
      event("start:first:", "msg-1", 1),
      event("delta:late:late", "msg-1", 1),
      event("end:late", "msg-1", 1),
    ]);
    expect(firstContact).toEqual(["msg-1"]);
  });

  it("uses prefix-match updates as suffix deltas", () => {
    const decoder = createDecoderCore(hooks());

    decoder.decode(
      message({
        action: "create",
        data: "hello",
        transport: { [HEADER_STREAM]: "true" },
      }),
    );

    expect(
      decoder.decode(
        message({
          action: "update",
          data: "hello world",
          transport: { [HEADER_STREAM]: "true" },
          serial: 2,
        }),
      ),
    ).toEqual([event("delta: world:hello world", "msg-1", 2)]);
  });

  it("closes streams on terminal metadata-only updates without replacing content", () => {
    const decoder = createDecoderCore(hooks());

    decoder.decode(
      message({
        action: "create",
        data: "hello",
        transport: { [HEADER_STREAM]: "true" },
      }),
    );

    expect(
      decoder.decode(
        message({
          action: "update",
          data: undefined,
          transport: {
            [HEADER_STREAM]: "true",
            [HEADER_STATUS]: "complete",
          },
          serial: 2,
        }),
      ),
    ).toEqual([event("end:hello", "msg-1", 2)]);
  });

  it("tracks concurrent streams independently under one codec message id", () => {
    const decoder = createDecoderCore(hooks());
    const common = {
      [HEADER_CODEC_MESSAGE_ID]: "assistant-1",
      [HEADER_STREAM]: "true",
    };

    expect(
      decoder.decode(
        message({
          action: "update",
          data: " thinking",
          messageSerial: "assistant-1",
          transport: {
            ...common,
            [HEADER_STREAM_ID]: "reasoning:1",
            [HEADER_STATUS]: "complete",
          },
        }),
      ),
    ).toEqual([
      event("start:first:", "assistant-1", 1),
      event("delta: thinking: thinking", "assistant-1", 1),
      event("end: thinking", "assistant-1", 1),
    ]);

    expect(
      decoder.decode(
        message({
          action: "update",
          data: " answer",
          messageSerial: "assistant-1",
          serial: 2,
          transport: {
            ...common,
            [HEADER_STREAM_ID]: "text:1",
            [HEADER_STATUS]: "complete",
          },
        }),
      ),
    ).toEqual([
      event("start:first:", "assistant-1", 2),
      event("delta: answer: answer", "assistant-1", 2),
      event("end: answer", "assistant-1", 2),
    ]);
  });

  it("emits replacement resync events for non-prefix updates", () => {
    const replacements: string[] = [];
    const decoder = createDecoderCore(hooks(), {
      onReplacement: (messageId) => replacements.push(messageId),
    });

    decoder.decode(
      message({
        action: "create",
        data: "abc",
        transport: { [HEADER_STREAM]: "true" },
      }),
    );

    expect(
      decoder.decode(
        message({
          action: "update",
          data: "xyz",
          transport: { [HEADER_STREAM]: "true" },
          serial: 2,
        }),
      ),
    ).toEqual([
      event("start:xyz", "msg-1", 2),
      event("delta:xyz:xyz", "msg-1", 2),
    ]);
    expect(replacements).toEqual(["msg-1"]);
  });

  it("cleans up deleted streams", () => {
    const decoder = createDecoderCore(hooks());

    decoder.decode(
      message({
        action: "create",
        data: "abc",
        transport: { [HEADER_STREAM]: "true" },
      }),
    );

    expect(
      decoder.decode(
        message({
          action: "delete",
          data: "",
          serial: 2,
        }),
      ),
    ).toEqual([event("delete:abc", "msg-1", 2)]);
  });

  it("evicts least-recently-used trackers at the configured cap", () => {
    const evicted: string[] = [];
    const decoder = createDecoderCore(hooks(), {
      maxStreams: 1,
      onTrackerEvicted: (messageId) => evicted.push(messageId),
    });

    decoder.decode(
      message({
        action: "create",
        messageSerial: "msg-1",
        data: "a",
        transport: { [HEADER_STREAM]: "true" },
      }),
    );
    decoder.decode(
      message({
        action: "create",
        messageSerial: "msg-2",
        data: "b",
        transport: { [HEADER_STREAM]: "true" },
      }),
    );

    expect(evicted).toEqual(["msg-1"]);
  });

  it("preserves final stream state for random append interleavings", () => {
    fc.assert(
      fc.property(
        fc.array(fc.string({ maxLength: 8 }), { minLength: 1, maxLength: 40 }),
        (deltas) => {
          expect(decodeCompleteStream(deltas, 0)).toBe(deltas.join(""));
        },
      ),
      { numRuns: 100 },
    );
  });

  it("converges for random mid-stream first-contact join points", () => {
    fc.assert(
      fc.property(
        fc.array(fc.string({ maxLength: 8 }), { minLength: 1, maxLength: 40 }),
        fc.integer({ min: 0, max: 39 }),
        (deltas, rawJoinIndex) => {
          const joinIndex = Math.min(rawJoinIndex, deltas.length - 1);
          expect(decodeCompleteStream(deltas, joinIndex)).toBe(deltas.join(""));
        },
      ),
      { numRuns: 100 },
    );
  });
});

interface StreamEvent {
  kind: "start" | "delta" | "end";
  value: string;
}

function decodeCompleteStream(
  deltas: readonly string[],
  joinIndex: number,
): string {
  const decoder = createDecoderCore<StreamEvent>({
    buildStartEvents(tracker) {
      return [streamEvent("start", tracker.accumulated, tracker)];
    },
    buildDeltaEvents(tracker) {
      return [streamEvent("delta", tracker.accumulated, tracker)];
    },
    buildEndEvents(tracker) {
      return [streamEvent("end", tracker.accumulated, tracker)];
    },
    decodeDiscrete() {
      return [];
    },
  });
  const events: DecodedEvent<StreamEvent>[] = [];
  if (joinIndex === 0) {
    events.push(
      ...decoder.decode(
        message({
          action: "create",
          data: deltas[0],
          transport: {
            [HEADER_STREAM]: "true",
            [HEADER_STATUS]: deltas.length === 1 ? "complete" : "streaming",
          },
        }),
      ),
    );
  } else {
    events.push(
      ...decoder.decode(
        message({
          action: "append",
          data: deltas.slice(0, joinIndex + 1).join(""),
          transport: {
            [HEADER_STREAM]: "true",
            [HEADER_STATUS]:
              joinIndex === deltas.length - 1 ? "complete" : "streaming",
          },
        }),
      ),
    );
  }
  for (let index = joinIndex + 1; index < deltas.length; index += 1) {
    events.push(
      ...decoder.decode(
        message({
          action: "append",
          data: deltas.slice(0, index + 1).join(""),
          serial: index + 1,
          transport: {
            [HEADER_STREAM]: "true",
            [HEADER_STATUS]:
              index === deltas.length - 1 ? "complete" : "streaming",
          },
        }),
      ),
    );
  }
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const decoded = events[index];
    if (decoded?.event.kind === "end") {
      return decoded.event.value;
    }
  }
  return "";
}

function streamEvent(
  kind: StreamEvent["kind"],
  value: string,
  tracker: { messageId: string; message: InboundMessage },
): DecodedEvent<StreamEvent> {
  return {
    event: { kind, value },
    messageId: tracker.messageId,
    meta: {
      serial: tracker.message.historySerial,
      messageId: tracker.messageId,
    },
  };
}

function hooks(): DecoderCoreHooks<string> {
  return {
    buildStartEvents(tracker) {
      const prefix = tracker.firstContact ? "start:first" : "start";
      return [
        event(
          `${prefix}:${tracker.accumulated}`,
          tracker.messageId,
          tracker.message.historySerial,
        ),
      ];
    },
    buildDeltaEvents(tracker, delta) {
      return [
        event(
          `delta:${delta}:${tracker.accumulated}`,
          tracker.messageId,
          tracker.message.historySerial,
        ),
      ];
    },
    buildEndEvents(tracker) {
      return [
        event(
          `end:${tracker.accumulated}`,
          tracker.messageId,
          tracker.message.historySerial,
        ),
      ];
    },
    decodeDiscrete(inbound) {
      return [
        event(
          `discrete:${String(inbound.data)}`,
          inbound.messageSerial,
          inbound.historySerial,
        ),
      ];
    },
    buildDeleteEvents(tracker) {
      return [
        event(
          `delete:${tracker.accumulated}`,
          tracker.messageId,
          tracker.message.historySerial,
        ),
      ];
    },
  };
}

function event(
  value: string,
  messageId: string,
  serial: Serial,
): DecodedEvent<string> {
  return {
    event: value,
    messageId,
    meta: {
      serial,
      messageId,
    },
  };
}

interface MessageOptions {
  action: InboundMessageAction;
  data: unknown;
  messageSerial?: string;
  serial?: Serial;
  transport?: Record<string, string>;
  codec?: Record<string, string>;
}

function message(options: MessageOptions): InboundMessage {
  const messageSerial = options.messageSerial ?? "msg-1";
  const serial = options.serial ?? 1;
  const transport = Object.create(null) as Record<string, string>;
  Object.assign(transport, options.transport);
  transport[HEADER_CODEC_MESSAGE_ID] = messageSerial;
  const codec = Object.create(null) as Record<string, string>;
  Object.assign(codec, options.codec);
  return {
    name: "ai-output",
    data: options.data,
    action: options.action,
    messageSerial,
    historySerial: serial,
    timestamp: 0,
    raw: {},
    getTransportHeaders() {
      return transport;
    },
    getCodecHeaders() {
      return codec;
    },
  };
}
