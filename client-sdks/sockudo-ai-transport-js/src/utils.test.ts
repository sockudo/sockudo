import { describe, expect, it } from "vitest";

import {
  EVENT_AI_CANCEL,
  EVENT_AI_INPUT,
  EVENT_AI_OUTPUT,
  EVENT_AI_TURN_END,
  EVENT_AI_TURN_START,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_DISCRETE,
  HEADER_ERROR_CODE,
  HEADER_ERROR_MESSAGE,
  HEADER_EVENT_ID,
  HEADER_FORK_OF,
  HEADER_INPUT_CLIENT_ID,
  HEADER_INVOCATION_ID,
  HEADER_MSG_REGENERATE,
  HEADER_PARENT,
  HEADER_ROLE,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_STREAM_ID,
  HEADER_TURN_CLIENT_ID,
  HEADER_TURN_CONTINUE,
  HEADER_TURN_ID,
  HEADER_TURN_REASON,
} from "./constants.js";
import {
  buildTransportHeaders,
  getCodecHeaders,
  getTransportHeaders,
  headerReader,
  headerWriter,
  mergeHeaders,
  stripUndefined,
} from "./utils.js";

describe("constants", () => {
  it("exports locked event names and transport header keys", () => {
    expect([
      EVENT_AI_INPUT,
      EVENT_AI_OUTPUT,
      EVENT_AI_TURN_START,
      EVENT_AI_TURN_END,
      EVENT_AI_CANCEL,
    ]).toEqual(["ai-input", "ai-output", "ai-turn-start", "ai-turn-end", "ai-cancel"]);
    expect([
      HEADER_TURN_ID,
      HEADER_TURN_CLIENT_ID,
      HEADER_TURN_REASON,
      HEADER_TURN_CONTINUE,
      HEADER_INVOCATION_ID,
      HEADER_EVENT_ID,
      HEADER_CODEC_MESSAGE_ID,
      HEADER_STREAM,
      HEADER_STREAM_ID,
      HEADER_STATUS,
      HEADER_DISCRETE,
      HEADER_ROLE,
      HEADER_PARENT,
      HEADER_FORK_OF,
      HEADER_MSG_REGENERATE,
      HEADER_ERROR_CODE,
      HEADER_ERROR_MESSAGE,
      HEADER_INPUT_CLIENT_ID,
    ]).toEqual([
      "turn-id",
      "turn-client-id",
      "turn-reason",
      "turn-continue",
      "invocation-id",
      "event-id",
      "codec-message-id",
      "stream",
      "stream-id",
      "status",
      "discrete",
      "role",
      "parent",
      "fork-of",
      "msg-regenerate",
      "error-code",
      "error-message",
      "input-client-id",
    ]);
  });
});

describe("header utilities", () => {
  it("reads hostile extras defensively into null-prototype maps", () => {
    const extras = {
      ai: {
        transport: {
          "turn-id": "turn-1",
          __proto__: "pollution",
          ignored: 1,
        },
        codec: {
          unicode: "żółć",
          oversized: "x".repeat(300),
        },
      },
    };

    const transport = getTransportHeaders(extras);
    const codec = getCodecHeaders(extras);

    expect(Object.getPrototypeOf(transport)).toBe(null);
    expect(Object.getPrototypeOf(codec)).toBe(null);
    expect(transport["turn-id"]).toBe("turn-1");
    expect(transport.ignored).toBeUndefined();
    expect(codec.unicode).toBe("żółć");
    expect(codec.oversized).toHaveLength(300);
    expect(({} as Record<string, unknown>).pollution).toBeUndefined();
    expect(getTransportHeaders({})).toEqual({});
    expect(getCodecHeaders(null)).toEqual({});
  });

  it("writes, reads, merges, and strips headers", () => {
    const writer = headerWriter();
    writer.str("s", "hello");
    writer.str("skip-str", undefined);
    writer.bool("yes", true);
    writer.bool("no", false);
    writer.bool("skip-bool", undefined);
    writer.json("json", { ok: true });
    writer.json("skip-json", undefined);
    writer.set("num", 1);
    writer.set("legacy-bool", true);
    writer.set("skip-set", undefined);
    writer.setJson("legacy-json", ["x"]);

    const merged = mergeHeaders(writer.headers, { other: "value" });
    const reader = headerReader(merged);

    expect(reader.str("s")).toBe("hello");
    expect(reader.string("other")).toBe("value");
    expect(reader.bool("yes")).toBe(true);
    expect(reader.bool("no")).toBe(false);
    expect(reader.boolean("legacy-bool")).toBe(true);
    expect(reader.boolean("missing")).toBeUndefined();
    expect(reader.json("json")).toEqual({ ok: true });
    expect(reader.json("legacy-json")).toEqual(["x"]);
    expect(reader.json("missing")).toBeUndefined();
    expect(headerReader({ broken: "{" }).json("broken")).toBeUndefined();
    expect(Object.getPrototypeOf(merged)).toBe(null);
    expect(stripUndefined({ a: 1, b: undefined, c: null })).toEqual({
      a: 1,
      c: null,
    });
  });

  it("builds canonical transport headers", () => {
    const headers = buildTransportHeaders({
      role: "assistant",
      turnId: "turn-1",
      codecMessageId: "msg-1",
      turnClientId: "client-1",
      parent: "parent-1",
      forkOf: "fork-1",
      regenerates: true,
      invocationId: "inv-1",
      inputClientId: "client-2",
      inputEventId: "event-1",
      turnContinue: false,
    });
    const reader = headerReader(headers);

    expect(reader.string(HEADER_ROLE)).toBe("assistant");
    expect(reader.string(HEADER_TURN_ID)).toBe("turn-1");
    expect(reader.string(HEADER_CODEC_MESSAGE_ID)).toBe("msg-1");
    expect(reader.string(HEADER_TURN_CLIENT_ID)).toBe("client-1");
    expect(reader.string(HEADER_PARENT)).toBe("parent-1");
    expect(reader.string(HEADER_FORK_OF)).toBe("fork-1");
    expect(reader.boolean(HEADER_MSG_REGENERATE)).toBe(true);
    expect(reader.string(HEADER_INVOCATION_ID)).toBe("inv-1");
    expect(reader.string(HEADER_INPUT_CLIENT_ID)).toBe("client-2");
    expect(reader.string(HEADER_EVENT_ID)).toBe("event-1");
    expect(reader.boolean(HEADER_TURN_CONTINUE)).toBe(false);
    expect(buildTransportHeaders({})).toEqual({});
  });
});
