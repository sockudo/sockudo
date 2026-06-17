import { describe, expect, it } from "vitest";

import { ErrorCode, ErrorInfo } from "../errors.js";
import { getCodecHeaders, getTransportHeaders } from "../utils.js";
import {
  adaptSockudoChannel,
  compareSerial,
  normalizeInboundMessage,
  validateAppendRollupWindow,
  type SockudoChannelPeer,
  type SockudoRawMessage,
} from "./adapter.js";

describe("realtime adapter", () => {
  it("normalizes mutable messages with lazy hostile-safe header views", () => {
    const raw: SockudoRawMessage = {
      event: "sockudo:message.append",
      channel: "chat",
      name: "ai-output",
      data: "hello",
      message_id: "event-1",
      serial: "9007199254740994",
      extras: {
        ai: {
          transport: {
            "turn-id": "turn-1",
            __proto__: "pollution",
          },
          codec: {
            provider: "test",
          },
        },
        headers: {
          sockudo_action: "message.append",
          sockudo_message_serial: "msg-1",
          sockudo_history_serial: "9007199254740993",
          sockudo_version_serial: "ver-1",
        },
      },
    };

    const message = normalizeInboundMessage(raw, () => ({
      action: "message.append",
      event: raw.event,
      messageSerial: "msg-1",
      historySerial: "9007199254740993",
      versionSerial: "ver-1",
    }));

    expect(message).toMatchObject({
      name: "ai-output",
      action: "append",
      messageSerial: "msg-1",
      historySerial: "9007199254740993",
      deliverySerial: "9007199254740994",
      messageId: "event-1",
    });
    expect(Object.getPrototypeOf(message.getTransportHeaders())).toBe(null);
    expect(message.getTransportHeaders()["turn-id"]).toBe("turn-1");
    expect(message.getCodecHeaders().provider).toBe("test");
    expect(({} as Record<string, unknown>).pollution).toBeUndefined();
  });

  it("infers ai-output for assistant mutable mutation frames without logical names", () => {
    const message = normalizeInboundMessage({
      event: "sockudo:message.update",
      channel: "chat",
      data: "hello world",
      serial: 7,
      extras: {
        headers: {
          "x-sockudo-role": "assistant",
          "x-sockudo-stream": "true",
          "x-sockudo-status": "complete",
          "x-sockudo-codec-type": "text-end",
          "x-sockudo-codec-message-id": "assistant-1",
          sockudo_action: "message.update",
          sockudo_message_serial: "msg-1",
          sockudo_history_serial: 7,
        },
      },
    });

    expect(message.name).toBe("ai-output");
    expect(message.action).toBe("update");
    expect(message.getTransportHeaders()).toMatchObject({
      role: "assistant",
      stream: "true",
      status: "complete",
      "codec-message-id": "assistant-1",
    });
    expect(message.getCodecHeaders()).toMatchObject({
      type: "text-end",
      "message-id": "assistant-1",
    });
  });

  it("compares unsafe integer serial strings as integers", () => {
    expect(compareSerial("9007199254740993", "9007199254740994")).toBe(-1);
    expect(compareSerial("9007199254740994", "9007199254740993")).toBe(1);
    expect(compareSerial("9007199254740993", "9007199254740993")).toBe(0);
  });

  it("validates append rollup windows locally", () => {
    expect(() => {
      validateAppendRollupWindow(40);
    }).not.toThrow();
    expect(() => {
      validateAppendRollupWindow(7);
    }).toThrow(ErrorInfo);
    expect(() => {
      validateAppendRollupWindow(7);
    }).toThrow(/appendRollupWindow/);
  });

  it("maps Sockudo channel methods and client-side name filtering", async () => {
    const listeners: ((event: SockudoRawMessage) => void)[] = [];
    const channel: SockudoChannelPeer = {
      name: "chat",
      publishCreate: () =>
        Promise.resolve({
          message_serial: "msg-1",
          history_serial: 1,
          delivery_serial: 2,
          version_serial: "ver-1",
        }),
      appendMessage: () =>
        Promise.resolve({
          messageSerial: "msg-1",
          historySerial: 3,
        }),
      channelHistory: () =>
        Promise.resolve({
          items: [
            {
              event: "ai-output",
              name: "ai-output",
              data: "from-history",
              message_serial: "msg-history",
              history_serial: 1,
            },
          ],
          hasNext: () => false,
        }),
      handleEvent(event) {
        for (const listener of listeners) {
          listener(event);
        }
      },
    };

    const adapted = adaptSockudoChannel(channel);
    const delivered: string[] = [];
    adapted.subscribe((message) => delivered.push(message.name), {
      names: ["ai-output"],
    });
    channel.handleEvent?.({
      event: "ignored",
      name: "ignored",
      data: null,
      history_serial: 1,
      message_serial: "ignored",
    });
    channel.handleEvent?.({
      event: "ai-output",
      name: "ai-output",
      data: null,
      history_serial: 2,
      message_serial: "msg-2",
    });

    await expect(adapted.publish({ name: "ai-output" })).resolves.toMatchObject(
      {
        messageSerial: "msg-1",
        historySerial: 1,
        deliverySerial: 2,
        versionSerial: "ver-1",
      },
    );
    await expect(adapted.appendMessage("msg-1", "x")).resolves.toMatchObject({
      historySerial: 3,
    });
    await expect(adapted.history({ untilAttach: true })).resolves.toMatchObject(
      {
        items: [expect.objectContaining({ messageSerial: "msg-history" })],
      },
    );
    expect(delivered).toEqual(["ai-output"]);
  });
});

describe("header utilities", () => {
  it("returns empty null-prototype maps for missing tiers", () => {
    expect(Object.getPrototypeOf(getTransportHeaders({}))).toBe(null);
    expect(Object.keys(getCodecHeaders({}))).toEqual([]);
  });

  it("ignores non-string headers", () => {
    expect(
      getTransportHeaders({
        ai: {
          transport: {
            valid: "yes",
            invalid: 1,
          },
        },
      }),
    ).toEqual({ valid: "yes" });
  });

  it("maps Sockudo x-sockudo flat headers into transport and codec tiers", () => {
    const extras = {
      headers: {
        "x-sockudo-turn-id": "turn-1",
        "x-sockudo-client-id": "client-1",
        "x-sockudo-role": "assistant",
        "x-sockudo-status": "streaming",
        "x-sockudo-stream": true,
        "x-sockudo-turn-continue": { invalid: true },
        "x-sockudo-stream-id": "text:msg-1:text",
        "x-sockudo-codec-message-id": "msg-1",
        "x-sockudo-codec-type": "text-delta",
        "x-sockudo-codec-id": "text",
        "x-sockudo-codec-provider-executed": 1,
        "x-sockudo-tool-call-id": "tool-1",
        ignored: "value",
      },
    };

    expect(getTransportHeaders(extras)).toEqual({
      "turn-id": "turn-1",
      "turn-client-id": "client-1",
      role: "assistant",
      status: "streaming",
      stream: "true",
      "stream-id": "text:msg-1:text",
      "codec-message-id": "msg-1",
    });
    expect(getCodecHeaders(extras)).toEqual({
      "message-id": "msg-1",
      type: "text-delta",
      id: "text",
      "provider-executed": "1",
      "tool-call-id": "tool-1",
    });
  });

  it("lets extras.ai override x-sockudo fallback headers", () => {
    const extras = {
      headers: {
        "x-sockudo-turn-id": "fallback-turn",
        "x-sockudo-codec-type": "text-delta",
      },
      ai: {
        transport: { "turn-id": "ai-turn" },
        codec: { type: "reasoning-delta" },
      },
    };

    expect(getTransportHeaders(extras)["turn-id"]).toBe("ai-turn");
    expect(getCodecHeaders(extras).type).toBe("reasoning-delta");
  });
});

describe("error mapping", () => {
  it("preserves ErrorInfo code semantics", () => {
    const error = new ErrorInfo({
      code: ErrorCode.ChannelContinuityLost,
      message: "unable to maintain channel continuity; position_expired",
    });
    expect(error.statusCode).toBe(500);
  });
});
