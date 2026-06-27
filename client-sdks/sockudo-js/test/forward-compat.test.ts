import { readFileSync } from "node:fs";
import { encode as encodeMsgpack } from "@msgpack/msgpack";
import { describe, expect, it, beforeAll, afterEach, vi } from "vitest";

import Channel from "../src/core/channels/channel";
import PresenceChannel from "../src/core/channels/presence_channel";
import Protocol from "../src/core/connection/protocol/protocol";
import { setProtocolVersion } from "../src/core/protocol_prefix";
import { setWireFormat } from "../src/core/wire_format";

const fixtureNames = [
  "future-v2-frame.json",
  "future-versioned-action.json",
  "future-webhook-events.json",
  "unknown-ai-extras.json",
] as const;

function fixtureRaw(name: (typeof fixtureNames)[number]): string {
  return readFileSync(
    new URL(`../../../tests/ai-conformance/fixtures/forward-compat/${name}`, import.meta.url),
    "utf8",
  );
}

function decodeRaw(raw: string) {
  return Protocol.decodeMessage({ data: raw } as MessageEvent);
}

function replayRaw(channel: Channel, raw: string) {
  const event = decodeRaw(raw);
  if (typeof event.event === "string" && event.event.length > 0 && event.channel === channel.name) {
    channel.handleEvent(event);
  }
  return event;
}

beforeAll(() => {
  Object.assign(globalThis, {
    VERSION: "test-version",
    CDN_HTTP: "",
    CDN_HTTPS: "",
    DEPENDENCY_SUFFIX: "",
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  setProtocolVersion(7);
  setWireFormat("json");
});

describe("forward-compatible realtime fixture replay", () => {
  it("decodes and replays forward-compat frames without corrupting channel state", () => {
    setProtocolVersion(2);
    setWireFormat("json");
    const channel = new Channel("private-ai-forward", {
      connection: { socket_id: "1.1", state: "connected" },
      send_event: vi.fn(),
      unsubscribe: vi.fn(),
    } as any);
    const known = vi.fn();
    const global = vi.fn();
    channel.bind("app-known", known);
    channel.bind_global(global);

    channel.handleEvent({
      event: "app-known",
      channel: "private-ai-forward",
      data: { before: true },
    } as any);
    const beforeState = {
      subscribed: channel.subscribed,
      subscriptionPending: channel.subscriptionPending,
      subscriptionCancelled: channel.subscriptionCancelled,
    };

    const decoded = fixtureNames.map((name) => replayRaw(channel, fixtureRaw(name)));

    channel.handleEvent({
      event: "app-known",
      channel: "private-ai-forward",
      data: { after: true },
    } as any);

    expect(known).toHaveBeenCalledTimes(2);
    expect({
      subscribed: channel.subscribed,
      subscriptionPending: channel.subscriptionPending,
      subscriptionCancelled: channel.subscriptionCancelled,
    }).toEqual(beforeState);
    expect(decoded.map((event) => event.event)).toEqual([
      "sockudo:future_event",
      "sockudo:message.future",
      undefined,
      "ai-output",
    ]);
    expect(global).toHaveBeenCalledWith("sockudo:future_event", {
      known: true,
      futureObject: { nested: "ignored" },
    });
  });

  it("preserves extras.ai and safe serials across decode boundaries", () => {
    setProtocolVersion(2);
    setWireFormat("json");

    const futureFrame = decodeRaw(fixtureRaw("future-v2-frame.json"));
    const extrasFrame = decodeRaw(fixtureRaw("unknown-ai-extras.json"));
    const safeFrame = decodeRaw(
      JSON.stringify({
        event: "sockudo:test",
        channel: "private-ai-forward",
        serial: 2147483648,
      }),
    );

    expect(futureFrame.serial).toBe("9007199254740993");
    expect(safeFrame.serial).toBe(2147483648);
    expect(extrasFrame.extras?.ai).toEqual({
      transport: {
        "turn-id": "turn-1",
        status: "streaming",
      },
      codec: {
        "provider-future-key": "opaque",
        "x-custom": "opaque",
      },
    });
    expect(extrasFrame.extras?.futureExtrasField).toBe(true);
  });

  it("normalizes binary uint64 serials without BigInt leakage", () => {
    setProtocolVersion(2);
    setWireFormat("messagepack");
    const msgpackPayload = encodeMsgpack(
      [
        "sockudo:test",
        "private-ai-forward",
        ["string", "content"],
        null,
        null,
        null,
        null,
        null,
        null,
        "stream-1",
        BigInt("9007199254740993"),
        null,
        {
          headers: {
            sockudo_history_serial: ["number", BigInt("9007199254740994")],
          },
          ai: {
            transport: { "turn-id": "turn-1" },
          },
        },
      ],
      { useBigInt64: true },
    );

    const decoded = Protocol.decodeMessage({ data: msgpackPayload } as MessageEvent);

    expect(decoded.serial).toBe("9007199254740993");
    expect(decoded.extras?.headers?.sockudo_history_serial).toBe("9007199254740994");
    expect(decoded.extras?.ai).toEqual({
      transport: { "turn-id": "turn-1" },
    });
  });

  it("routes unknown internal presence events without changing the member map", () => {
    setProtocolVersion(2);
    const channel = new PresenceChannel("presence-ai-forward", {
      connection: { socket_id: "1.1", state: "connected" },
      user: { signinDonePromise: Promise.resolve(), user_data: null },
      config: {},
      send_event: vi.fn(),
      unsubscribe: vi.fn(),
    } as any);
    const internal = vi.fn();
    channel.bind_global(internal);
    channel.members.setMyID("user-1");
    channel.members.onSubscription({
      presence: {
        hash: { "user-1": { role: "reader" } },
        count: 1,
      },
    });

    channel.handleEvent({
      event: "sockudo_internal:presence_update",
      channel: "presence-ai-forward",
      data: { user_id: "user-1", user_info: { role: "writer" } },
    } as any);
    channel.handleEvent({
      event: "sockudo_internal:member_added",
      channel: "presence-ai-forward",
      data: { user_info: { malformed: true } },
    } as any);

    expect(internal).toHaveBeenCalledWith("sockudo_internal:presence_update", {
      user_id: "user-1",
      user_info: { role: "writer" },
    });
    expect(channel.members.count).toBe(1);
    expect(channel.members.get("user-1")).toEqual({
      id: "user-1",
      info: { role: "reader" },
    });
    expect(channel.members.get("undefined")).toBeNull();
  });
});
