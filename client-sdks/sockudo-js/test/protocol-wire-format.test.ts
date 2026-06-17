import { beforeAll, describe, expect, it, vi } from "vitest";

let Protocol: typeof import("../src/core/connection/protocol/protocol").default;
let setProtocolVersion: typeof import("../src/core/protocol_prefix").setProtocolVersion;
let setWireFormat: typeof import("../src/core/wire_format").setWireFormat;
let ws: typeof import("../src/core/transports/url_schemes").ws;
let Channel: typeof import("../src/core/channels/channel").default;
let Sockudo: typeof import("../src/core/sockudo").default;

beforeAll(async () => {
  Object.assign(globalThis, {
    VERSION: "test-version",
    CDN_HTTP: "",
    CDN_HTTPS: "",
    DEPENDENCY_SUFFIX: "",
  });

  ({
    default: Protocol,
  } = await import("../src/core/connection/protocol/protocol"));
  ({ setProtocolVersion } = await import("../src/core/protocol_prefix"));
  ({ setWireFormat } = await import("../src/core/wire_format"));
  ({ ws } = await import("../src/core/transports/url_schemes"));
  ({ default: Channel } = await import("../src/core/channels/channel"));
  ({ default: Sockudo } = await import("../src/core/sockudo"));
});

describe("protocol wire formats", () => {
  it("uses v1 by default and omits the format query", () => {
    setProtocolVersion(7);
    const url = ws.getInitial("app-key", {
      useTLS: false,
      hostTLS: "ws.example.com",
      hostNonTLS: "ws.example.com",
      httpPath: "",
      wireFormat: "messagepack",
      echoMessages: true,
    });

    expect(url).toContain("protocol=7");
    expect(url).not.toContain("format=");
  });

  it("encodes websocket URL with v2 format query", () => {
    setProtocolVersion(2);
    const url = ws.getInitial("app-key", {
      useTLS: false,
      hostTLS: "ws.example.com",
      hostNonTLS: "ws.example.com",
      httpPath: "",
      wireFormat: "messagepack",
      echoMessages: false,
    });

    expect(url).toContain("protocol=2");
    expect(url).toContain("format=messagepack");
    expect(url).toContain("echo_messages=false");
  });

  it("round trips messagepack", () => {
    setWireFormat("messagepack");
    const payload = Protocol.encodeMessage({
      event: "sockudo:test",
      channel: "chat:room-1",
      data: { hello: "world", count: 3 },
      stream_id: "stream-1",
      message_id: "msg-1",
      serial: 7,
      sequence: 7,
      conflation_key: "room",
    });

    const decoded = Protocol.decodeMessage({
      data: payload,
    } as MessageEvent);

    expect(decoded.event).toBe("sockudo:test");
    expect(decoded.channel).toBe("chat:room-1");
    expect(decoded.data).toEqual({ hello: "world", count: 3 });
    expect(decoded.stream_id).toBe("stream-1");
    expect(decoded.message_id).toBe("msg-1");
    expect(decoded.serial).toBe(7);
    expect(decoded.sequence).toBe(7);
    expect(decoded.conflation_key).toBe("room");
  });

  it("round trips protobuf", () => {
    setWireFormat("protobuf");
    const payload = Protocol.encodeMessage({
      event: "sockudo:test",
      channel: "chat:room-1",
      data: { hello: "world" },
      stream_id: "stream-2",
      message_id: "msg-2",
      serial: 9,
      sequence: 11,
      conflation_key: "btc",
      extras: {
        headers: { region: "eu", ttl: 5, replay: true },
        echo: false,
      },
    });

    const decoded = Protocol.decodeMessage({
      data: payload,
    } as MessageEvent);

    expect(decoded.event).toBe("sockudo:test");
    expect(decoded.channel).toBe("chat:room-1");
    expect(decoded.data).toEqual({ hello: "world" });
    expect(decoded.stream_id).toBe("stream-2");
    expect(decoded.message_id).toBe("msg-2");
    expect(decoded.serial).toBe(9);
    expect(decoded.sequence).toBe(11);
    expect(decoded.conflation_key).toBe("btc");
    expect(decoded.extras).toEqual({
      headers: { region: "eu", ttl: 5, replay: true },
      echo: false,
      ephemeral: undefined,
      idempotency_key: undefined,
    });
  });

  it("serializes rewind subscription options", () => {
    const sent: Array<{ event: string; data: any; channel?: string }> = [];
    const mockSockudo = {
      connection: { socket_id: "1.1", state: "connected" },
      send_event(event: string, data: any, channel?: string) {
        sent.push({ event, data, channel });
        return true;
      },
      unsubscribe() {},
    };

    const channel = new Channel("market:btc", mockSockudo as any);
    channel.rewind = { seconds: 30 };
    channel.subscribe();

    expect(sent[0]?.data?.rewind).toEqual({ seconds: 30 });
  });

  it("treats rewind-only options as subscription options", () => {
    const fakeChannel = {
      subscriptionPending: false,
      subscriptionCancelled: false,
      tagsFilter: null,
      eventsFilter: null,
      rewind: null,
      setDeltaSettings: vi.fn(),
      subscribe: vi.fn(),
      reinstateSubscription: vi.fn(),
    };
    const fakeSockudo = {
      channels: {
        add: vi.fn(() => fakeChannel),
      },
      connection: {
        state: "connected",
      },
    };

    Sockudo.prototype.subscribe.call(fakeSockudo, "market:btc", {
      rewind: { count: 2 },
    });

    expect(fakeChannel.tagsFilter).toBeNull();
    expect(fakeChannel.rewind).toEqual({ count: 2 });
    expect(fakeChannel.subscribe).toHaveBeenCalledTimes(1);
  });
});
