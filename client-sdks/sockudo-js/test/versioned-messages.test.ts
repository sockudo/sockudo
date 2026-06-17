import { afterEach, describe, expect, it, vi } from "vitest";

import Channel from "../src/core/channels/channel";
import {
  getMutableMessageInfo,
  isMutableMessageEvent,
  reduceMutableMessageEvent,
  reduceMutableMessageEvents,
} from "../src/core/versioned_messages";
import type { SockudoEvent } from "../src/core/connection/protocol/message-types";
import { setProtocolVersion } from "../src/core/protocol_prefix";

function mutableEvent(
  action: "message.update" | "message.delete" | "message.append",
  data: unknown,
): SockudoEvent {
  return {
    event: `sockudo:${action}`,
    channel: "chat:room-1",
    data,
    name: "chat.message",
    serial: 12,
    stream_id: "stream-1",
    message_id: "msg-1",
    extras: {
      headers: {
        sockudo_action: action,
        sockudo_message_serial: "message-1",
        sockudo_version_serial: "version-2",
        sockudo_history_serial: 7,
        sockudo_version_timestamp_ms: 1713100805000,
      },
    },
  };
}

function makeChannel() {
  return new Channel("chat:room-1", {
    config: {
      versionedMessages: {
        endpoint: "/sockudo/versioned",
      },
    },
  } as any);
}

afterEach(() => {
  vi.restoreAllMocks();
  setProtocolVersion(7);
});

describe("versioned message helpers", () => {
  it("detects mutable message events from runtime headers", () => {
    const event = mutableEvent("message.update", { text: "patched" });

    expect(isMutableMessageEvent(event)).toBe(true);
    expect(getMutableMessageInfo(event)).toEqual({
      action: "message.update",
      event: "sockudo:message.update",
      messageSerial: "message-1",
      versionSerial: "version-2",
      historySerial: 7,
      versionTimestampMs: 1713100805000,
    });
  });

  it("treats update as replace semantics", () => {
    const next = reduceMutableMessageEvent(
      null,
      mutableEvent("message.update", { text: "patched" }),
    );

    expect(next.action).toBe("message.update");
    expect(next.data).toEqual({ text: "patched" });
    expect(next.messageSerial).toBe("message-1");
  });

  it("treats append as concatenate semantics", () => {
    const updated = reduceMutableMessageEvent(
      null,
      mutableEvent("message.update", "hello"),
    );
    const appended = reduceMutableMessageEvent(
      updated,
      mutableEvent("message.append", " world"),
    );

    expect(appended.action).toBe("message.append");
    expect(appended.data).toBe("hello world");
  });

  it("treats delete as replace-to-null semantics by default", () => {
    const updated = reduceMutableMessageEvent(
      null,
      mutableEvent("message.update", { text: "hello" }),
    );
    const deleted = reduceMutableMessageEvent(
      updated,
      mutableEvent("message.delete", null),
    );

    expect(deleted.action).toBe("message.delete");
    expect(deleted.data).toBeNull();
  });

  it("reduces ordered version history events to the latest visible state", () => {
    const latest = reduceMutableMessageEvents([
      mutableEvent("message.update", "hello"),
      mutableEvent("message.append", " world"),
    ]);

    expect(latest?.data).toBe("hello world");
    expect(latest?.action).toBe("message.append");
  });

  it("throws when append arrives without a string base", () => {
    expect(() =>
      reduceMutableMessageEvent(null, mutableEvent("message.append", " world")),
    ).toThrow(/requires an existing string base/);
  });

  it("fetches the latest message through the configured proxy endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        channel: "chat:room-1",
        item: {
          message_serial: "msg:1",
          action: "update",
          data: "hello brave",
        },
      }),
    });
    vi.stubGlobal("fetch", fetchMock as any);

    const channel = makeChannel();
    const payload = await channel.getMessage("msg:1");

    expect(payload.item.data).toBe("hello brave");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("fetches paged message versions through the configured proxy endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        channel: "chat:room-1",
        direction: "oldest_first",
        limit: 2,
        has_more: true,
        next_cursor: "cursor-2",
        items: [
          { message_serial: "msg:1", action: "update", data: "hello brave" },
        ],
      }),
    });
    vi.stubGlobal("fetch", fetchMock as any);

    const channel = makeChannel();
    const payload = await channel.getMessageVersions("msg:1", {
      limit: 2,
      direction: "oldest_first",
    });

    expect(payload.items).toHaveLength(1);
    expect(payload.hasNext()).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("fetches channel history through the configured proxy endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        items: [{ serial: 1, event_name: "sockudo:message.update" }],
        direction: "newest_first",
        limit: 1,
        has_more: false,
        bounds: {},
        continuity: {},
      }),
    });
    vi.stubGlobal("fetch", fetchMock as any);

    const channel = makeChannel();
    const page = await channel.channelHistory({
      limit: 1,
      direction: "newest_first",
    });

    expect(page.items).toHaveLength(1);
    expect(page.hasNext()).toBe(false);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("publishes annotations through the configured proxy endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ annotationSerial: "ann:1" }),
    });
    vi.stubGlobal("fetch", fetchMock as any);

    const channel = makeChannel();
    const payload = await channel.publishAnnotation("msg:1", {
      type: "reactions:distinct.v1",
      name: "like",
      clientId: "user-1",
    });

    expect(payload.annotationSerial).toBe("ann:1");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      action: "publish_annotation",
      channel: "chat:room-1",
      messageSerial: "msg:1",
      annotation: {
        type: "reactions:distinct.v1",
        name: "like",
        clientId: "user-1",
      },
    });
  });

  it("deletes annotations through the configured proxy endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        annotationSerial: "ann:2",
        deletedAnnotationSerial: "ann:1",
      }),
    });
    vi.stubGlobal("fetch", fetchMock as any);

    const channel = makeChannel();
    const payload = await channel.deleteAnnotation("msg:1", "ann:1", "1.1");

    expect(payload.deletedAnnotationSerial).toBe("ann:1");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      action: "delete_annotation",
      channel: "chat:room-1",
      messageSerial: "msg:1",
      annotationSerial: "ann:1",
      socketId: "1.1",
    });
  });

  it("lists annotation events through the configured proxy endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        channel: "chat:room-1",
        messageSerial: "msg:1",
        limit: 1,
        hasMore: false,
        nextCursor: null,
        items: [{ action: "annotation.create", serial: "ann:1" }],
      }),
    });
    vi.stubGlobal("fetch", fetchMock as any);

    const channel = makeChannel();
    const payload = await channel.listAnnotations("msg:1", {
      type: "reactions:distinct.v1",
      limit: 1,
    });

    expect(payload.items).toHaveLength(1);
    expect(payload.hasNext()).toBe(false);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      action: "list_annotations",
      channel: "chat:room-1",
      messageSerial: "msg:1",
      params: {
        type: "reactions:distinct.v1",
        limit: 1,
      },
    });
  });

  it("emits annotation summaries and raw annotation actions", () => {
    setProtocolVersion(2);
    const channel = makeChannel();
    const summary = vi.fn();
    const raw = vi.fn();

    channel.bind("message.summary", summary);
    channel.bind("annotation.create", raw);

    channel.handleEvent({
      event: "sockudo_internal:message",
      data: {
        action: "message.summary",
        serial: "msg:1",
        annotations: { summary: {} },
      },
    } as any);
    channel.handleEvent({
      event: "sockudo_internal:annotation",
      data: {
        action: "annotation.create",
        serial: "ann:1",
        messageSerial: "msg:1",
        type: "reactions:distinct.v1",
      },
    } as any);

    expect(summary).toHaveBeenCalledTimes(1);
    expect(raw).toHaveBeenCalledTimes(1);
  });
});
