// @vitest-environment jsdom

import { createElement, act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";
import Dispatcher from "../src/core/events/dispatcher";
import Members from "../src/core/channels/members";
import { prefixedEvent } from "../src/core/protocol_prefix";
import {
  SockudoProvider,
  useChannel,
  useSockudoEvent,
} from "../src/framework-react/index";

class FakePresenceChannel extends Dispatcher {
  name = "presence-room";
  subscribed = false;
  subscriptionPending = true;
  subscriptionCount: number | null = null;
  members = new Members();
}

class FakeClient extends Dispatcher {
  subscribe = vi.fn();
  unsubscribe = vi.fn();
}

describe("react hooks", () => {
  let container: HTMLDivElement | null = null;
  let root: Root | null = null;

  afterEach(async () => {
    if (root) {
      await act(async () => {
        root?.unmount();
      });
    }
    root = null;
    container?.remove();
    container = null;
  });

  it("subscribes through the provider and tracks presence state", async () => {
    const client = new FakeClient() as any;
    const channel = new FakePresenceChannel() as any;
    let latest: any = null;

    client.subscribe.mockReturnValue(channel);

    function Consumer() {
      latest = useChannel("presence-room");
      return null;
    }

    container = document.createElement("div");
    root = createRoot(container);

    await act(async () => {
      root?.render(
        createElement(SockudoProvider, { client }, createElement(Consumer)),
      );
    });

    expect(client.subscribe).toHaveBeenCalledWith("presence-room", undefined);
    expect(latest.subscriptionPending).toBe(true);

    channel.subscribed = true;
    channel.subscriptionPending = false;
    channel.subscriptionCount = 2;
    channel.members.setMyID("u-1");
    channel.members.onSubscription({
      presence: {
        count: 2,
        hash: {
          "u-1": { name: "Ada" },
          "u-2": { name: "Linus" },
        },
      },
    });

    await act(async () => {
      channel.emit(prefixedEvent("subscription_succeeded"), channel.members);
    });

    expect(latest.subscribed).toBe(true);
    expect(latest.membersCount).toBe(2);
    expect(latest.members).toEqual([
      { id: "u-1", info: { name: "Ada" } },
      { id: "u-2", info: { name: "Linus" } },
    ]);

    await act(async () => {
      root?.unmount();
    });

    expect(client.unsubscribe).toHaveBeenCalledWith("presence-room");
  });

  it("binds and unbinds global client events", async () => {
    const client = new FakeClient() as any;
    const callback = vi.fn();

    function Consumer() {
      useSockudoEvent("sockudo:ready", callback);
      return null;
    }

    container = document.createElement("div");
    root = createRoot(container);

    await act(async () => {
      root?.render(
        createElement(SockudoProvider, { client }, createElement(Consumer)),
      );
    });

    await act(async () => {
      client.emit("sockudo:ready", { ok: true });
    });

    expect(callback).toHaveBeenCalledWith({ ok: true });
  });
});
