// @vitest-environment jsdom

import { createApp, defineComponent, h, nextTick } from "vue";
import { afterEach, describe, expect, it, vi } from "vitest";
import Dispatcher from "../src/core/events/dispatcher";
import Members from "../src/core/channels/members";
import { prefixedEvent } from "../src/core/protocol_prefix";
import {
  createSockudoPlugin,
  useChannel,
  useSockudoEvent,
} from "../src/framework-vue/index";

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

describe("vue composables", () => {
  let app: ReturnType<typeof createApp> | null = null;
  let container: HTMLDivElement | null = null;

  afterEach(() => {
    app?.unmount();
    app = null;
    container?.remove();
    container = null;
  });

  it("subscribes through the plugin and exposes reactive channel state", async () => {
    const client = new FakeClient() as any;
    const channel = new FakePresenceChannel() as any;
    let latest: any = null;

    client.subscribe.mockReturnValue(channel);

    const Root = defineComponent({
      setup() {
        latest = useChannel("presence-room");
        return () => h("div");
      },
    });

    app = createApp(Root);
    app.use(createSockudoPlugin(client));
    container = document.createElement("div");
    app.mount(container);

    expect(client.subscribe).toHaveBeenCalledWith("presence-room", undefined);
    expect(latest.subscriptionPending.value).toBe(true);

    channel.subscribed = true;
    channel.subscriptionPending = false;
    channel.subscriptionCount = 1;
    channel.members.setMyID("u-1");
    channel.members.onSubscription({
      presence: {
        count: 1,
        hash: {
          "u-1": { name: "Ada" },
        },
      },
    });
    channel.emit(prefixedEvent("subscription_succeeded"), channel.members);
    await nextTick();

    expect(latest.subscribed.value).toBe(true);
    expect(latest.membersCount.value).toBe(1);
    expect(latest.me.value).toEqual({ id: "u-1", info: { name: "Ada" } });

    app.unmount();
    expect(client.unsubscribe).toHaveBeenCalledWith("presence-room");
  });

  it("binds client events with useSockudoEvent", async () => {
    const client = new FakeClient() as any;
    const callback = vi.fn();

    const Root = defineComponent({
      setup() {
        useSockudoEvent("sockudo:ready", callback);
        return () => h("div");
      },
    });

    app = createApp(Root);
    app.use(createSockudoPlugin(client));
    container = document.createElement("div");
    app.mount(container);

    client.emit("sockudo:ready", { ok: true });
    await nextTick();

    expect(callback).toHaveBeenCalledWith({ ok: true });
  });
});
