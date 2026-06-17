import { describe, expect, it, vi } from "vitest";
import Dispatcher from "../../src/core/events/dispatcher";
import Members from "../../src/core/channels/members";
import {
  bindChannelState,
  createChannelSnapshot,
  emptyChannelSnapshot,
} from "../../src/framework/shared";
import { prefixedEvent } from "../../src/core/protocol_prefix";

class FakeChannel extends Dispatcher {
  name = "public-chat";
  subscribed = false;
  subscriptionPending = true;
  subscriptionCount: number | null = null;
}

class FakePresenceChannel extends FakeChannel {
  members = new Members();
}

describe("framework shared helpers", () => {
  it("creates an empty channel snapshot", () => {
    expect(emptyChannelSnapshot()).toEqual({
      channel: null,
      subscribed: false,
      subscriptionPending: false,
      subscriptionCount: null,
      members: null,
      membersCount: null,
      me: null,
      lastEventName: null,
      lastEventData: undefined,
    });
  });

  it("captures presence member state in snapshots", () => {
    const channel = new FakePresenceChannel();
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

    const snapshot = createChannelSnapshot(channel as any, "custom:event", {
      ok: true,
    });

    expect(snapshot.subscribed).toBe(true);
    expect(snapshot.membersCount).toBe(2);
    expect(snapshot.me).toEqual({ id: "u-1", info: { name: "Ada" } });
    expect(snapshot.members).toEqual([
      { id: "u-1", info: { name: "Ada" } },
      { id: "u-2", info: { name: "Linus" } },
    ]);
    expect(snapshot.lastEventName).toBe("custom:event");
  });

  it("binds channel state callbacks and semantic handlers", () => {
    const channel = new FakePresenceChannel();
    const onStateChange = vi.fn();
    const onEvent = vi.fn();
    const onSubscriptionSucceeded = vi.fn();
    const onSubscriptionError = vi.fn();
    const onMemberAdded = vi.fn();
    const onMemberRemoved = vi.fn();

    const unbind = bindChannelState(channel as any, {
      onStateChange,
      onEvent,
      onSubscriptionSucceeded,
      onSubscriptionError,
      onMemberAdded,
      onMemberRemoved,
    });

    channel.emit(prefixedEvent("subscription_succeeded"), { ready: true });
    channel.emit(prefixedEvent("subscription_error"), { reason: "denied" });
    channel.emit(prefixedEvent("member_added"), { id: "u-2" });
    channel.emit(prefixedEvent("member_removed"), { id: "u-2" });

    expect(onStateChange).toHaveBeenCalled();
    expect(onEvent).toHaveBeenCalledWith(
      prefixedEvent("subscription_succeeded"),
      { ready: true },
    );
    expect(onSubscriptionSucceeded).toHaveBeenCalledWith({ ready: true });
    expect(onSubscriptionError).toHaveBeenCalledWith({ reason: "denied" });
    expect(onMemberAdded).toHaveBeenCalledWith({ id: "u-2" });
    expect(onMemberRemoved).toHaveBeenCalledWith({ id: "u-2" });

    unbind();
  });
});
