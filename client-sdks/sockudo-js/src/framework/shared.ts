import type Sockudo from "../core/sockudo";
import type Channel from "../core/channels/channel";
import type { ChannelDeltaSettings } from "../core/channels/channel";
import type PresenceChannel from "../core/channels/presence_channel";
import type Members from "../core/channels/members";
import type { FilterNode } from "../core/channels/filter";
import { prefixedEvent } from "../core/protocol_prefix";

export interface ChannelHookOptions {
  client?: Sockudo;
  enabled?: boolean;
  filter?: FilterNode;
  delta?: ChannelDeltaSettings;
  events?: string[];
  onEvent?: (eventName: string, data: any) => void;
  onSubscriptionSucceeded?: (data: any) => void;
  onSubscriptionError?: (error: any) => void;
  onMemberAdded?: (member: any) => void;
  onMemberRemoved?: (member: any) => void;
}

export interface ChannelStateSnapshot<TChannel extends Channel = Channel> {
  channel: TChannel | null;
  subscribed: boolean;
  subscriptionPending: boolean;
  subscriptionCount: number | null;
  members: any[] | null;
  membersCount: number | null;
  me: any;
  lastEventName: string | null;
  lastEventData: any;
}

interface BoundChannelCallbacks<TChannel extends Channel> {
  onStateChange: (snapshot: ChannelStateSnapshot<TChannel>) => void;
  onEvent?: (eventName: string, data: any) => void;
  onSubscriptionSucceeded?: (data: any) => void;
  onSubscriptionError?: (error: any) => void;
  onMemberAdded?: (member: any) => void;
  onMemberRemoved?: (member: any) => void;
}

export function isPresenceChannel(
  channel: Channel | null,
): channel is PresenceChannel {
  return Boolean(channel && "members" in channel);
}

function snapshotMembers(members: Members): any[] {
  const snapshot: any[] = [];
  members.each((member: any) => {
    snapshot.push(member);
  });
  return snapshot;
}

export function emptyChannelSnapshot<
  TChannel extends Channel = Channel,
>(): ChannelStateSnapshot<TChannel> {
  return {
    channel: null,
    subscribed: false,
    subscriptionPending: false,
    subscriptionCount: null,
    members: null,
    membersCount: null,
    me: null,
    lastEventName: null,
    lastEventData: undefined,
  };
}

export function createChannelSnapshot<TChannel extends Channel = Channel>(
  channel: TChannel,
  lastEventName: string | null = null,
  lastEventData: any = undefined,
): ChannelStateSnapshot<TChannel> {
  if (isPresenceChannel(channel)) {
    return {
      channel,
      subscribed: channel.subscribed,
      subscriptionPending: channel.subscriptionPending,
      subscriptionCount: channel.subscriptionCount,
      members: snapshotMembers(channel.members),
      membersCount: channel.members.count,
      me: channel.members.me,
      lastEventName,
      lastEventData,
    };
  }

  return {
    channel,
    subscribed: channel.subscribed,
    subscriptionPending: channel.subscriptionPending,
    subscriptionCount: channel.subscriptionCount,
    members: null,
    membersCount: null,
    me: null,
    lastEventName,
    lastEventData,
  };
}

export function subscribeToChannel<TChannel extends Channel = Channel>(
  client: Sockudo,
  channelName: string,
  options: ChannelHookOptions = {},
): TChannel {
  const subscribeOptions =
    options.filter || options.delta || options.events
      ? {
          filter: options.filter,
          delta: options.delta,
          events: options.events,
        }
      : undefined;

  return client.subscribe(channelName, subscribeOptions) as TChannel;
}

export function bindChannelState<TChannel extends Channel = Channel>(
  channel: TChannel,
  callbacks: BoundChannelCallbacks<TChannel>,
): () => void {
  const globalHandler = (eventName: string, data: any) => {
    callbacks.onEvent?.(eventName, data);
    callbacks.onStateChange(createChannelSnapshot(channel, eventName, data));
  };
  const subscriptionSucceededHandler = (data: any) => {
    callbacks.onSubscriptionSucceeded?.(data);
  };
  const subscriptionErrorHandler = (error: any) => {
    callbacks.onSubscriptionError?.(error);
  };
  const memberAddedHandler = (member: any) => {
    callbacks.onMemberAdded?.(member);
  };
  const memberRemovedHandler = (member: any) => {
    callbacks.onMemberRemoved?.(member);
  };

  channel.bind_global(globalHandler);
  channel.bind(
    prefixedEvent("subscription_succeeded"),
    subscriptionSucceededHandler,
  );
  channel.bind(prefixedEvent("subscription_error"), subscriptionErrorHandler);

  if (isPresenceChannel(channel)) {
    channel.bind(prefixedEvent("member_added"), memberAddedHandler);
    channel.bind(prefixedEvent("member_removed"), memberRemovedHandler);
  }

  callbacks.onStateChange(createChannelSnapshot(channel));

  return () => {
    channel.unbind_global(globalHandler);
    channel.unbind(
      prefixedEvent("subscription_succeeded"),
      subscriptionSucceededHandler,
    );
    channel.unbind(
      prefixedEvent("subscription_error"),
      subscriptionErrorHandler,
    );

    if (isPresenceChannel(channel)) {
      channel.unbind(prefixedEvent("member_added"), memberAddedHandler);
      channel.unbind(prefixedEvent("member_removed"), memberRemovedHandler);
    }
  };
}
