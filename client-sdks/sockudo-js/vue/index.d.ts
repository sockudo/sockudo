import type Sockudo from "../types/core/sockudo";
import type Channel from "../types/core/channels/channel";
import type PresenceChannel from "../types/core/channels/presence_channel";
import type { FilterNode } from "../types/core/channels/filter";
import type { App, Ref, ShallowRef } from "vue";

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

export interface ChannelDeltaSettings {
  enabled?: boolean;
  algorithm?: "fossil" | "xdelta3";
}

export interface UseChannelResult<TChannel extends Channel = Channel> {
  client: Sockudo;
  channel: Readonly<ShallowRef<TChannel | null>>;
  subscribed: Readonly<Ref<boolean>>;
  subscriptionPending: Readonly<Ref<boolean>>;
  subscriptionCount: Readonly<Ref<number | null>>;
  members: Readonly<Ref<any[] | null>>;
  membersCount: Readonly<Ref<number | null>>;
  me: Readonly<Ref<any>>;
  lastEventName: Readonly<Ref<string | null>>;
  lastEventData: Readonly<Ref<any>>;
  stop: () => void;
}

export declare function provideSockudo(client: Sockudo): Sockudo;
export declare function createSockudoPlugin(client: Sockudo): {
  install(app: App): void;
};
export declare function useSockudo(explicitClient?: Sockudo): Sockudo;
export declare function useSockudoEvent(
  eventName: string,
  callback: (...args: any[]) => void,
  explicitClient?: Sockudo,
): () => void;
export declare function useChannel<TChannel extends Channel = Channel>(
  channelName: string,
  options?: ChannelHookOptions,
): UseChannelResult<TChannel>;
export declare function usePresenceChannel(
  channelName: string,
  options?: ChannelHookOptions,
): UseChannelResult<PresenceChannel>;
