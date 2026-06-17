import type Sockudo from "../types/core/sockudo";
import type Channel from "../types/core/channels/channel";
import type PresenceChannel from "../types/core/channels/presence_channel";
import type { FilterNode } from "../types/core/channels/filter";
import type { ReactNode } from "react";

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

export interface ChannelDeltaSettings {
  enabled?: boolean;
  algorithm?: "fossil" | "xdelta3";
}

export interface SockudoProviderProps {
  client: Sockudo;
  children?: ReactNode;
}

export interface UseChannelResult<
  TChannel extends Channel = Channel,
> extends ChannelStateSnapshot<TChannel> {
  client: Sockudo;
}

export declare function SockudoProvider(props: SockudoProviderProps): any;
export declare function useSockudo(explicitClient?: Sockudo): Sockudo;
export declare function useSockudoEvent(
  eventName: string,
  callback: (...args: any[]) => void,
  client?: Sockudo,
): Sockudo;
export declare function useChannel<TChannel extends Channel = Channel>(
  channelName: string,
  options?: ChannelHookOptions,
): UseChannelResult<TChannel>;
export declare function usePresenceChannel(
  channelName: string,
  options?: ChannelHookOptions,
): UseChannelResult<PresenceChannel>;
