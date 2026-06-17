import {
  getCurrentInstance,
  inject,
  onBeforeUnmount,
  onMounted,
  provide,
  readonly,
  ref,
  shallowRef,
  type App,
  type InjectionKey,
  type Ref,
  type ShallowRef,
} from "vue";
import type Sockudo from "../core/sockudo";
import type Channel from "../core/channels/channel";
import type PresenceChannel from "../core/channels/presence_channel";
import {
  bindChannelState,
  createChannelSnapshot,
  emptyChannelSnapshot,
  subscribeToChannel,
  type ChannelHookOptions,
} from "../framework/shared";

const sockudoKey: InjectionKey<Sockudo> = Symbol("sockudo-client");

function registerLifecycle(start: () => void, stop: () => void): void {
  if (getCurrentInstance()) {
    onMounted(start);
    onBeforeUnmount(stop);
  } else {
    start();
  }
}

export function provideSockudo(client: Sockudo): Sockudo {
  provide(sockudoKey, client);
  return client;
}

export function createSockudoPlugin(client: Sockudo) {
  return {
    install(app: App) {
      app.provide(sockudoKey, client);
    },
  };
}

export function useSockudo(explicitClient?: Sockudo): Sockudo {
  const client = explicitClient ?? inject(sockudoKey, null);

  if (!client) {
    throw new Error(
      "Sockudo client not found. Provide it with createSockudoPlugin(), provideSockudo(), or pass client explicitly.",
    );
  }

  return client;
}

export function useSockudoEvent(
  eventName: string,
  callback: (...args: any[]) => void,
  explicitClient?: Sockudo,
): () => void {
  const client = useSockudo(explicitClient);
  const handler = (...args: any[]) => {
    callback(...args);
  };
  const stop = () => {
    client.unbind(eventName, handler);
  };

  registerLifecycle(() => {
    client.bind(eventName, handler);
  }, stop);

  return stop;
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

export function useChannel<TChannel extends Channel = Channel>(
  channelName: string,
  options: ChannelHookOptions = {},
): UseChannelResult<TChannel> {
  const client = useSockudo(options.client);
  const channel = shallowRef<TChannel | null>(null);
  const subscribed = ref(false);
  const subscriptionPending = ref(false);
  const subscriptionCount = ref<number | null>(null);
  const members = ref<any[] | null>(null);
  const membersCount = ref<number | null>(null);
  const me = ref<any>(null);
  const lastEventName = ref<string | null>(null);
  const lastEventData = ref<any>(undefined);
  let unbindChannel: (() => void) | null = null;

  const applySnapshot = (
    snapshot: ReturnType<typeof createChannelSnapshot>,
  ) => {
    channel.value = snapshot.channel as TChannel | null;
    subscribed.value = snapshot.subscribed;
    subscriptionPending.value = snapshot.subscriptionPending;
    subscriptionCount.value = snapshot.subscriptionCount;
    members.value = snapshot.members;
    membersCount.value = snapshot.membersCount;
    me.value = snapshot.me;
    lastEventName.value = snapshot.lastEventName;
    lastEventData.value = snapshot.lastEventData;
  };

  const start = () => {
    if (options.enabled === false || channel.value) {
      return;
    }

    const subscribedChannel = subscribeToChannel<TChannel>(
      client,
      channelName,
      options,
    );

    unbindChannel = bindChannelState(subscribedChannel, {
      onStateChange: (snapshot) => applySnapshot(snapshot),
      onEvent: options.onEvent,
      onSubscriptionSucceeded: options.onSubscriptionSucceeded,
      onSubscriptionError: options.onSubscriptionError,
      onMemberAdded: options.onMemberAdded,
      onMemberRemoved: options.onMemberRemoved,
    });

    applySnapshot(createChannelSnapshot(subscribedChannel));
  };

  const stop = () => {
    unbindChannel?.();
    unbindChannel = null;

    if (channel.value) {
      client.unsubscribe(channelName);
    }

    applySnapshot(emptyChannelSnapshot());
  };

  registerLifecycle(start, stop);

  return {
    client,
    channel: readonly(channel) as Readonly<ShallowRef<TChannel | null>>,
    subscribed: readonly(subscribed),
    subscriptionPending: readonly(subscriptionPending),
    subscriptionCount: readonly(subscriptionCount),
    members: readonly(members) as Readonly<Ref<any[] | null>>,
    membersCount: readonly(membersCount),
    me: readonly(me),
    lastEventName: readonly(lastEventName),
    lastEventData: readonly(lastEventData),
    stop,
  };
}

export function usePresenceChannel(
  channelName: string,
  options: ChannelHookOptions = {},
): UseChannelResult<PresenceChannel> {
  return useChannel<PresenceChannel>(channelName, options);
}
