import {
  createContext,
  createElement,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type Sockudo from "../core/sockudo";
import type Channel from "../core/channels/channel";
import type PresenceChannel from "../core/channels/presence_channel";
import {
  bindChannelState,
  createChannelSnapshot,
  emptyChannelSnapshot,
  subscribeToChannel,
  type ChannelHookOptions,
  type ChannelStateSnapshot,
} from "../framework/shared";

const SockudoContext = createContext<Sockudo | null>(null);

export interface SockudoProviderProps {
  client: Sockudo;
  children?: ReactNode;
}

export function SockudoProvider({ client, children }: SockudoProviderProps) {
  return createElement(SockudoContext.Provider, { value: client }, children);
}

export function useSockudo(explicitClient?: Sockudo): Sockudo {
  const contextClient = useContext(SockudoContext);
  const client = explicitClient ?? contextClient;

  if (!client) {
    throw new Error(
      "Sockudo client not found. Wrap your tree in SockudoProvider or pass client explicitly.",
    );
  }

  return client;
}

export function useSockudoEvent(
  eventName: string,
  callback: (...args: any[]) => void,
  client?: Sockudo,
): Sockudo {
  const resolvedClient = useSockudo(client);
  const callbackRef = useRef(callback);

  callbackRef.current = callback;

  useEffect(() => {
    const handler = (...args: any[]) => {
      callbackRef.current(...args);
    };

    resolvedClient.bind(eventName, handler);

    return () => {
      resolvedClient.unbind(eventName, handler);
    };
  }, [resolvedClient, eventName]);

  return resolvedClient;
}

export interface UseChannelResult<TChannel extends Channel = Channel>
  extends ChannelStateSnapshot<TChannel> {
  client: Sockudo;
}

export function useChannel<TChannel extends Channel = Channel>(
  channelName: string,
  options: ChannelHookOptions = {},
): UseChannelResult<TChannel> {
  const client = useSockudo(options.client);
  const [snapshot, setSnapshot] = useState<ChannelStateSnapshot<TChannel>>(
    emptyChannelSnapshot<TChannel>,
  );
  const callbacksRef = useRef(options);
  const subscriptionKey = useMemo(
    () =>
      JSON.stringify({
        filter: options.filter ?? null,
        delta: options.delta ?? null,
        events: options.events ?? null,
      }),
    [options.filter, options.delta, options.events],
  );

  callbacksRef.current = options;

  useEffect(() => {
    if (options.enabled === false) {
      setSnapshot(emptyChannelSnapshot<TChannel>());
      return;
    }

    const channel = subscribeToChannel<TChannel>(client, channelName, options);
    const unbind = bindChannelState(channel, {
      onStateChange: setSnapshot,
      onEvent: (eventName, data) =>
        callbacksRef.current.onEvent?.(eventName, data),
      onSubscriptionSucceeded: (data) =>
        callbacksRef.current.onSubscriptionSucceeded?.(data),
      onSubscriptionError: (error) =>
        callbacksRef.current.onSubscriptionError?.(error),
      onMemberAdded: (member) => callbacksRef.current.onMemberAdded?.(member),
      onMemberRemoved: (member) =>
        callbacksRef.current.onMemberRemoved?.(member),
    });

    setSnapshot(createChannelSnapshot(channel));

    return () => {
      unbind();
      client.unsubscribe(channelName);
      setSnapshot(emptyChannelSnapshot<TChannel>());
    };
  }, [client, channelName, options.enabled, subscriptionKey]);

  return {
    client,
    ...snapshot,
  };
}

export function usePresenceChannel(
  channelName: string,
  options: ChannelHookOptions = {},
): UseChannelResult<PresenceChannel> {
  return useChannel<PresenceChannel>(channelName, options);
}
