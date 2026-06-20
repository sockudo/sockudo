export { version } from "../version.js";

import { getContext, onDestroy, setContext } from "svelte";
import { get, readable, writable, type Readable, type Writable } from "svelte/store";
import { ErrorCode, ErrorInfo } from "../errors.js";
import type { InboundMessage } from "../realtime/index.js";
import {
  createClientTransport,
  type BranchSelectionIntent,
  type ClientTransport,
  type ClientTransportOptions,
  type TurnNode,
  type View,
} from "../core/transport/index.js";

/**
 * Svelte transport store options.
 */
export type TransportStoreOptions<TInput, TOutput, TProjection, TMessage> = ClientTransportOptions<
  TInput,
  TOutput,
  TProjection,
  TMessage
> & {
  /** Closes the transport when the current Svelte component is destroyed.
   *
   * @defaultValue `true`.
   */
  closeOnDestroy?: boolean;
};

/**
 * Options for {@link getClientTransport}.
 */
export interface GetClientTransportOptions {
  /** Provider channel name. Defaults to nearest context transport. */
  channelName?: string;
  /** Suppresses lookup and returns empty state. */
  skip?: boolean;
  /** Subscribes to resolved transport errors. */
  onError?(error: ErrorInfo): void;
}

/**
 * Svelte transport state.
 */
export interface ClientTransportState<TInput, TOutput, TProjection, TMessage> {
  /** Resolved transport. */
  transport?: ClientTransport<TInput, TOutput, TProjection, TMessage>;
  /** Provider construction or lookup error. */
  transportError?: ErrorInfo;
}

/**
 * Svelte transport store.
 */
export interface ClientTransportStore<TInput, TOutput, TProjection, TMessage> extends Readable<
  ClientTransportState<TInput, TOutput, TProjection, TMessage>
> {
  /** Channel registry key. */
  readonly channelName?: string;
  /** Closes the transport if it exists. */
  close(): Promise<void>;
}

/**
 * Options for Svelte view stores.
 */
export interface ViewStoreOptions<TInput, TMessage> {
  /** Explicit transport. */
  transport?:
    | ClientTransport<TInput, unknown, unknown, TMessage>
    | Readable<ClientTransportState<TInput, unknown, unknown, TMessage>>;
  /** Explicit view; wins over `transport`. */
  view?: View<TInput, TMessage>;
  /** Auto-load page size once per view instance. */
  limit?: number;
  /** Suppresses lookup and returns stable empty state. */
  skip?: boolean;
}

/**
 * Svelte view state.
 */
export interface ViewState<TMessage> {
  /** Current visible messages. */
  messages: readonly TMessage[];
  /** Current visible turn nodes. */
  nodes: readonly TurnNode<unknown>[];
  /** Whether older messages can be loaded. */
  hasOlder: boolean;
  /** Whether a load operation is active. */
  loading: boolean;
  /** Latest load error. */
  loadError?: ErrorInfo;
}

/**
 * Svelte branch-aware view store.
 */
export interface ViewStore<TMessage> extends Readable<ViewState<TMessage>> {
  /** Loads older messages unless already loading. */
  loadOlder(limit?: number): Promise<void>;
  /** Selects a sibling branch. */
  select(id: string, index: number, intent?: BranchSelectionIntent): void;
  /** Gets selected sibling index. */
  getSelectedIndex(id: string): number;
  /** Gets sibling turn nodes. */
  getSiblings(id: string): readonly TurnNode<unknown>[];
  /** Returns whether siblings exist. */
  hasSiblings(id: string): boolean;
  /** Gets a turn node by turn id or codec message id. */
  getNode(id: string): TurnNode<unknown> | undefined;
  /** Sends a user message. */
  send(message: TMessage): Promise<unknown>;
  /** Requests regeneration. */
  regenerate(target: string, parent: string): Promise<unknown>;
  /** Edits a message. */
  edit(messageId: string, message: TMessage): Promise<unknown>;
  /** Updates a message. */
  update(messageId: string, patch: unknown): Promise<unknown>;
  /** Closes an owned view. */
  close(): void;
}

/**
 * Options for active-turn subscriptions.
 */
export interface ActiveTurnsStoreOptions<TInput, TMessage> {
  /** Explicit transport. Defaults to context transport. */
  transport?:
    | ClientTransport<TInput, unknown, unknown, TMessage>
    | Readable<ClientTransportState<TInput, unknown, unknown, TMessage>>;
}

/**
 * Options for raw Sockudo message subscriptions.
 */
export interface SockudoMessagesStoreOptions<TInput, TMessage> {
  /** Explicit transport. Defaults to context transport. */
  transport?:
    | ClientTransport<TInput, unknown, unknown, TMessage>
    | Readable<ClientTransportState<TInput, unknown, unknown, TMessage>>;
  /** Suppresses subscription and returns a stable empty list. */
  skip?: boolean;
}

interface TransportRegistry {
  defaultChannelName?: string;
  slots: Map<string, ClientTransportStore<unknown, unknown, unknown, unknown>>;
}

const transportContextKey = Symbol("sockudo-ai-transport-svelte");
const stableEmptyMessages: readonly unknown[] = [];
const stableEmptyNodes: readonly TurnNode<unknown>[] = [];
const stableEmptyRawMessages: readonly InboundMessage[] = [];
const stableEmptyActiveTurns = new Map<string, Set<string>>();
const autoLoadedViews = new WeakSet<View<unknown, unknown>>();

/**
 * Creates a Svelte readable store that owns one client transport.
 */
export function createTransportStore<
  TInput = unknown,
  TOutput = unknown,
  TProjection = unknown,
  TMessage = unknown,
>(
  options: TransportStoreOptions<TInput, TOutput, TProjection, TMessage>,
): ClientTransportStore<TInput, TOutput, TProjection, TMessage> {
  const state = writable<ClientTransportState<TInput, TOutput, TProjection, TMessage>>({});
  let transport: ClientTransport<TInput, TOutput, TProjection, TMessage> | undefined;
  try {
    transport = createClientTransport(options);
    state.set({ transport });
  } catch (error) {
    state.set({ transportError: toConstructionError(error) });
  }
  const store: ClientTransportStore<TInput, TOutput, TProjection, TMessage> = {
    subscribe: state.subscribe,
    close() {
      return transport?.close() ?? Promise.resolve();
    },
    ...(options.channelName !== undefined ? { channelName: options.channelName } : {}),
  };
  if (options.closeOnDestroy !== false) {
    safeOnDestroy(() => {
      void store.close();
    });
  }
  return store;
}

/**
 * Sets the Svelte transport context for child components.
 */
export function setTransportContext<TInput, TOutput, TProjection, TMessage>(
  store: ClientTransportStore<TInput, TOutput, TProjection, TMessage>,
): ClientTransportStore<TInput, TOutput, TProjection, TMessage> {
  const registry = readRegistry();
  const slots = new Map(registry?.slots);
  if (store.channelName !== undefined) {
    slots.set(store.channelName, store);
  }
  const defaultChannelName = store.channelName ?? registry?.defaultChannelName;
  setContext(
    transportContextKey,
    defaultChannelName === undefined
      ? ({ slots } satisfies TransportRegistry)
      : ({ slots, defaultChannelName } satisfies TransportRegistry),
  );
  return store;
}

/**
 * Creates, stores, and provides a Svelte transport in one call.
 */
export function provideTransport<
  TInput = unknown,
  TOutput = unknown,
  TProjection = unknown,
  TMessage = unknown,
>(
  options: TransportStoreOptions<TInput, TOutput, TProjection, TMessage>,
): ClientTransportStore<TInput, TOutput, TProjection, TMessage> {
  return setTransportContext(createTransportStore(options));
}

/**
 * Reads the nearest or named Svelte client transport store.
 */
export function getClientTransport<
  TInput = unknown,
  TOutput = unknown,
  TProjection = unknown,
  TMessage = unknown,
>(
  options: GetClientTransportOptions = {},
): Readable<ClientTransportState<TInput, TOutput, TProjection, TMessage>> {
  if (options.skip === true) {
    return readable({});
  }
  const registry = readRegistry();
  const key = options.channelName ?? registry?.defaultChannelName;
  const store = key === undefined ? undefined : registry?.slots.get(key);
  if (!store) {
    return readable({
      transportError: missingProviderError(options.channelName),
    });
  }
  const typed = store as ClientTransportStore<TInput, TOutput, TProjection, TMessage>;
  if (options.onError) {
    const state = get(typed);
    const unsubscribe = state.transport?.on("error", (error) => {
      options.onError?.(error);
    });
    if (unsubscribe) {
      safeOnDestroy(unsubscribe);
    }
  }
  return typed;
}

/**
 * Creates a branch-aware Svelte view store.
 */
export function createViewStore<TInput = unknown, TMessage = unknown>(
  options: ViewStoreOptions<TInput, TMessage> = {},
): ViewStore<TMessage> {
  const transport = resolveTransport(options.transport);
  const view = options.skip === true ? undefined : (options.view ?? transport?.view);
  return createViewStoreFromView(view, options.limit, false);
}

/**
 * Creates and owns an additional branch-aware Svelte view store.
 */
export function createOwnedViewStore<TInput = unknown, TMessage = unknown>(
  options: Omit<ViewStoreOptions<TInput, TMessage>, "view"> = {},
): ViewStore<TMessage> {
  const transport = resolveTransport(options.transport);
  const view = options.skip === true ? undefined : transport?.createView();
  return createViewStoreFromView(view, options.limit, true);
}

/**
 * Creates stable tree callbacks for a Svelte transport.
 */
export function createTreeHandle<TInput = unknown, TMessage = unknown>(
  options: ActiveTurnsStoreOptions<TInput, TMessage> = {},
): {
  /** Gets sibling turn nodes without subscribing to tree changes. */
  getSiblings(id: string): readonly TurnNode<unknown>[];
  /** Returns whether siblings exist without subscribing to tree changes. */
  hasSiblings(id: string): boolean;
  /** Gets a turn node without subscribing to tree changes. */
  getNode(id: string): TurnNode<unknown> | undefined;
} {
  const transport = resolveTransport(options.transport);
  return {
    getSiblings(id) {
      return requireTransport(transport).tree.getSiblings(id);
    },
    hasSiblings(id) {
      return requireTransport(transport).tree.hasSiblings(id);
    },
    getNode(id) {
      return requireTransport(transport).tree.getNode(id);
    },
  };
}

/**
 * Subscribes to active/suspended turn ownership.
 */
export function createActiveTurnsStore<TInput = unknown, TMessage = unknown>(
  options: ActiveTurnsStoreOptions<TInput, TMessage> = {},
): Readable<Map<string, Set<string>>> {
  const transport = resolveTransport(options.transport);
  return readable(cloneActiveTurns(transport), (set) => {
    if (!transport) {
      set(stableEmptyActiveTurns);
      return undefined;
    }
    set(cloneActiveTurns(transport));
    return transport.tree.on("turn", () => {
      set(cloneActiveTurns(transport));
    });
  });
}

/**
 * Subscribes to the raw normalized Sockudo inbound message firehose.
 */
export function createSockudoMessagesStore<TInput = unknown, TMessage = unknown>(
  options: SockudoMessagesStoreOptions<TInput, TMessage> = {},
): Readable<readonly InboundMessage[]> {
  const transport = options.skip === true ? undefined : resolveTransport(options.transport);
  return readable(stableEmptyRawMessages, (set) => {
    if (!transport) {
      set(stableEmptyRawMessages);
      return undefined;
    }
    let messages = stableEmptyRawMessages;
    return transport.on("message", (message) => {
      messages = messages === stableEmptyRawMessages ? [message] : [...messages, message];
      set(messages);
    });
  });
}

function createViewStoreFromView<TInput, TMessage>(
  view: View<TInput, TMessage> | undefined,
  limit: number | undefined,
  owned: boolean,
): ViewStore<TMessage> {
  const state: Writable<ViewState<TMessage>> = writable(viewSnapshot(view));
  const unsubscribe = view?.on("update", () => {
    state.set(viewSnapshot(view));
  });
  if (view && limit !== undefined && !autoLoadedViews.has(view)) {
    autoLoadedViews.add(view);
    if (view.hasOlder() && !view.loading) {
      void view.loadOlder(limit);
    }
  }
  const store: ViewStore<TMessage> = {
    subscribe: state.subscribe,
    async loadOlder(requestedLimit?: number): Promise<void> {
      if (!view || view.loading) {
        return;
      }
      await view.loadOlder(requestedLimit);
      state.set(viewSnapshot(view));
    },
    select(id, index, intent) {
      view?.select(id, index, intent);
      state.set(viewSnapshot(view));
    },
    getSelectedIndex(id) {
      return view?.getSelectedIndex(id) ?? 0;
    },
    getSiblings(id) {
      return view?.getSiblings(id) ?? stableEmptyNodes;
    },
    hasSiblings(id) {
      return view?.hasSiblings(id) ?? false;
    },
    getNode(id) {
      return view?.getNode(id);
    },
    send(message) {
      return requireView(view).send(message);
    },
    regenerate(target, parent) {
      return requireView(view).regenerate(target, parent);
    },
    edit(messageId, message) {
      return requireView(view).edit(messageId, message);
    },
    update(messageId, patch) {
      return requireView(view).update(messageId, patch);
    },
    close() {
      unsubscribe?.();
      if (owned) {
        view?.close();
      }
    },
  };
  safeOnDestroy(() => {
    store.close();
  });
  return store;
}

function resolveTransport<TInput, TMessage>(
  source:
    | ClientTransport<TInput, unknown, unknown, TMessage>
    | Readable<ClientTransportState<TInput, unknown, unknown, TMessage>>
    | undefined,
): ClientTransport<TInput, unknown, unknown, TMessage> | undefined {
  if (source !== undefined) {
    return isReadable(source) ? get(source).transport : source;
  }
  return get(getClientTransport<TInput, unknown, unknown, TMessage>()).transport;
}

function readRegistry(): TransportRegistry | undefined {
  try {
    return getContext<TransportRegistry | undefined>(transportContextKey);
  } catch {
    return undefined;
  }
}

function viewSnapshot<TInput, TMessage>(
  view: View<TInput, TMessage> | undefined,
): ViewState<TMessage> {
  if (!view) {
    return {
      messages: stableEmptyMessages as readonly TMessage[],
      nodes: stableEmptyNodes,
      hasOlder: false,
      loading: false,
    };
  }
  return {
    messages: view.getMessages(),
    nodes: view.flattenNodes(),
    hasOlder: view.hasOlder(),
    loading: view.loading,
    ...(view.loadError !== undefined ? { loadError: view.loadError } : {}),
  };
}

function cloneActiveTurns(
  transport: ClientTransport<unknown, unknown, unknown, unknown> | undefined,
): Map<string, Set<string>> {
  if (!transport) {
    return stableEmptyActiveTurns;
  }
  const clone = new Map<string, Set<string>>();
  for (const [clientId, turns] of transport.tree.getActiveTurnIds()) {
    clone.set(clientId, new Set(turns));
  }
  return clone;
}

function missingProviderError(channelName: string | undefined): ErrorInfo {
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message:
      channelName === undefined
        ? "unable to resolve client transport; no Svelte transport context is available"
        : `unable to resolve client transport; no Svelte transport context for channel ${channelName}`,
  });
}

function toConstructionError(error: unknown): ErrorInfo {
  if (error instanceof ErrorInfo) {
    return error;
  }
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message: "unable to create client transport in Svelte transport store",
    cause: error,
  });
}

function requireView<TInput, TMessage>(
  view: View<TInput, TMessage> | undefined,
): View<TInput, TMessage> {
  if (!view) {
    throw missingProviderError(undefined);
  }
  return view;
}

function requireTransport(
  transport: ClientTransport<unknown, unknown, unknown, unknown> | undefined,
): ClientTransport<unknown, unknown, unknown, unknown> {
  if (!transport) {
    throw missingProviderError(undefined);
  }
  return transport;
}

function isReadable<T>(value: unknown): value is Readable<T> {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { subscribe?: unknown }).subscribe === "function"
  );
}

function safeOnDestroy(cleanup: () => void): void {
  try {
    onDestroy(cleanup);
  } catch {
    // Store factories can be used outside component initialization in tests and
    // plain modules. In that case callers own cleanup through `close()`.
  }
}
