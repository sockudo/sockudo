export { version } from "../version.js";

import {
  computed,
  inject,
  onScopeDispose,
  provide,
  ref,
  shallowRef,
  type ComputedRef,
  type InjectionKey,
  type Ref,
  type ShallowRef,
} from "vue";
import { ErrorCode, ErrorInfo } from "../errors.js";
import type { ClientLike, InboundMessage } from "../realtime/index.js";
import { useSockudoRealtimeClient } from "../realtime/vue.js";
import {
  createClientSession,
  type BranchSelectionIntent,
  type ClientSession,
  type ClientSessionOptions,
  type RunNode,
  type View,
} from "../core/transport/index.js";

/**
 * Provider options for the generic AI Transport Vue layer.
 */
export type ClientSessionProviderOptions<TInput, TOutput, TProjection, TMessage> = Omit<
  ClientSessionOptions<TInput, TOutput, TProjection, TMessage>,
  "client" | "channel" | "channelName"
> & {
  /** Registry key and Sockudo channel name. */
  channelName: string;
  /** Explicit realtime client. Defaults to `@sockudo/client/vue` context. */
  client?: ClientLike;
  /** Closes the transport when the current Vue effect scope is disposed.
   *
   * @defaultValue `true`.
   */
  closeOnScopeDispose?: boolean;
};

/**
 * Options for {@link useClientSession}.
 */
export interface UseClientSessionOptions {
  /** Provider channel name. Defaults to nearest provided transport. */
  channelName?: string;
  /** Suppresses lookup and returns empty refs. */
  skip?: boolean;
  /** Subscribes to resolved transport errors. */
  onError?(error: ErrorInfo): void;
}

/**
 * Result returned by {@link useClientSession}.
 */
export interface UseClientSessionResult<TInput, TOutput, TProjection, TMessage> {
  /** Resolved transport ref. */
  session: ShallowRef<ClientSession<TInput, TOutput, TProjection, TMessage> | undefined>;
  /** Provider construction or lookup error ref. */
  sessionError: ShallowRef<ErrorInfo | undefined>;
}

/**
 * Options for Vue view composables.
 */
export interface UseViewOptions<TInput, TMessage> {
  /** Explicit transport. */
  session?: ClientSession<TInput, unknown, unknown, TMessage>;
  /** Explicit view; wins over `transport`. */
  view?: View<TInput, TMessage>;
  /** Auto-load page size once per view instance. */
  limit?: number;
  /** Suppresses lookup and returns stable empty refs. */
  skip?: boolean;
}

/**
 * Vue branch-aware view handle.
 */
export interface ViewHandle<TMessage> {
  /** Current visible messages. */
  messages: Ref<readonly TMessage[]>;
  /** Current visible turn nodes. */
  nodes: Ref<readonly RunNode<unknown>[]>;
  /** Whether older messages can be loaded. */
  hasOlder: Ref<boolean>;
  /** Whether a load operation is active. */
  loading: Ref<boolean>;
  /** Latest load error. */
  loadError: Ref<ErrorInfo | undefined>;
  /** Loads older messages unless already loading. */
  loadOlder(limit?: number): Promise<void>;
  /** Selects a sibling branch. */
  select(id: string, index: number, intent?: BranchSelectionIntent): void;
  /** Gets selected sibling index. */
  getSelectedIndex(id: string): number;
  /** Gets sibling turn nodes. */
  getSiblings(id: string): readonly RunNode<unknown>[];
  /** Returns whether siblings exist. */
  hasSiblings(id: string): boolean;
  /** Gets a turn node by turn id or codec message id. */
  getNode(id: string): RunNode<unknown> | undefined;
  /** Sends a user message. */
  send(message: TMessage): Promise<unknown>;
  /** Requests regeneration. */
  regenerate(target: string, parent: string): Promise<unknown>;
  /** Edits a message. */
  edit(messageId: string, message: TMessage): Promise<unknown>;
  /** Updates a message. */
  update(messageId: string, patch: unknown): Promise<unknown>;
}

/**
 * Options for {@link useCreateView}.
 */
export interface UseCreateViewOptions<TInput, TMessage> {
  /** Explicit transport. Defaults to context transport. */
  session?: ClientSession<TInput, unknown, unknown, TMessage>;
  /** Auto-load page size once per owned view instance. */
  limit?: number;
  /** Suppresses view creation. */
  skip?: boolean;
}

/**
 * Options for tree composables.
 */
export interface UseTreeOptions<TInput, TMessage> {
  /** Explicit transport. Defaults to context transport. */
  session?: ClientSession<TInput, unknown, unknown, TMessage>;
}

/**
 * Stable tree helper handle.
 */
export interface TreeHandle {
  /** Gets sibling turn nodes without subscribing to tree changes. */
  getSiblings(id: string): readonly RunNode<unknown>[];
  /** Returns whether siblings exist without subscribing to tree changes. */
  hasSiblings(id: string): boolean;
  /** Gets a turn node without subscribing to tree changes. */
  getNode(id: string): RunNode<unknown> | undefined;
}

/**
 * Options for active-turn subscriptions.
 */
export interface UseActiveRunsOptions<TInput, TMessage> {
  /** Explicit transport. Defaults to context transport. */
  session?: ClientSession<TInput, unknown, unknown, TMessage>;
}

/**
 * Options for raw Sockudo message subscriptions.
 */
export interface UseSockudoMessagesOptions<TInput, TMessage> {
  /** Explicit transport. Defaults to context transport. */
  session?: ClientSession<TInput, unknown, unknown, TMessage>;
  /** Suppresses subscription and returns a stable empty list. */
  skip?: boolean;
}

/**
 * Vue session scope returned by {@link createSessionScope}.
 */
export interface SessionScope<TInput, TOutput, TProjection, TMessage> {
  /** Provides a channel-keyed transport registry. */
  provideSession(
    options: ClientSessionProviderOptions<TInput, TOutput, TProjection, TMessage>,
  ): UseClientSessionResult<TInput, TOutput, TProjection, TMessage>;
  /** Reads the nearest or named client transport. */
  useClientSession(
    options?: UseClientSessionOptions,
  ): UseClientSessionResult<TInput, TOutput, TProjection, TMessage>;
  /** Subscribes to a branch-aware view. */
  useView(options?: UseViewOptions<TInput, TMessage>): ViewHandle<TMessage>;
  /** Creates, owns, and subscribes to an additional view. */
  useCreateView(options?: UseCreateViewOptions<TInput, TMessage>): ViewHandle<TMessage>;
  /** Returns stable tree callbacks without re-rendering on tree changes. */
  useTree(options?: UseTreeOptions<TInput, TMessage>): TreeHandle;
  /** Subscribes to active/suspended turn ownership. */
  useActiveRuns(
    options?: UseActiveRunsOptions<TInput, TMessage>,
  ): ComputedRef<Map<string, Set<string>>>;
  /** Subscribes to raw normalized inbound messages. */
  useSockudoMessages(
    options?: UseSockudoMessagesOptions<TInput, TMessage>,
  ): Ref<readonly InboundMessage[]>;
}

interface SessionSlot {
  session: ShallowRef<ClientSession<unknown, unknown, unknown, unknown> | undefined>;
  sessionError: ShallowRef<ErrorInfo | undefined>;
}

interface SessionRegistry {
  defaultChannelName?: string;
  slots: Map<string, SessionSlot>;
}

const defaultSessionKey: InjectionKey<SessionRegistry> = Symbol("sockudo-ai-transport");
const stableEmptyMessages: readonly unknown[] = [];
const stableEmptyNodes: readonly RunNode<unknown>[] = [];
const stableEmptyRawMessages: readonly InboundMessage[] = [];
const stableEmptyActiveRuns = new Map<string, Set<string>>();
const autoLoadedViews = new WeakSet<View<unknown, unknown>>();

/**
 * Creates generic Vue composables for a specific codec/message type family.
 */
export function createSessionScope<
  TInput = unknown,
  TOutput = unknown,
  TProjection = unknown,
  TMessage = unknown,
>(
  key: InjectionKey<SessionRegistry> = Symbol("sockudo-ai-transport-scope"),
): SessionScope<TInput, TOutput, TProjection, TMessage> {
  function provideSession(
    options: ClientSessionProviderOptions<TInput, TOutput, TProjection, TMessage>,
  ): UseClientSessionResult<TInput, TOutput, TProjection, TMessage> {
    const parent = inject(key, undefined);
    const session = shallowRef<ClientSession<TInput, TOutput, TProjection, TMessage> | undefined>();
    const sessionError = shallowRef<ErrorInfo | undefined>();
    try {
      const client = useSockudoRealtimeClient(options.client);
      const { client: _client, closeOnScopeDispose: _close, ...rest } = options;
      session.value = createClientSession({
        ...rest,
        client,
        channelName: options.channelName,
      });
    } catch (error) {
      sessionError.value = toConstructionError(error);
    }
    const slot: SessionSlot = {
      session,
      sessionError,
    };
    const slots = new Map(parent?.slots);
    slots.set(options.channelName, slot);
    const defaultChannelName = options.channelName || parent?.defaultChannelName;
    provide(key, defaultChannelName === undefined ? { slots } : { slots, defaultChannelName });
    if (options.closeOnScopeDispose !== false) {
      onScopeDispose(() => {
        void session.value?.close();
      });
    }
    return { session, sessionError };
  }

  function useClientSession(
    options: UseClientSessionOptions = {},
  ): UseClientSessionResult<TInput, TOutput, TProjection, TMessage> {
    const registry = inject(key, undefined);
    const missing = shallowRef<ErrorInfo | undefined>(
      options.skip === true ? undefined : missingProviderError(options.channelName),
    );
    if (options.skip === true) {
      return {
        session: shallowRef(undefined),
        sessionError: shallowRef(undefined),
      };
    }
    const slot = resolveSlot(registry, options.channelName);
    if (!slot) {
      return {
        session: shallowRef(undefined),
        sessionError: missing,
      };
    }
    const result = {
      session: slot.session as ShallowRef<
        ClientSession<TInput, TOutput, TProjection, TMessage> | undefined
      >,
      sessionError: slot.sessionError,
    };
    const active = result.session.value;
    if (active && options.onError) {
      const unsubscribe = active.on("error", (error) => {
        options.onError?.(error);
      });
      onScopeDispose(unsubscribe);
    }
    return result;
  }

  function useView(options: UseViewOptions<TInput, TMessage> = {}): ViewHandle<TMessage> {
    const context = useClientSession({ skip: options.skip === true });
    const sourceView =
      options.skip === true
        ? undefined
        : (options.view ?? options.session?.view ?? context.session.value?.view);
    return createViewHandle(sourceView, options.limit);
  }

  function useCreateView(
    options: UseCreateViewOptions<TInput, TMessage> = {},
  ): ViewHandle<TMessage> {
    const context = useClientSession({ skip: options.skip === true });
    const session = options.skip === true ? undefined : (options.session ?? context.session.value);
    const view = session?.createView();
    if (view) {
      onScopeDispose(() => {
        view.close();
      });
    }
    return createViewHandle(view, options.limit);
  }

  function useTree(options: UseTreeOptions<TInput, TMessage> = {}): TreeHandle {
    const context = useClientSession({
      skip: options.session !== undefined,
    });
    const session = options.session ?? context.session.value;
    return {
      getSiblings(id) {
        return requireSession(session).tree.getSiblings(id);
      },
      hasSiblings(id) {
        return requireSession(session).tree.hasSiblings(id);
      },
      getNode(id) {
        return requireSession(session).tree.getNode(id);
      },
    };
  }

  function useActiveRuns(
    options: UseActiveRunsOptions<TInput, TMessage> = {},
  ): ComputedRef<Map<string, Set<string>>> {
    const context = useClientSession({
      skip: options.session !== undefined,
    });
    const session = options.session ?? context.session.value;
    const tick = ref(0);
    if (session) {
      const unsubscribe = session.tree.on("turn", () => {
        tick.value += 1;
      });
      onScopeDispose(unsubscribe);
    }
    return computed(() => {
      // Reading the tick is what makes this computed re-run when runs change.
      // eslint-disable-next-line @typescript-eslint/no-meaningless-void-operator
      void tick.value;
      return cloneActiveRuns(session);
    });
  }

  function useSockudoMessages(
    options: UseSockudoMessagesOptions<TInput, TMessage> = {},
  ): Ref<readonly InboundMessage[]> {
    const context = useClientSession({
      skip: options.skip === true || options.session !== undefined,
    });
    const session = options.skip === true ? undefined : (options.session ?? context.session.value);
    const messages = ref<readonly InboundMessage[]>(stableEmptyRawMessages);
    if (session) {
      const unsubscribe = session.on("message", (message) => {
        messages.value =
          messages.value === stableEmptyRawMessages ? [message] : [...messages.value, message];
      });
      onScopeDispose(unsubscribe);
    }
    return messages;
  }

  return {
    provideSession,
    useClientSession,
    useView,
    useCreateView,
    useTree,
    useActiveRuns,
    useSockudoMessages,
  };
}

const defaultScope = createSessionScope(defaultSessionKey);

/**
 * Provides a channel-keyed AI client session using `@sockudo/client/vue`.
 */
export function provideSession(
  options: ClientSessionProviderOptions<unknown, unknown, unknown, unknown>,
): UseClientSessionResult<unknown, unknown, unknown, unknown> {
  return defaultScope.provideSession(options);
}

/**
 * Reads the nearest or named Vue client session.
 */
export function useClientSession(
  options?: UseClientSessionOptions,
): UseClientSessionResult<unknown, unknown, unknown, unknown> {
  return defaultScope.useClientSession(options);
}

/**
 * Subscribes to a branch-aware view and returns reactive refs.
 */
export function useView(options?: UseViewOptions<unknown, unknown>): ViewHandle<unknown> {
  return defaultScope.useView(options);
}

/**
 * Creates and owns an additional view for the resolved transport.
 */
export function useCreateView(
  options?: UseCreateViewOptions<unknown, unknown>,
): ViewHandle<unknown> {
  return defaultScope.useCreateView(options);
}

/**
 * Returns stable tree callbacks without subscribing to tree updates.
 */
export function useTree(options?: UseTreeOptions<unknown, unknown>): TreeHandle {
  return defaultScope.useTree(options);
}

/**
 * Subscribes to active/suspended turn ownership.
 */
export function useActiveRuns(
  options?: UseActiveRunsOptions<unknown, unknown>,
): ComputedRef<Map<string, Set<string>>> {
  return defaultScope.useActiveRuns(options);
}

/**
 * Subscribes to the raw normalized Sockudo inbound message firehose.
 */
export function useSockudoMessages(
  options?: UseSockudoMessagesOptions<unknown, unknown>,
): Ref<readonly InboundMessage[]> {
  return defaultScope.useSockudoMessages(options);
}

function createViewHandle<TInput, TMessage>(
  view: View<TInput, TMessage> | undefined,
  limit: number | undefined,
): ViewHandle<TMessage> {
  const snapshot = shallowRef(viewSnapshot(view));
  if (view) {
    const unsubscribe = view.on("update", () => {
      snapshot.value = viewSnapshot(view);
    });
    onScopeDispose(unsubscribe);
    if (limit !== undefined && !autoLoadedViews.has(view)) {
      autoLoadedViews.add(view);
      if (view.hasOlder() && !view.loading) {
        void view.loadOlder(limit);
      }
    }
  }
  return {
    messages: computed(() => snapshot.value.messages),
    nodes: computed(() => snapshot.value.nodes),
    hasOlder: computed(() => snapshot.value.hasOlder),
    loading: computed(() => snapshot.value.loading),
    loadError: computed(() => snapshot.value.loadError),
    async loadOlder(requestedLimit?: number): Promise<void> {
      if (!view || view.loading) {
        return;
      }
      await view.loadOlder(requestedLimit);
      snapshot.value = viewSnapshot(view);
    },
    select(id, index, intent) {
      view?.select(id, index, intent);
      snapshot.value = viewSnapshot(view);
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
  };
}

function viewSnapshot<TInput, TMessage>(
  view: View<TInput, TMessage> | undefined,
): {
  messages: readonly TMessage[];
  nodes: readonly RunNode<unknown>[];
  hasOlder: boolean;
  loading: boolean;
  loadError?: ErrorInfo;
} {
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

function resolveSlot(
  registry: SessionRegistry | undefined,
  channelName: string | undefined,
): SessionSlot | undefined {
  const key = channelName ?? registry?.defaultChannelName;
  return key === undefined ? undefined : registry?.slots.get(key);
}

function cloneActiveRuns(
  session: ClientSession<unknown, unknown, unknown, unknown> | undefined,
): Map<string, Set<string>> {
  if (!session) {
    return stableEmptyActiveRuns;
  }
  const clone = new Map<string, Set<string>>();
  for (const [clientId, turns] of session.tree.getActiveRunIds()) {
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
        ? "unable to resolve client transport; no Vue transport provider is available"
        : `unable to resolve client transport; no Vue transport provider for channel ${channelName}`,
  });
}

function toConstructionError(error: unknown): ErrorInfo {
  if (error instanceof ErrorInfo) {
    return error;
  }
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message: "unable to create client transport in Vue transport provider",
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

function requireSession(
  session: ClientSession<unknown, unknown, unknown, unknown> | undefined,
): ClientSession<unknown, unknown, unknown, unknown> {
  if (!session) {
    throw missingProviderError(undefined);
  }
  return session;
}
