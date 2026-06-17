// core
export { version } from "./version.js";
export { EVENT_AI_CANCEL, EVENT_AI_INPUT, EVENT_AI_OUTPUT, EVENT_AI_TURN_END, EVENT_AI_TURN_START, HEADER_CODEC_MESSAGE_ID, HEADER_DISCRETE, HEADER_ERROR_CODE, HEADER_ERROR_MESSAGE, HEADER_EVENT_ID, HEADER_FORK_OF, HEADER_INPUT_CLIENT_ID, HEADER_INVOCATION_ID, HEADER_MSG_REGENERATE, HEADER_PARENT, HEADER_ROLE, HEADER_STATUS, HEADER_STREAM, HEADER_STREAM_ID, HEADER_TURN_CLIENT_ID, HEADER_TURN_CONTINUE, HEADER_TURN_ID, HEADER_TURN_REASON, } from "./constants.js";
export { ErrorCode, ErrorInfo, errorInfoIs, formatErrorMessage, statusCodeForErrorCode, toErrorInfo, type ErrorInfoOptions, } from "./errors.js";
export { EventEmitter, type EventEmitterOptions, type EventUnsubscribe, type EventsMap, } from "./event-emitter.js";
export { LogLevel, consoleLogger, makeLogger, redactValue, type LogContext, type LogHandler, type Logger, type MakeLoggerOptions, } from "./logger.js";
export { buildTransportHeaders, getCodecHeaders, getTransportHeaders, headerReader, headerWriter, mergeHeaders, stripUndefined, type AiExtras, type BuildTransportHeadersOptions, type HeaderMap, } from "./utils.js";
export * from "./realtime/index.js";
export * from "./core/codec/index.js";
export * from "./core/transport/index.js";
//# sourceMappingURL=index.d.ts.map

// react
export { version } from "../version.js";
import { createElement, type ReactNode } from "react";
import { ErrorInfo } from "../errors.js";
import type { InboundMessage } from "../realtime/index.js";
import { type BranchSelectionIntent, type ClientTransport, type ClientTransportOptions, type TurnNode, type View } from "../core/transport/index.js";
/**
 * Provider props for the generic AI Transport React layer.
 *
 * Final provider stack: use `@sockudo/client/react`'s `SockudoProvider` as the
 * outer realtime client owner, then place this provider inside it with the
 * AI channel name and transport options. This package imports the peer only
 * through `src/realtime/react`, where the Sockudo client is adapted into the
 * realtime seam.
 *
 * Construction errors are caught and exposed by {@link useClientTransport} as
 * `transportError`; children still render under the provider registry.
 */
export type TransportProviderProps<TInput, TOutput, TProjection, TMessage> = Omit<ClientTransportOptions<TInput, TOutput, TProjection, TMessage>, "client" | "channel" | "channelName"> & {
    /**
     * Registry key and Sockudo channel name for unnamed hook lookups.
     *
     * @defaultValue No default; a channel name is required.
     */
    channelName: string;
    /** Child React tree. */
    children?: ReactNode;
};
/**
 * Options for {@link useClientTransport}.
 *
 * Lookup failures return a throwing `InvalidArgument` stub and set
 * `transportError`; when `skip` is true, `transportError` is omitted.
 */
export interface UseClientTransportOptions {
    /**
     * Provider channel name.
     *
     * @defaultValue Nearest provider in the React tree.
     */
    channelName?: string;
    /**
     * Suppresses lookup and returns an inert throwing stub with no error.
     *
     * @defaultValue `false`.
     */
    skip?: boolean;
    /**
     * Subscribes to resolved transport errors.
     *
     * @defaultValue No callback.
     */
    onError?(error: ErrorInfo): void;
}
/**
 * Result returned by {@link useClientTransport}.
 *
 * Missing, skipped, and failed providers expose a transport proxy that throws
 * {@link ErrorInfo} with {@link ErrorCode.InvalidArgument} on property access.
 */
export interface UseClientTransportResult<TInput, TOutput, TProjection, TMessage> {
    /** Resolved transport or a throwing stub. */
    transport: ClientTransport<TInput, TOutput, TProjection, TMessage>;
    /**
     * Provider construction or lookup error.
     *
     * @defaultValue `undefined` when a transport resolves or lookup is skipped.
     */
    transportError?: ErrorInfo;
}
/**
 * Options for view hooks.
 *
 * Without a resolved view, methods that require one throw {@link ErrorInfo}
 * with {@link ErrorCode.InvalidArgument}; snapshot fields remain stable
 * empties for SSR and pre-mount renders.
 */
export interface UseViewOptions<TInput, TMessage> {
    /**
     * Explicit transport.
     *
     * @defaultValue Context transport.
     */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
    /**
     * Explicit view; wins over `transport`.
     *
     * @defaultValue Transport default view.
     */
    view?: View<TInput, TMessage>;
    /**
     * Auto-load page size once per view instance.
     *
     * @defaultValue No automatic load.
     */
    limit?: number;
    /**
     * Suppresses lookup and returns a stable empty handle.
     *
     * @defaultValue `false`.
     */
    skip?: boolean;
}
/**
 * Branch-aware view hook handle.
 *
 * Methods that need a live view throw {@link ErrorInfo} with
 * {@link ErrorCode.InvalidArgument} when no view is resolved.
 */
export interface ViewHandle<TMessage> {
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
}
/**
 * Options for {@link useCreateView}.
 *
 * Without a resolved transport the hook returns the same stable empty handle as
 * {@link useView}; methods that need a live view throw `InvalidArgument`.
 */
export interface UseCreateViewOptions<TInput, TMessage> {
    /**
     * Explicit transport.
     *
     * @defaultValue Context transport.
     */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
    /**
     * Auto-load page size once per owned view instance.
     *
     * @defaultValue No automatic load.
     */
    limit?: number;
    /**
     * Suppresses view creation.
     *
     * @defaultValue `false`.
     */
    skip?: boolean;
}
/**
 * Options for tree hooks.
 *
 * Tree callbacks do not subscribe to tree changes and throw `InvalidArgument`
 * only when called without a resolved transport.
 */
export interface UseTreeOptions<TInput, TMessage> {
    /**
     * Explicit transport.
     *
     * @defaultValue Context transport.
     */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
}
/**
 * Stable tree helper handle.
 *
 * This handle intentionally does not re-render on tree changes; use
 * {@link useView} for reactive branch snapshots.
 */
export interface TreeHandle {
    /** Gets sibling turn nodes without subscribing to tree changes. */
    getSiblings(id: string): readonly TurnNode<unknown>[];
    /** Returns whether siblings exist without subscribing to tree changes. */
    hasSiblings(id: string): boolean;
    /** Gets a turn node without subscribing to tree changes. */
    getNode(id: string): TurnNode<unknown> | undefined;
}
/**
 * Options for {@link useActiveTurns}.
 *
 * Without a resolved transport, the hook returns a stable empty map.
 */
export interface UseActiveTurnsOptions<TInput, TMessage> {
    /**
     * Explicit transport.
     *
     * @defaultValue Context transport.
     */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
}
/**
 * Options for {@link useSockudoMessages}.
 *
 * Without a resolved transport or when skipped, the hook returns a stable empty
 * list.
 */
export interface UseSockudoMessagesOptions<TInput, TMessage> {
    /**
     * Explicit transport.
     *
     * @defaultValue Context transport.
     */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
    /**
     * Suppresses subscription and returns a stable empty list.
     *
     * @defaultValue `false`.
     */
    skip?: boolean;
}
/**
 * Generic hook bundle returned by {@link createTransportHooks}.
 *
 * The bundled hooks share the same baked type parameters and error behavior as
 * the default exports.
 */
export interface TransportHooks<TInput, TOutput, TProjection, TMessage> {
    /** Provider for a channel-keyed transport registry. */
    TransportProvider(props: TransportProviderProps<TInput, TOutput, TProjection, TMessage>): ReturnType<typeof createElement>;
    /** Reads the nearest or named client transport. */
    useClientTransport(options?: UseClientTransportOptions): UseClientTransportResult<TInput, TOutput, TProjection, TMessage>;
    /** Subscribes to a branch-aware view. */
    useView(options?: UseViewOptions<TInput, TMessage>): ViewHandle<TMessage>;
    /** Creates, owns, and subscribes to an additional view. */
    useCreateView(options?: UseCreateViewOptions<TInput, TMessage>): ViewHandle<TMessage>;
    /** Returns stable tree callbacks without re-rendering on tree changes. */
    useTree(options?: UseTreeOptions<TInput, TMessage>): TreeHandle;
    /** Subscribes to active/suspended turn ownership. */
    useActiveTurns(options?: UseActiveTurnsOptions<TInput, TMessage>): Map<string, Set<string>>;
    /** Subscribes to raw normalized inbound messages. */
    useSockudoMessages(options?: UseSockudoMessagesOptions<TInput, TMessage>): readonly InboundMessage[];
}
/**
 * Creates generic React hooks for a specific codec/message type family.
 *
 * @defaultValue Type parameters default to `unknown`.
 *
 * Returned hooks throw only via synchronous handle/stub access for invalid
 * usage; async transport/view methods reject with {@link ErrorInfo}.
 */
export declare function createTransportHooks<TInput = unknown, TOutput = unknown, TProjection = unknown, TMessage = unknown>(): TransportHooks<TInput, TOutput, TProjection, TMessage>;
/**
 * Provides a channel-keyed AI client transport using the outer
 * `@sockudo/client/react` `SockudoProvider`.
 *
 * @defaultValue No default `channelName`; the prop is required.
 *
 * Construction failures are caught and exposed through
 * {@link useClientTransport}; children continue to render.
 */
export declare function TransportProvider(props: TransportProviderProps<unknown, unknown, unknown, unknown>): ReturnType<typeof createElement>;
/**
 * Reads the nearest or named AI client transport.
 *
 * @defaultValue Uses the nearest provider when `channelName` is omitted.
 *
 * Missing, skipped, and failed providers return a throwing `InvalidArgument`
 * stub; `transportError` is set except when `skip` is true.
 */
export declare function useClientTransport(options?: UseClientTransportOptions): UseClientTransportResult<unknown, unknown, unknown, unknown>;
/**
 * Subscribes to a branch-aware view and returns a reactive snapshot handle.
 *
 * @defaultValue Uses the context transport's default view.
 *
 * Snapshot fields are stable empties before mount; methods that require a
 * resolved view throw {@link ErrorInfo} with {@link ErrorCode.InvalidArgument}.
 */
export declare function useView(options?: UseViewOptions<unknown, unknown>): ViewHandle<unknown>;
/**
 * Creates and owns an additional view for the resolved transport.
 *
 * @defaultValue Uses the context transport and performs no automatic history
 * load unless `limit` is provided.
 *
 * The owned view is closed on unmount or transport change; unresolved transports
 * return the stable empty view handle.
 */
export declare function useCreateView(options?: UseCreateViewOptions<unknown, unknown>): ViewHandle<unknown>;
/**
 * Returns stable tree callbacks without subscribing to tree updates.
 *
 * @defaultValue Uses the context transport.
 *
 * Callback access throws `InvalidArgument` only when invoked without a resolved
 * transport.
 */
export declare function useTree(options?: UseTreeOptions<unknown, unknown>): TreeHandle;
/**
 * Subscribes to active/suspended turn ownership.
 *
 * @defaultValue Uses the context transport.
 *
 * Returns a new `Map<clientId, Set<turnId>>` reference for each turn event and a
 * stable empty map without a resolved transport.
 */
export declare function useActiveTurns(options?: UseActiveTurnsOptions<unknown, unknown>): Map<string, Set<string>>;
/**
 * Subscribes to the raw normalized Sockudo inbound message firehose.
 *
 * @defaultValue Uses the context transport and does not subscribe when `skip` is
 * true.
 *
 * The returned array is append-only for a transport instance and resets on
 * transport changes.
 */
export declare function useSockudoMessages(options?: UseSockudoMessagesOptions<unknown, unknown>): readonly InboundMessage[];
//# sourceMappingURL=index.d.ts.map

// vue
export { version } from "../version.js";
import { type ComputedRef, type InjectionKey, type Ref, type ShallowRef } from "vue";
import { ErrorInfo } from "../errors.js";
import type { ClientLike, InboundMessage } from "../realtime/index.js";
import { type BranchSelectionIntent, type ClientTransport, type ClientTransportOptions, type TurnNode, type View } from "../core/transport/index.js";
/**
 * Provider options for the generic AI Transport Vue layer.
 */
export type TransportProviderOptions<TInput, TOutput, TProjection, TMessage> = Omit<ClientTransportOptions<TInput, TOutput, TProjection, TMessage>, "client" | "channel" | "channelName"> & {
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
 * Options for {@link useClientTransport}.
 */
export interface UseClientTransportOptions {
    /** Provider channel name. Defaults to nearest provided transport. */
    channelName?: string;
    /** Suppresses lookup and returns empty refs. */
    skip?: boolean;
    /** Subscribes to resolved transport errors. */
    onError?(error: ErrorInfo): void;
}
/**
 * Result returned by {@link useClientTransport}.
 */
export interface UseClientTransportResult<TInput, TOutput, TProjection, TMessage> {
    /** Resolved transport ref. */
    transport: ShallowRef<ClientTransport<TInput, TOutput, TProjection, TMessage> | undefined>;
    /** Provider construction or lookup error ref. */
    transportError: ShallowRef<ErrorInfo | undefined>;
}
/**
 * Options for Vue view composables.
 */
export interface UseViewOptions<TInput, TMessage> {
    /** Explicit transport. */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
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
    nodes: Ref<readonly TurnNode<unknown>[]>;
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
}
/**
 * Options for {@link useCreateView}.
 */
export interface UseCreateViewOptions<TInput, TMessage> {
    /** Explicit transport. Defaults to context transport. */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
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
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
}
/**
 * Stable tree helper handle.
 */
export interface TreeHandle {
    /** Gets sibling turn nodes without subscribing to tree changes. */
    getSiblings(id: string): readonly TurnNode<unknown>[];
    /** Returns whether siblings exist without subscribing to tree changes. */
    hasSiblings(id: string): boolean;
    /** Gets a turn node without subscribing to tree changes. */
    getNode(id: string): TurnNode<unknown> | undefined;
}
/**
 * Options for active-turn subscriptions.
 */
export interface UseActiveTurnsOptions<TInput, TMessage> {
    /** Explicit transport. Defaults to context transport. */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
}
/**
 * Options for raw Sockudo message subscriptions.
 */
export interface UseSockudoMessagesOptions<TInput, TMessage> {
    /** Explicit transport. Defaults to context transport. */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage>;
    /** Suppresses subscription and returns a stable empty list. */
    skip?: boolean;
}
/**
 * Vue transport scope returned by {@link createTransportScope}.
 */
export interface TransportScope<TInput, TOutput, TProjection, TMessage> {
    /** Provides a channel-keyed transport registry. */
    provideTransport(options: TransportProviderOptions<TInput, TOutput, TProjection, TMessage>): UseClientTransportResult<TInput, TOutput, TProjection, TMessage>;
    /** Reads the nearest or named client transport. */
    useClientTransport(options?: UseClientTransportOptions): UseClientTransportResult<TInput, TOutput, TProjection, TMessage>;
    /** Subscribes to a branch-aware view. */
    useView(options?: UseViewOptions<TInput, TMessage>): ViewHandle<TMessage>;
    /** Creates, owns, and subscribes to an additional view. */
    useCreateView(options?: UseCreateViewOptions<TInput, TMessage>): ViewHandle<TMessage>;
    /** Returns stable tree callbacks without re-rendering on tree changes. */
    useTree(options?: UseTreeOptions<TInput, TMessage>): TreeHandle;
    /** Subscribes to active/suspended turn ownership. */
    useActiveTurns(options?: UseActiveTurnsOptions<TInput, TMessage>): ComputedRef<Map<string, Set<string>>>;
    /** Subscribes to raw normalized inbound messages. */
    useSockudoMessages(options?: UseSockudoMessagesOptions<TInput, TMessage>): Ref<readonly InboundMessage[]>;
}
interface TransportSlot {
    transport: ShallowRef<ClientTransport<unknown, unknown, unknown, unknown> | undefined>;
    transportError: ShallowRef<ErrorInfo | undefined>;
}
interface TransportRegistry {
    defaultChannelName?: string;
    slots: Map<string, TransportSlot>;
}
/**
 * Creates generic Vue composables for a specific codec/message type family.
 */
export declare function createTransportScope<TInput = unknown, TOutput = unknown, TProjection = unknown, TMessage = unknown>(key?: InjectionKey<TransportRegistry>): TransportScope<TInput, TOutput, TProjection, TMessage>;
/**
 * Provides a channel-keyed AI client transport using `@sockudo/client/vue`.
 */
export declare function provideTransport(options: TransportProviderOptions<unknown, unknown, unknown, unknown>): UseClientTransportResult<unknown, unknown, unknown, unknown>;
/**
 * Reads the nearest or named Vue client transport.
 */
export declare function useClientTransport(options?: UseClientTransportOptions): UseClientTransportResult<unknown, unknown, unknown, unknown>;
/**
 * Subscribes to a branch-aware view and returns reactive refs.
 */
export declare function useView(options?: UseViewOptions<unknown, unknown>): ViewHandle<unknown>;
/**
 * Creates and owns an additional view for the resolved transport.
 */
export declare function useCreateView(options?: UseCreateViewOptions<unknown, unknown>): ViewHandle<unknown>;
/**
 * Returns stable tree callbacks without subscribing to tree updates.
 */
export declare function useTree(options?: UseTreeOptions<unknown, unknown>): TreeHandle;
/**
 * Subscribes to active/suspended turn ownership.
 */
export declare function useActiveTurns(options?: UseActiveTurnsOptions<unknown, unknown>): ComputedRef<Map<string, Set<string>>>;
/**
 * Subscribes to the raw normalized Sockudo inbound message firehose.
 */
export declare function useSockudoMessages(options?: UseSockudoMessagesOptions<unknown, unknown>): Ref<readonly InboundMessage[]>;
//# sourceMappingURL=index.d.ts.map

// svelte
export { version } from "../version.js";
import { type Readable } from "svelte/store";
import { ErrorInfo } from "../errors.js";
import type { InboundMessage } from "../realtime/index.js";
import { type BranchSelectionIntent, type ClientTransport, type ClientTransportOptions, type TurnNode, type View } from "../core/transport/index.js";
/**
 * Svelte transport store options.
 */
export type TransportStoreOptions<TInput, TOutput, TProjection, TMessage> = ClientTransportOptions<TInput, TOutput, TProjection, TMessage> & {
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
export interface ClientTransportStore<TInput, TOutput, TProjection, TMessage> extends Readable<ClientTransportState<TInput, TOutput, TProjection, TMessage>> {
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
    transport?: ClientTransport<TInput, unknown, unknown, TMessage> | Readable<ClientTransportState<TInput, unknown, unknown, TMessage>>;
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
    transport?: ClientTransport<TInput, unknown, unknown, TMessage> | Readable<ClientTransportState<TInput, unknown, unknown, TMessage>>;
}
/**
 * Options for raw Sockudo message subscriptions.
 */
export interface SockudoMessagesStoreOptions<TInput, TMessage> {
    /** Explicit transport. Defaults to context transport. */
    transport?: ClientTransport<TInput, unknown, unknown, TMessage> | Readable<ClientTransportState<TInput, unknown, unknown, TMessage>>;
    /** Suppresses subscription and returns a stable empty list. */
    skip?: boolean;
}
/**
 * Creates a Svelte readable store that owns one client transport.
 */
export declare function createTransportStore<TInput = unknown, TOutput = unknown, TProjection = unknown, TMessage = unknown>(options: TransportStoreOptions<TInput, TOutput, TProjection, TMessage>): ClientTransportStore<TInput, TOutput, TProjection, TMessage>;
/**
 * Sets the Svelte transport context for child components.
 */
export declare function setTransportContext<TInput, TOutput, TProjection, TMessage>(store: ClientTransportStore<TInput, TOutput, TProjection, TMessage>): ClientTransportStore<TInput, TOutput, TProjection, TMessage>;
/**
 * Creates, stores, and provides a Svelte transport in one call.
 */
export declare function provideTransport<TInput = unknown, TOutput = unknown, TProjection = unknown, TMessage = unknown>(options: TransportStoreOptions<TInput, TOutput, TProjection, TMessage>): ClientTransportStore<TInput, TOutput, TProjection, TMessage>;
/**
 * Reads the nearest or named Svelte client transport store.
 */
export declare function getClientTransport<TInput = unknown, TOutput = unknown, TProjection = unknown, TMessage = unknown>(options?: GetClientTransportOptions): Readable<ClientTransportState<TInput, TOutput, TProjection, TMessage>>;
/**
 * Creates a branch-aware Svelte view store.
 */
export declare function createViewStore<TInput = unknown, TMessage = unknown>(options?: ViewStoreOptions<TInput, TMessage>): ViewStore<TMessage>;
/**
 * Creates and owns an additional branch-aware Svelte view store.
 */
export declare function createOwnedViewStore<TInput = unknown, TMessage = unknown>(options?: Omit<ViewStoreOptions<TInput, TMessage>, "view">): ViewStore<TMessage>;
/**
 * Creates stable tree callbacks for a Svelte transport.
 */
export declare function createTreeHandle<TInput = unknown, TMessage = unknown>(options?: ActiveTurnsStoreOptions<TInput, TMessage>): {
    /** Gets sibling turn nodes without subscribing to tree changes. */
    getSiblings(id: string): readonly TurnNode<unknown>[];
    /** Returns whether siblings exist without subscribing to tree changes. */
    hasSiblings(id: string): boolean;
    /** Gets a turn node without subscribing to tree changes. */
    getNode(id: string): TurnNode<unknown> | undefined;
};
/**
 * Subscribes to active/suspended turn ownership.
 */
export declare function createActiveTurnsStore<TInput = unknown, TMessage = unknown>(options?: ActiveTurnsStoreOptions<TInput, TMessage>): Readable<Map<string, Set<string>>>;
/**
 * Subscribes to the raw normalized Sockudo inbound message firehose.
 */
export declare function createSockudoMessagesStore<TInput = unknown, TMessage = unknown>(options?: SockudoMessagesStoreOptions<TInput, TMessage>): Readable<readonly InboundMessage[]>;
//# sourceMappingURL=index.d.ts.map

// vercel
export { version } from "../version.js";
import { type ClientTransport, type ClientTransportOptions, type ServerTransport, type ServerTransportOptions } from "../core/transport/index.js";
export * from "./codec/index.js";
import { type AI, type VercelInput, type VercelOutput, type VercelProjection } from "./codec/index.js";
/**
 * Client transport options for Vercel UI messages.
 *
 * @defaultValue `api` defaults to `"/api/chat"`.
 */
export type VercelClientTransportOptions = Omit<ClientTransportOptions<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>, "api" | "codec"> & {
    /** Server endpoint URL for the HTTP poke.
     *
     * @defaultValue `"/api/chat"`.
     */
    api?: string;
};
/**
 * Server transport options for Vercel UI messages.
 */
export type VercelServerTransportOptions = Omit<ServerTransportOptions<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>, "codec">;
/**
 * Creates a Sockudo client transport pre-bound to {@link UIMessageCodec}.
 *
 * Async methods reject with `ErrorInfo`; synchronous misuse throws `ErrorInfo`
 * with `InvalidArgument`.
 */
export declare function createClientTransport(options: VercelClientTransportOptions): ClientTransport<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>;
/**
 * Creates a Sockudo server transport pre-bound to {@link UIMessageCodec}.
 *
 * Public methods reject with `ErrorInfo`; synchronous misuse throws
 * `ErrorInfo` with `InvalidArgument`.
 */
export declare function createServerTransport(options: VercelServerTransportOptions): ServerTransport<VercelOutput, VercelProjection, AI.UIMessage>;
export * from "./transport/index.js";
//# sourceMappingURL=index.d.ts.map

// vercel/react
export { version } from "../../version.js";
import { createElement, type ReactNode } from "react";
import { ErrorInfo } from "../../errors.js";
import { type TransportHooks, type TransportProviderProps, type TreeHandle, type UseActiveTurnsOptions, type UseClientTransportOptions, type UseClientTransportResult, type UseCreateViewOptions, type UseSockudoMessagesOptions, type UseTreeOptions, type UseViewOptions, type ViewHandle } from "../../react/index.js";
import type { InboundMessage } from "../../realtime/index.js";
import type { ClientTransport } from "../../core/transport/index.js";
import { type ChatTransport, type ChatTransportOptions } from "../transport/index.js";
import { type AI, type VercelInput, type VercelOutput, type VercelProjection } from "../codec/index.js";
type VercelTransport = ClientTransport<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>;
type MessageSetter = (value: readonly AI.UIMessage[] | ((messages: readonly AI.UIMessage[]) => readonly AI.UIMessage[])) => void;
/**
 * Provider props for the Vercel `useChat` transport layer.
 *
 * `chatOptions` is captured by `useMemo([transport, chatOptions])`; callers
 * should pass a referentially stable object to avoid replacing the chat
 * transport between renders.
 *
 * @defaultValue `api` defaults to `"/api/chat"`.
 */
export type ChatTransportProviderProps = Omit<TransportProviderProps<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>, "api" | "codec"> & {
    /** Server endpoint URL for the route handler.
     *
     * @defaultValue `"/api/chat"`.
     */
    api?: string;
    /** Optional Vercel chat adapter hooks; keep this object stable. */
    chatOptions?: ChatTransportOptions;
    /** Child React tree. */
    children?: ReactNode;
};
/**
 * Options for {@link useChatTransport}.
 *
 * Missing, skipped, and failed providers return throwing stubs. Error fields
 * are set except when `skip` is true.
 */
export interface UseChatTransportOptions {
    /**
     * Provider channel name.
     *
     * @defaultValue Nearest chat transport provider.
     */
    channelName?: string;
    /**
     * Suppresses lookup and returns throwing stubs with no error fields.
     *
     * @defaultValue `false`.
     */
    skip?: boolean;
}
/**
 * Result returned by {@link useChatTransport}.
 *
 * Stub access throws {@link ErrorInfo} with
 * {@link ErrorCode.InvalidArgument}; async methods on real transports reject
 * with {@link ErrorInfo}.
 */
export interface UseChatTransportResult {
    /** Resolved Vercel chat transport or a throwing stub. */
    chatTransport: ChatTransport;
    /** Resolved underlying client transport or a throwing stub. */
    transport: VercelTransport;
    /**
     * Chat transport lookup or construction error.
     *
     * @defaultValue `undefined` when resolved or skipped.
     */
    chatTransportError?: ErrorInfo;
    /**
     * Underlying client transport lookup or construction error.
     *
     * @defaultValue `undefined` when resolved or skipped.
     */
    transportError?: ErrorInfo;
}
/**
 * Options for {@link useMessageSync}.
 */
export interface UseMessageSyncOptions {
    /** Vercel `useChat` `setMessages` function. */
    setMessages: MessageSetter;
    /**
     * Provider channel name.
     *
     * @defaultValue Nearest chat transport provider.
     */
    channelName?: string;
    /**
     * Suppresses subscriptions.
     *
     * @defaultValue `false`.
     */
    skip?: boolean;
}
/**
 * Creates a Vercel-typed generic transport hook bundle.
 *
 * @defaultValue Type parameters are fixed to the Vercel UIMessage codec.
 */
export declare function createTransportHooks(): TransportHooks<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>;
/**
 * Provides a Vercel `ChatTransport` and the underlying Vercel-typed client
 * transport for one Sockudo channel.
 *
 * This component wraps the generic {@link TransportProvider} with
 * {@link UIMessageCodec}. It does not close the chat transport on unmount; the
 * underlying generic transport provider owns lifecycle and strict-mode cleanup.
 */
export declare function ChatTransportProvider(props: ChatTransportProviderProps): ReturnType<typeof createElement>;
/**
 * Reads the nearest or named Vercel chat transport.
 *
 * @defaultValue Uses the nearest provider when `channelName` is omitted.
 *
 * Missing, skipped, and failed providers return throwing stubs; error fields are
 * omitted only when skipped.
 */
export declare function useChatTransport(options?: UseChatTransportOptions): UseChatTransportResult;
/**
 * Synchronizes Sockudo view updates into Vercel `useChat` state.
 *
 * While this client owns an active stream, synchronization is suppressed to
 * avoid Vercel optimistic id replacement flicker. When streaming transitions to
 * `false`, a sync runs immediately.
 */
export declare function useMessageSync(options: UseMessageSyncOptions): void;
/**
 * Merges Sockudo tree messages with Vercel's local optimistic overlay.
 *
 * Tree message order is preserved. Assistant tool-resolution parts from the
 * overlay win only over matching unresolved tree tool parts, preserving the
 * tree part's `type`. Overlay messages unknown to the tree are appended.
 */
export declare function mergeMessages(treeMessages: readonly AI.UIMessage[], overlayMessages: readonly AI.UIMessage[]): readonly AI.UIMessage[];
/**
 * Reads the nearest or named Vercel client transport.
 *
 * @defaultValue Uses the nearest provider when `channelName` is omitted.
 */
export declare function useClientTransport(options?: UseClientTransportOptions): UseClientTransportResult<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>;
/**
 * Subscribes to a Vercel UIMessage view.
 *
 * @defaultValue Uses the context transport's default view.
 */
export declare function useView(options?: UseViewOptions<VercelInput, AI.UIMessage>): ViewHandle<AI.UIMessage>;
/**
 * Creates and owns an additional Vercel UIMessage view.
 *
 * @defaultValue Uses the context transport.
 */
export declare function useCreateView(options?: UseCreateViewOptions<VercelInput, AI.UIMessage>): ViewHandle<AI.UIMessage>;
/**
 * Returns stable tree callbacks for the Vercel transport.
 *
 * @defaultValue Uses the context transport.
 */
export declare function useTree(options?: UseTreeOptions<VercelInput, AI.UIMessage>): TreeHandle;
/**
 * Subscribes to active/suspended Vercel turn ownership.
 *
 * @defaultValue Uses the context transport.
 */
export declare function useActiveTurns(options?: UseActiveTurnsOptions<VercelInput, AI.UIMessage>): Map<string, Set<string>>;
/**
 * Subscribes to raw normalized Sockudo inbound messages for the Vercel
 * transport.
 *
 * @defaultValue Uses the context transport.
 */
export declare function useSockudoMessages(options?: UseSockudoMessagesOptions<VercelInput, AI.UIMessage>): readonly InboundMessage[];
//# sourceMappingURL=index.d.ts.map

// vercel/vue
export { version } from "../../version.js";
import { type ComputedRef, type Ref, type ShallowRef } from "vue";
import { ErrorInfo } from "../../errors.js";
import { type TransportProviderOptions, type UseActiveTurnsOptions, type UseClientTransportOptions, type UseClientTransportResult, type UseCreateViewOptions, type UseSockudoMessagesOptions, type UseTreeOptions, type UseViewOptions, type ViewHandle, type TreeHandle } from "../../vue/index.js";
import type { InboundMessage } from "../../realtime/index.js";
import type { ClientTransport } from "../../core/transport/index.js";
import { type ChatTransport, type ChatTransportOptions } from "../transport/index.js";
import { type AI, type VercelInput, type VercelOutput, type VercelProjection } from "../codec/index.js";
type VercelTransport = ClientTransport<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>;
/**
 * Provider options for the Vercel AI SDK Vue transport layer.
 */
export type ChatTransportProviderOptions = Omit<TransportProviderOptions<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>, "api" | "codec"> & {
    /** Server endpoint URL for the route handler.
     *
     * @defaultValue `"/api/chat"`.
     */
    api?: string;
    /** Optional Vercel chat adapter hooks. */
    chatOptions?: ChatTransportOptions;
};
/**
 * Result returned by {@link provideChatTransport} and {@link useChatTransport}.
 */
export interface UseChatTransportResult {
    /** Resolved Vercel chat transport ref. */
    chatTransport: ShallowRef<ChatTransport | undefined>;
    /** Resolved underlying client transport ref. */
    transport: ShallowRef<VercelTransport | undefined>;
    /** Chat transport lookup or construction error ref. */
    chatTransportError: ShallowRef<ErrorInfo | undefined>;
    /** Underlying client transport lookup or construction error ref. */
    transportError: ShallowRef<ErrorInfo | undefined>;
}
/**
 * Provides a Vercel `ChatTransport` and underlying Vercel-typed client
 * transport for one Sockudo channel.
 */
export declare function provideChatTransport(options: ChatTransportProviderOptions): UseChatTransportResult;
/**
 * Reads the nearest or named Vercel chat transport.
 */
export declare function useChatTransport(options?: UseClientTransportOptions): UseChatTransportResult;
/**
 * Creates a Vercel-typed generic transport scope.
 */
export declare function createTransportScope(): import("../../vue/index.js").TransportScope<VercelInput, AI.UIMessageChunk, VercelProjection, AI.UIMessage>;
/**
 * Reads the nearest or named Vercel client transport.
 */
export declare function useClientTransport(options?: UseClientTransportOptions): UseClientTransportResult<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>;
/**
 * Subscribes to a Vercel UIMessage view.
 */
export declare function useView(options?: UseViewOptions<VercelInput, AI.UIMessage>): ViewHandle<AI.UIMessage>;
/**
 * Creates and owns an additional Vercel UIMessage view.
 */
export declare function useCreateView(options?: UseCreateViewOptions<VercelInput, AI.UIMessage>): ViewHandle<AI.UIMessage>;
/**
 * Returns stable tree callbacks for the Vercel transport.
 */
export declare function useTree(options?: UseTreeOptions<VercelInput, AI.UIMessage>): TreeHandle;
/**
 * Subscribes to active/suspended Vercel turn ownership.
 */
export declare function useActiveTurns(options?: UseActiveTurnsOptions<VercelInput, AI.UIMessage>): ComputedRef<Map<string, Set<string>>>;
/**
 * Subscribes to raw normalized Sockudo inbound messages for the Vercel
 * transport.
 */
export declare function useSockudoMessages(options?: UseSockudoMessagesOptions<VercelInput, AI.UIMessage>): Ref<readonly InboundMessage[]>;
//# sourceMappingURL=index.d.ts.map

// vercel/svelte
export { version } from "../../version.js";
import { type Readable } from "svelte/store";
import { ErrorInfo } from "../../errors.js";
import { type ActiveTurnsStoreOptions, type ClientTransportState, type ClientTransportStore, type GetClientTransportOptions, type SockudoMessagesStoreOptions, type TransportStoreOptions, type ViewStore, type ViewStoreOptions } from "../../svelte/index.js";
import { type ChatTransport, type ChatTransportOptions } from "../transport/index.js";
import { type AI, type VercelInput, type VercelOutput, type VercelProjection } from "../codec/index.js";
/**
 * Svelte store options for the Vercel AI SDK transport layer.
 */
export type ChatTransportStoreOptions = Omit<TransportStoreOptions<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>, "api" | "codec"> & {
    /** Server endpoint URL for the route handler.
     *
     * @defaultValue `"/api/chat"`.
     */
    api?: string;
    /** Optional Vercel chat adapter hooks. */
    chatOptions?: ChatTransportOptions;
};
/**
 * Svelte Vercel chat transport state.
 */
export interface ChatTransportState {
    /** Resolved Vercel chat transport. */
    chatTransport?: ChatTransport;
    /** Resolved underlying client transport. */
    transport?: ClientTransportState<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>["transport"];
    /** Chat transport lookup or construction error. */
    chatTransportError?: ErrorInfo;
    /** Underlying client transport lookup or construction error. */
    transportError?: ErrorInfo;
}
/**
 * Svelte Vercel chat transport store.
 */
export interface ChatTransportStore extends Readable<ChatTransportState> {
    /** Channel registry key. */
    readonly channelName?: string;
    /** Closes the chat and client transport. */
    close(): Promise<void>;
}
/**
 * Creates a Sockudo-backed Vercel `ChatTransport` Svelte store.
 */
export declare function createChatTransportStore(options: ChatTransportStoreOptions): ChatTransportStore;
/**
 * Creates, stores, and provides a Vercel chat transport in one call.
 */
export declare function provideChatTransport(options: ChatTransportStoreOptions): ChatTransportStore;
/**
 * Reads the nearest or named Vercel chat transport store.
 */
export declare function getChatTransport(options?: GetClientTransportOptions): Readable<ChatTransportState>;
/**
 * Creates a Vercel-typed client transport store.
 */
export declare function createClientTransportStore(options: Omit<TransportStoreOptions<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>, "codec">): ClientTransportStore<VercelInput, VercelOutput, VercelProjection, AI.UIMessage>;
/**
 * Creates a Vercel UIMessage view store.
 */
export declare function createViewStore(options?: ViewStoreOptions<VercelInput, AI.UIMessage>): ViewStore<AI.UIMessage>;
/**
 * Creates and owns an additional Vercel UIMessage view store.
 */
export declare function createOwnedViewStore(options?: Omit<ViewStoreOptions<VercelInput, AI.UIMessage>, "view">): ViewStore<AI.UIMessage>;
/**
 * Creates stable tree callbacks for the Vercel transport.
 */
export declare function createTreeHandle(options?: ActiveTurnsStoreOptions<VercelInput, AI.UIMessage>): {
    getSiblings(id: string): readonly import("../../index.js").TurnNode<unknown>[];
    hasSiblings(id: string): boolean;
    getNode(id: string): import("../../index.js").TurnNode<unknown> | undefined;
};
/**
 * Subscribes to active/suspended Vercel turn ownership.
 */
export declare function createActiveTurnsStore(options?: ActiveTurnsStoreOptions<VercelInput, AI.UIMessage>): Readable<Map<string, Set<string>>>;
/**
 * Subscribes to raw normalized Sockudo inbound messages for the Vercel
 * transport.
 */
export declare function createSockudoMessagesStore(options?: SockudoMessagesStoreOptions<VercelInput, AI.UIMessage>): Readable<readonly import("../../index.js").InboundMessage[]>;
//# sourceMappingURL=index.d.ts.map

// providers
export { version } from "../version.js";
import type { StreamResult, Turn, TurnEndReason } from "../core/transport/index.js";
import type { AI, VercelOutput, VercelProjection } from "../vercel/codec/index.js";
/**
 * Well-known OpenAI-compatible provider identifiers.
 *
 * These providers expose a Chat Completions-compatible streaming endpoint and
 * can be used through {@link streamOpenAICompatibleText} without a provider SDK.
 */
export type OpenAICompatibleProviderName = "openai" | "openrouter" | "groq" | "togetherai" | "fireworks" | "deepseek" | "perplexity" | "mistral" | "xai" | "ollama" | "lmstudio";
/**
 * Default base URLs for high-traffic OpenAI-compatible providers.
 */
export declare const OPENAI_COMPATIBLE_PROVIDER_BASE_URLS: {
    readonly openai: "https://api.openai.com/v1";
    readonly openrouter: "https://openrouter.ai/api/v1";
    readonly groq: "https://api.groq.com/openai/v1";
    readonly togetherai: "https://api.together.xyz/v1";
    readonly fireworks: "https://api.fireworks.ai/inference/v1";
    readonly deepseek: "https://api.deepseek.com";
    readonly perplexity: "https://api.perplexity.ai";
    readonly mistral: "https://api.mistral.ai/v1";
    readonly xai: "https://api.x.ai/v1";
    readonly ollama: "http://127.0.0.1:11434/v1";
    readonly lmstudio: "http://127.0.0.1:1234/v1";
};
/**
 * Chat message shape accepted by OpenAI-compatible chat completion endpoints.
 */
export interface OpenAICompatibleChatMessage {
    /** Message role. */
    role: "system" | "user" | "assistant" | "tool";
    /** Provider-specific message content. */
    content?: unknown;
    /** Optional tool call id for tool result messages. */
    tool_call_id?: string;
    /** Provider-specific extra fields. */
    [key: string]: unknown;
}
/**
 * Shared text-generation request accepted by direct provider adapters.
 */
export interface ProviderTextRequest {
    /** Provider model id. */
    model: string;
    /** Simple user prompt. Ignored when `messages` is supplied. */
    prompt?: string;
    /** OpenAI-compatible chat history. */
    messages?: readonly OpenAICompatibleChatMessage[];
    /** Maximum generated tokens. */
    maxOutputTokens?: number;
    /** Sampling temperature. */
    temperature?: number;
    /** Nucleus sampling value. */
    topP?: number;
    /** Provider-specific request fields. */
    body?: Record<string, unknown>;
    /** Provider-specific headers. */
    headers?: Record<string, string | undefined>;
    /** Abort signal. */
    signal?: AbortSignal;
    /** Stable assistant message id for emitted UI chunks. */
    messageId?: string;
}
/**
 * OpenAI-compatible HTTP streaming options.
 */
export interface OpenAICompatibleStreamOptions extends ProviderTextRequest {
    /** Provider preset. Ignored when `baseURL` is supplied. */
    provider?: OpenAICompatibleProviderName;
    /** Base URL ending before `/chat/completions`. */
    baseURL?: string;
    /** Bearer token. Optional for local providers such as Ollama and LM Studio. */
    apiKey?: string;
    /** Endpoint path.
     *
     * @defaultValue `"/chat/completions"`.
     */
    path?: string;
    /** Fetch implementation.
     *
     * @defaultValue `globalThis.fetch`.
     */
    fetch?: typeof globalThis.fetch;
}
/**
 * Structural subset of the official OpenAI SDK used for Chat Completions.
 */
export interface OpenAIChatCompletionsClient {
    /** Chat completions namespace. */
    chat: {
        completions: {
            /** Creates a streaming chat completion. */
            create(request: Record<string, unknown> & {
                stream: true;
            }): Promise<AsyncIterable<unknown>>;
        };
    };
}
/**
 * Structural subset of the official OpenAI SDK used for Responses.
 */
export interface OpenAIResponsesClient {
    /** Responses namespace. */
    responses: {
        /** Creates a streaming response. */
        create(request: Record<string, unknown> & {
            stream: true;
        }): Promise<AsyncIterable<unknown>>;
    };
}
/**
 * OpenAI SDK Chat Completions stream options.
 */
export interface OpenAIChatCompletionStreamOptions extends ProviderTextRequest {
    /** Official OpenAI SDK client or structural equivalent. */
    client: OpenAIChatCompletionsClient;
}
/**
 * OpenAI SDK Responses stream options.
 */
export interface OpenAIResponseStreamOptions extends ProviderTextRequest {
    /** Official OpenAI SDK client or structural equivalent. */
    client: OpenAIResponsesClient;
    /** Raw Responses API `input` override. */
    input?: unknown;
}
/**
 * Structural subset of the official Anthropic SDK messages client.
 */
export interface AnthropicMessagesClient {
    /** Messages namespace. */
    messages: {
        /** Creates a streaming Anthropic messages response. */
        create(request: Record<string, unknown> & {
            stream: true;
        }): Promise<AsyncIterable<unknown>>;
    };
}
/**
 * Anthropic SDK message stream options.
 */
export interface AnthropicMessageStreamOptions extends ProviderTextRequest {
    /** Official Anthropic SDK client or structural equivalent. */
    client: AnthropicMessagesClient;
    /** Anthropic system prompt. */
    system?: string;
}
/**
 * Minimal direct LLM provider contract.
 */
export interface DirectLlmProvider {
    /** Streams a provider response as Vercel UI message chunks for Sockudo. */
    streamText(request: ProviderTextRequest): Promise<ReadableStream<VercelOutput>>;
}
/**
 * Provider registry returned by {@link createDirectLlmProviderRegistry}.
 */
export interface DirectLlmProviderRegistry {
    /** Resolves a provider by name. */
    get(name: string): DirectLlmProvider | undefined;
    /** Streams with a named provider. */
    streamText(name: string, request: ProviderTextRequest): Promise<ReadableStream<VercelOutput>>;
}
/**
 * Result returned by {@link runDirectLlmTurn}.
 */
export interface RunDirectLlmTurnResult {
    /** Pipe result from `turn.streamResponse`. */
    pipeResult: StreamResult;
    /** Published turn end reason. */
    turnEndReason: TurnEndReason;
}
/**
 * Streams text through a Chat Completions-compatible HTTP endpoint.
 */
export declare function streamOpenAICompatibleText(options: OpenAICompatibleStreamOptions): Promise<ReadableStream<VercelOutput>>;
/**
 * Streams with the official OpenAI SDK Chat Completions API.
 */
export declare function streamOpenAIChatCompletion(options: OpenAIChatCompletionStreamOptions): Promise<ReadableStream<VercelOutput>>;
/**
 * Streams with the official OpenAI SDK Responses API.
 */
export declare function streamOpenAIResponse(options: OpenAIResponseStreamOptions): Promise<ReadableStream<VercelOutput>>;
/**
 * Streams with the official Anthropic SDK Messages API.
 */
export declare function streamAnthropicMessage(options: AnthropicMessageStreamOptions): Promise<ReadableStream<VercelOutput>>;
/**
 * Creates a reusable OpenAI-compatible HTTP provider.
 */
export declare function createOpenAICompatibleProvider(defaults: Omit<OpenAICompatibleStreamOptions, "model"> & {
    model?: string;
}): DirectLlmProvider;
/**
 * Creates a reusable OpenAI SDK provider.
 */
export declare function createOpenAISdkProvider(defaults: (Omit<OpenAIChatCompletionStreamOptions, "model"> & {
    mode?: "chat";
    model?: string;
}) | (Omit<OpenAIResponseStreamOptions, "model"> & {
    mode: "responses";
    model?: string;
})): DirectLlmProvider;
/**
 * Creates a reusable Anthropic SDK provider.
 */
export declare function createAnthropicSdkProvider(defaults: Omit<AnthropicMessageStreamOptions, "model"> & {
    model?: string;
}): DirectLlmProvider;
/**
 * Creates a named direct-provider registry.
 */
export declare function createDirectLlmProviderRegistry(providers: Record<string, DirectLlmProvider>): DirectLlmProviderRegistry;
/**
 * Runs a Sockudo server turn from a direct provider stream.
 *
 * This helper starts the turn, streams provider chunks through
 * `turn.streamResponse`, maps completion to a turn end reason, publishes
 * `ai-turn-end`, and returns the evidence.
 */
export declare function runDirectLlmTurn(turn: Turn<VercelOutput, VercelProjection, AI.UIMessage>, provider: DirectLlmProvider, request: ProviderTextRequest): Promise<RunDirectLlmTurnResult>;
/**
 * Maps OpenAI Chat Completions stream events into UI message chunks.
 */
export declare function openAIChatCompletionEventsToUIMessageStream(events: AsyncIterable<unknown>, options?: {
    messageId?: string;
}): ReadableStream<VercelOutput>;
/**
 * Maps OpenAI Responses stream events into UI message chunks.
 */
export declare function openAIResponseEventsToUIMessageStream(events: AsyncIterable<unknown>, options?: {
    messageId?: string;
}): ReadableStream<VercelOutput>;
/**
 * Maps Anthropic Messages stream events into UI message chunks.
 */
export declare function anthropicMessageEventsToUIMessageStream(events: AsyncIterable<unknown>, options?: {
    messageId?: string;
}): ReadableStream<VercelOutput>;
//# sourceMappingURL=index.d.ts.map
