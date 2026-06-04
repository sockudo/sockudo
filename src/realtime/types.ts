import type { ErrorInfo } from "../errors.js";
import type { HeaderMap } from "../utils.js";

/**
 * Sockudo serial values preserve unsafe integer boundaries as strings.
 */
export type Serial = number | string;

/**
 * Allowed append rollup windows in milliseconds.
 */
export type AppendRollupWindow = 0 | 20 | 40 | 100 | 500;

/**
 * Mutable-message action normalized for AI Transport core logic.
 */
export type InboundMessageAction =
  | "create"
  | "append"
  | "update"
  | "delete"
  | "summary";

/**
 * Version metadata carried by a versioned Sockudo message.
 */
export interface InboundMessageVersion {
  /** Version serial for this mutation. */
  serial?: string;
  /** Verified client identity associated with this version. */
  clientId?: string;
  /** Version timestamp in milliseconds since the Unix epoch. */
  timestamp?: number;
  /** Optional version description. */
  description?: string;
  /** Optional opaque version metadata. */
  metadata?: unknown;
}

/**
 * Normalized Sockudo channel message passed into AI Transport core logic.
 */
export interface InboundMessage {
  /** Event name, for example `ai-output`. */
  name: string;
  /** Opaque message payload. */
  data: unknown;
  /** Versioned-message action normalized without the `message.` prefix. */
  action: InboundMessageAction;
  /** Stable logical message serial. */
  messageSerial: string;
  /** Durable history serial used for total ordering. */
  historySerial: Serial;
  /** Recovery delivery serial when present. */
  deliverySerial?: Serial;
  /** Version metadata when present. */
  version?: InboundMessageVersion;
  /** Verified client identity from the server when present. */
  clientId?: string;
  /** Message timestamp in milliseconds; `0` when the peer did not provide one. */
  timestamp: number;
  /** Idempotent message identity when present. */
  messageId?: string;
  /** Original Sockudo extras object. */
  extras?: unknown;
  /** Original Sockudo message object. */
  raw: unknown;
  /** Lazily reads `extras.ai.transport` into a null-prototype map. */
  getTransportHeaders(): HeaderMap;
  /** Lazily reads `extras.ai.codec` into a null-prototype map. */
  getCodecHeaders(): HeaderMap;
}

/**
 * Acknowledgement returned by create and mutation operations.
 */
export interface MessageAck {
  /** Stable logical message serial. */
  messageSerial: string;
  /** Durable history serial for the accepted operation. */
  historySerial: Serial;
  /** Recovery delivery serial for the accepted operation. */
  deliverySerial?: Serial;
  /** Version serial for the accepted operation. */
  versionSerial?: string;
  /** Server acknowledgement status, for example `duplicate`. */
  status?: string;
}

/**
 * Publish-create request accepted by {@link ChannelLike.publish}.
 */
export interface PublishMessage {
  /** Event name to publish. */
  name?: string;
  /** Opaque payload to publish. */
  data?: unknown;
  /** Sockudo extras to pass through untouched. */
  extras?: unknown;
  /** Optional caller-provided message serial. */
  messageSerial?: string;
  /** Optional caller-provided idempotent message id. */
  messageId?: string;
  /** Optional idempotency operation id. */
  opId?: string;
  /** Optional verified client id for server-authorized publish paths. */
  clientId?: string;
  /** Optional connected socket id for actor-scoped publish paths. */
  socketId?: string;
}

/**
 * Mutation request accepted by update and delete operations.
 */
export interface MessageMutation {
  /** Replacement payload for update/delete operations. */
  data?: unknown;
  /** Optional replacement event name. */
  name?: string;
  /** Sockudo extras to pass through untouched. */
  extras?: unknown;
  /** Fields to clear from the aggregate. */
  clearFields?: readonly string[];
  /** Optional idempotency operation id. */
  opId?: string;
  /** Optional verified client id for server-authorized mutation paths. */
  clientId?: string;
  /** Optional connected socket id for actor-scoped mutation paths. */
  socketId?: string;
  /** Optional mutation description. */
  description?: string;
  /** Optional opaque mutation metadata. */
  metadata?: unknown;
}

/**
 * History paging options for Sockudo client history.
 */
export interface HistoryOptions {
  /** Page size; server caps this to the configured maximum. */
  limit?: number;
  /** Page direction. */
  direction?:
    | "newest_first"
    | "oldest_first"
    | "backwards"
    | "reverse"
    | "forwards"
    | "forward";
  /** Opaque cursor returned by the previous page. */
  cursor?: string;
  /** Inclusive start serial bound. */
  start?: Serial;
  /** Inclusive end serial bound. */
  end?: Serial;
  /** Inclusive start serial bound using Sockudo's request field name. */
  startSerial?: Serial;
  /** Inclusive end serial bound using Sockudo's request field name. */
  endSerial?: Serial;
  /** Inclusive start timestamp bound in milliseconds. */
  startTimeMs?: Serial;
  /** Inclusive end timestamp bound in milliseconds. */
  endTimeMs?: Serial;
  /** Bound history to the subscription attach serial for gapless backfill. */
  untilAttach?: boolean;
}

/**
 * Paginated result returned by history-style APIs.
 */
export interface PaginatedResult<T> {
  /** Items in the current page. */
  items: readonly T[];
  /** Returns true when another page is available. */
  hasNext(): boolean;
  /** Loads the next page or rejects with `ErrorInfo` when none exists. */
  next(): Promise<PaginatedResult<T>>;
}

/**
 * Rewind options accepted when subscribing.
 */
export type RewindOption = number | { count: number } | { seconds: number };

/**
 * Subscription options for {@link ChannelLike.subscribe}.
 */
export interface SubscribeOptions {
  /** Optional client-side name filter; the underlying Sockudo subscription remains unfiltered. */
  names?: readonly string[];
  /** Optional Sockudo rewind request. */
  rewind?: RewindOption;
}

/**
 * Function called for each normalized channel message.
 */
export type MessageListener = (message: InboundMessage) => void;

/**
 * Unsubscribe function returned by event subscriptions.
 */
export type Unsubscribe = () => void;

/**
 * Presence member in a Sockudo presence channel.
 */
export interface PresenceMember {
  /** Member id. */
  id: string;
  /** Member data. */
  data: unknown;
}

/**
 * Presence event names normalized by the realtime seam.
 */
export type PresenceEventName = "enter" | "update" | "leave";

/**
 * Minimal presence API consumed by AI Transport upper layers.
 */
export interface PresenceLike {
  /** Enters the presence channel with optional member data. */
  enter(data?: unknown): Promise<void>;
  /** Updates the current member data without leaving and re-entering. */
  update(data?: unknown): Promise<void>;
  /** Leaves the presence channel. */
  leave(data?: unknown): Promise<void>;
  /** Gets the current presence member snapshot. */
  get(): Promise<readonly PresenceMember[]>;
  /** Subscribes to presence member events. */
  subscribe(
    listener: (event: PresenceEventName, member: PresenceMember) => void,
  ): Unsubscribe;
}

/**
 * Channel-level events emitted by the realtime seam.
 */
export interface ChannelEvents {
  /** Continuity was lost and history backfill is required. */
  continuity_lost: ErrorInfo;
  /** Underlying channel attached. */
  attached: undefined;
  /** Underlying channel detached. */
  detached: undefined;
  /** Underlying channel failed. */
  failed: ErrorInfo;
}

/**
 * Minimal channel API consumed by AI Transport core logic.
 */
export interface ChannelLike {
  /** Channel name. */
  readonly name: string;
  /** Attach serial captured by Sockudo at subscription time. */
  readonly attachSerial?: Serial;
  /** Presence API for this channel. */
  readonly presence: PresenceLike;
  /** Publishes a versioned create and resolves with typed serial acknowledgement. */
  publish(message: PublishMessage): Promise<MessageAck>;
  /** Appends data to a mutable message by `messageSerial`. */
  appendMessage(
    messageSerial: string,
    data: string,
    options?: Omit<MessageMutation, "data">,
  ): Promise<MessageAck>;
  /** Updates a mutable message by `messageSerial`. */
  updateMessage(
    messageSerial: string,
    options?: MessageMutation,
  ): Promise<MessageAck>;
  /** Deletes a mutable message by `messageSerial`. */
  deleteMessage(
    messageSerial: string,
    options?: MessageMutation,
  ): Promise<MessageAck>;
  /** Subscribes to the unfiltered channel firehose with optional client-side name filtering. */
  subscribe(listener: MessageListener, options?: SubscribeOptions): Unsubscribe;
  /** Reads channel history with Sockudo pagination and optional `untilAttach`. */
  history(options?: HistoryOptions): Promise<PaginatedResult<InboundMessage>>;
  /** Subscribes to channel seam events. */
  on<K extends keyof ChannelEvents>(
    event: K,
    listener: (payload: ChannelEvents[K]) => void,
  ): Unsubscribe;
}

/**
 * Realtime connection states surfaced by the seam.
 */
export type ConnectionState =
  | "initialized"
  | "connecting"
  | "connected"
  | "disconnected"
  | "unavailable"
  | "failed";

/**
 * Options for channel retrieval through {@link ClientLike.channels.get}.
 */
export interface GetChannelOptions {
  /** Optional Sockudo subscription params. */
  params?: {
    /** Optional subscribe-time rewind. */
    rewind?: RewindOption;
  };
}

/**
 * Minimal client API consumed by AI Transport providers and tests.
 */
export interface ClientLike {
  /** Channel registry. */
  readonly channels: {
    /** Gets a normalized channel adapter by name. */
    get(name: string, options?: GetChannelOptions): ChannelLike;
  };
  /** Current connection state. */
  readonly connection: {
    /** Realtime connection state. */
    readonly state: ConnectionState | string;
    /** Verified server identity for this connection. */
    readonly clientId?: string;
  };
  /** Closes the underlying realtime connection. */
  close(): void;
}
