import type { ErrorInfo } from "../errors.js";
import type { HeaderMap } from "../utils.js";

/** Sockudo serial values preserve unsafe integers as strings. */
export type Serial = number | string;

/** Allowed append rollup windows in milliseconds. */
export type AppendRollupWindow = 0 | 20 | 40 | 100 | 500;

/** Versioned-message action normalized for core logic. */
export type InboundMessageAction = "create" | "append" | "update" | "delete" | "summary";

/** Version metadata for a mutable Sockudo message. */
export interface InboundMessageVersion {
  /** Version serial. */
  serial?: string;
  /** Verified client identity for this version. */
  clientId?: string;
  /** Timestamp in milliseconds since the Unix epoch. */
  timestamp?: number;
  /** Optional version description. */
  description?: string;
  /** Optional opaque version metadata. */
  metadata?: unknown;
}

/** Normalized inbound message consumed by core transport logic. */
export interface InboundMessage {
  /** Logical event name. */
  name: string;
  /** Opaque payload. */
  data: unknown;
  /** Normalized mutable-message action. */
  action: InboundMessageAction;
  /** Stable logical message serial. */
  messageSerial: string;
  /** Durable history serial. */
  historySerial: Serial;
  /** Recovery delivery serial. */
  deliverySerial?: Serial;
  /** Version metadata. */
  version?: InboundMessageVersion;
  /** Verified client identity. */
  clientId?: string;
  /** Timestamp in milliseconds; `0` when absent. */
  timestamp: number;
  /** Idempotent message id. */
  messageId?: string;
  /** Original Sockudo extras. */
  extras?: unknown;
  /** Original Sockudo message object. */
  raw: unknown;
  /** Lazily reads AI transport headers. */
  getTransportHeaders(): HeaderMap;
  /** Lazily reads AI codec headers. */
  getCodecHeaders(): HeaderMap;
}

/** Typed acknowledgement returned by publish and mutation operations. */
export interface MessageAck {
  /** Stable logical message serial. */
  messageSerial: string;
  /** Durable history serial. */
  historySerial: Serial;
  /** Recovery delivery serial. */
  deliverySerial?: Serial;
  /** Version serial. */
  versionSerial?: string;
  /** Server acknowledgement status. */
  status?: string;
}

/** Publish-create request accepted by the realtime seam. */
export interface PublishMessage {
  /** Event name. */
  name?: string;
  /** Opaque payload. */
  data?: unknown;
  /** Extras passed through untouched. */
  extras?: unknown;
  /** Optional caller-provided message serial. */
  messageSerial?: string;
  /** Optional caller-provided idempotent message id. */
  messageId?: string;
  /** Optional operation id. */
  opId?: string;
  /** Optional verified client id for privileged paths. */
  clientId?: string;
  /** Optional socket id for actor-scoped paths. */
  socketId?: string;
}

/** Mutation request accepted by update and delete operations. */
export interface MessageMutation {
  /** Replacement payload. */
  data?: unknown;
  /** Replacement event name. */
  name?: string;
  /** Extras passed through untouched. */
  extras?: unknown;
  /** Fields to clear from the aggregate. */
  clearFields?: readonly string[];
  /** Optional operation id. */
  opId?: string;
  /** Optional verified client id for privileged paths. */
  clientId?: string;
  /** Optional socket id for actor-scoped paths. */
  socketId?: string;
  /** Optional mutation description. */
  description?: string;
  /** Optional opaque mutation metadata. */
  metadata?: unknown;
}

/** History paging options. */
export interface HistoryOptions {
  /** Page size. */
  limit?: number;
  /** Page direction. */
  direction?: "newest_first" | "oldest_first" | "backwards" | "reverse" | "forwards" | "forward";
  /** Opaque cursor. */
  cursor?: string;
  /** Inclusive start bound alias. */
  start?: Serial;
  /** Inclusive end bound alias. */
  end?: Serial;
  /** Inclusive start serial. */
  startSerial?: Serial;
  /** Inclusive end serial. */
  endSerial?: Serial;
  /** Inclusive start timestamp in milliseconds. */
  startTimeMs?: Serial;
  /** Inclusive end timestamp in milliseconds. */
  endTimeMs?: Serial;
  /** Bound history to the subscription attach serial. */
  untilAttach?: boolean;
}

/** Generic paginated result. */
export interface PaginatedResult<T> {
  /** Current page items. */
  items: readonly T[];
  /** Returns true when another page is available. */
  hasNext(): boolean;
  /** Loads the next page. */
  next(): Promise<PaginatedResult<T>>;
}

/** Subscribe-time rewind option. */
export type RewindOption = number | { count: number } | { seconds: number };

/** Channel subscription options. */
export interface SubscribeOptions {
  /** Optional client-side name filter. */
  names?: readonly string[];
  /** Optional Sockudo rewind passthrough. */
  rewind?: RewindOption;
}

/** Inbound message listener. */
export type MessageListener = (message: InboundMessage) => void;

/** Unsubscribe callback. */
export type Unsubscribe = () => void;

/** Presence member snapshot item. */
export interface PresenceMember {
  /** Member id. */
  id: string;
  /** Member data. */
  data: unknown;
}

/** Presence event names. */
export type PresenceEventName = "enter" | "update" | "leave";

/** Minimal presence API used by upper layers. */
export interface PresenceLike {
  /** Enters presence with optional member data. */
  enter(data?: unknown): Promise<void>;
  /** Updates current member data. */
  update(data?: unknown): Promise<void>;
  /** Leaves presence. */
  leave(data?: unknown): Promise<void>;
  /** Gets current members. */
  get(): Promise<readonly PresenceMember[]>;
  /** Subscribes to presence events. */
  subscribe(listener: (event: PresenceEventName, member: PresenceMember) => void): Unsubscribe;
}

/** Channel-level seam events. */
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

/** Minimal channel API consumed by AI Transport core. */
export interface ChannelLike {
  /** Channel name. */
  readonly name: string;
  /** Sockudo attach serial captured at subscription time. */
  readonly attachSerial: Serial | undefined;
  /** Presence API for this channel. */
  readonly presence: PresenceLike;
  /** Publishes a versioned create. */
  publish(message: PublishMessage): Promise<MessageAck>;
  /** Appends to a mutable message by message serial. */
  appendMessage(
    messageSerial: string,
    data: string,
    options?: Omit<MessageMutation, "data">,
  ): Promise<MessageAck>;
  /** Updates a mutable message by message serial. */
  updateMessage(messageSerial: string, options?: MessageMutation): Promise<MessageAck>;
  /** Deletes a mutable message by message serial. */
  deleteMessage(messageSerial: string, options?: MessageMutation): Promise<MessageAck>;
  /** Subscribes to the unfiltered delivery firehose. */
  subscribe(listener: MessageListener, options?: SubscribeOptions): Unsubscribe;
  /** Reads channel history. */
  history(options?: HistoryOptions): Promise<PaginatedResult<InboundMessage>>;
  /** Subscribes to channel seam events. */
  on<K extends keyof ChannelEvents>(
    event: K,
    listener: (payload: ChannelEvents[K]) => void,
  ): Unsubscribe;
}

/** Realtime connection state. */
export type ConnectionState =
  | "initialized"
  | "connecting"
  | "connected"
  | "disconnected"
  | "unavailable"
  | "failed";

/** Channel retrieval options. */
export interface GetChannelOptions {
  /** Optional subscription params. */
  params?: {
    /** Optional subscribe-time rewind. */
    rewind?: RewindOption;
  };
}

/** Minimal realtime client API consumed by providers and tests. */
export interface ClientLike {
  /** Channel registry. */
  readonly channels: {
    /** Gets a channel by name. */
    get(name: string, options?: GetChannelOptions): ChannelLike;
  };
  /** Connection state and verified identity. */
  readonly connection: {
    /** Current connection state. */
    readonly state: string;
    /** Verified server identity for this connection. */
    readonly clientId: string | undefined;
  };
  /** Closes the client. */
  close(): void;
}
