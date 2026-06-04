import {
  HEADER_CODEC_MESSAGE_ID,
  HEADER_EVENT_ID,
} from "../constants.js";
import { ErrorCode, ErrorInfo, toErrorInfo } from "../errors.js";
import {
  getCodecHeaders,
  getTransportHeaders,
  type HeaderMap,
} from "../utils.js";
import type {
  AppendRollupWindow,
  ChannelEvents,
  ChannelLike,
  ClientLike,
  ConnectionState,
  GetChannelOptions,
  HistoryOptions,
  InboundMessage,
  InboundMessageAction,
  InboundMessageVersion,
  MessageAck,
  MessageListener,
  MessageMutation,
  PaginatedResult,
  PresenceEventName,
  PresenceLike,
  PresenceMember,
  PublishMessage,
  RewindOption,
  Serial,
  SubscribeOptions,
  Unsubscribe,
} from "./types.js";

/**
 * Structural subset of `@sockudo/client` mutable-message helper output.
 */
export interface SockudoMutableMessageInfo {
  /** Sockudo mutable-message action with the `message.` prefix. */
  action: string;
  /** Delivered event name. */
  event: string;
  /** Stable logical message serial. */
  messageSerial: string;
  /** Version serial when available. */
  versionSerial?: string;
  /** Durable history serial when available. */
  historySerial?: Serial;
  /** Version timestamp in milliseconds when available. */
  versionTimestampMs?: number;
}

/**
 * Function signature compatible with `@sockudo/client` `getMutableMessageInfo`.
 */
export type MutableMessageInfoReader = (
  event: Pick<SockudoRawMessage, "event" | "extras">,
) => SockudoMutableMessageInfo | null;

/**
 * Raw Sockudo message shape consumed by the adapter.
 */
export interface SockudoRawMessage {
  /** Delivered Sockudo event name. */
  event: string;
  /** Delivered channel name. */
  channel?: string;
  /** Opaque payload. */
  data?: unknown;
  /** Logical event name inside mutable-message envelopes. */
  name?: string;
  /** Verified user/client identity when supplied by Sockudo. */
  user_id?: string;
  /** Recovery stream identity. */
  stream_id?: string;
  /** Idempotent message id. */
  message_id?: string;
  /** Delivery serial. */
  serial?: Serial;
  /** Sockudo extras. */
  extras?: unknown;
  /** Raw websocket message retained by sockudo-js. */
  rawMessage?: string;
  /** Additional peer fields from future additive protocol versions. */
  [key: string]: unknown;
}

/**
 * Structural channel subset required from `@sockudo/client`.
 */
export interface SockudoChannelPeer {
  /** Channel name. */
  name: string;
  /** Sockudo attach serial captured at subscription time. */
  attachSerial?: Serial;
  /** Publish-create helper added by Sockudo AI platform primitives. */
  publishCreate?(message: Record<string, unknown>): Promise<unknown>;
  /** Append helper added by Sockudo AI platform primitives. */
  appendMessage?(
    messageSerial: string,
    data: string,
    options?: Record<string, unknown>,
  ): Promise<unknown>;
  /** Update helper added by Sockudo AI platform primitives. */
  updateMessage?(
    messageSerial: string,
    options?: Record<string, unknown>,
  ): Promise<unknown>;
  /** Delete helper added by Sockudo AI platform primitives. */
  deleteMessage?(
    messageSerial: string,
    options?: Record<string, unknown>,
  ): Promise<unknown>;
  /** Client history helper added by Sockudo AI platform primitives. */
  channelHistory?(params?: Record<string, unknown>): Promise<unknown>;
  /** Event binding from sockudo-js's dispatcher. */
  bind?(event: string, listener: (...args: readonly unknown[]) => void): unknown;
  /** Event unbinding from sockudo-js's dispatcher. */
  unbind?(event?: string, listener?: (...args: readonly unknown[]) => void): unknown;
  /** Global channel event binding from sockudo-js's dispatcher. */
  bind_global?(listener: (...args: readonly unknown[]) => void): unknown;
  /** Global channel event unbinding from sockudo-js's dispatcher. */
  unbind_global?(listener?: (...args: readonly unknown[]) => void): unknown;
  /** Raw event handler used by sockudo-js before dispatcher emission. */
  handleEvent?(event: SockudoRawMessage): unknown;
  /** Presence methods when this is a presence channel. */
  members?: SockudoMembersPeer;
  /** Presence update helper on presence channels. */
  update?(data?: unknown): Promise<void>;
  /** Presence enter helper on compatible presence channels. */
  enter?(data?: unknown): Promise<void>;
  /** Presence leave helper on compatible presence channels. */
  leave?(data?: unknown): Promise<void>;
}

/**
 * Structural client subset required from `@sockudo/client`.
 */
export interface SockudoClientPeer {
  /** Sockudo channel registry. */
  channels?: {
    /** Adds or returns a channel object. */
    add?(name: string, client: SockudoClientPeer): SockudoChannelPeer;
    /** Finds a channel object. */
    find?(name: string): SockudoChannelPeer | undefined;
  };
  /** Sockudo connection object. */
  connection?: {
    /** Current connection state. */
    state?: string;
    /** Verified client identity from the server. */
    clientId?: string;
  };
  /** Subscribe to a channel. */
  subscribe?(name: string, options?: Record<string, unknown>): SockudoChannelPeer;
  /** Returns a channel if present. */
  channel?(name: string): SockudoChannelPeer | undefined;
  /** Binds a global client event. */
  bind?(event: string, listener: (payload: unknown) => void): unknown;
  /** Unbinds a global client event. */
  unbind?(event?: string, listener?: (payload: unknown) => void): unknown;
  /** Returns the verified client identity when available. */
  verifiedClientId?(): string | undefined;
  /** Closes the Sockudo connection. */
  disconnect?(): void;
}

/**
 * Options for adapting a Sockudo client.
 */
export interface AdaptSockudoClientOptions {
  /** Optional `getMutableMessageInfo` implementation from `@sockudo/client`. */
  mutableMessageInfoReader?: MutableMessageInfoReader;
}

/**
 * Options for adapting a Sockudo channel.
 */
export interface AdaptSockudoChannelOptions extends AdaptSockudoClientOptions {
  /** Optional parent Sockudo client used for subscription and recovery events. */
  client?: SockudoClientPeer;
  /** Optional subscribe-time rewind passthrough. */
  rewind?: RewindOption;
}

/**
 * Sockudo client construction options accepted by the lazy peer factory.
 */
export interface CreateSockudoRealtimeClientOptions
  extends AdaptSockudoClientOptions {
  /** Options passed through to `new Sockudo(appKey, options)`. */
  clientOptions?: Record<string, unknown>;
  /** Optional append rollup window passed as `transportParams.append_rollup_window`. */
  appendRollupWindow?: AppendRollupWindow;
}

const allowedAppendRollupWindows = new Set<number>([0, 20, 40, 100, 500]);
const channelState = new WeakMap<SockudoChannelPeer, ChannelAdapterState>();
const clientState = new WeakMap<SockudoClientPeer, ClientAdapterState>();

/**
 * Creates a `ClientLike` by lazily importing the `@sockudo/client` peer dependency.
 *
 * This keeps the SDK tree-shakeable for consumers that pass their own adapted
 * channel and ensures the peer dependency is loaded only when this factory is used.
 */
export async function createSockudoRealtimeClient(
  appKey: string,
  options: CreateSockudoRealtimeClientOptions = {},
): Promise<ClientLike> {
  const Sockudo = await loadSockudoClientConstructor();
  const clientOptions = normalizeSockudoClientOptions(options);
  return adaptSockudoClient(new Sockudo(appKey, clientOptions), options);
}

/**
 * Adapts an existing `@sockudo/client` instance to the SDK seam.
 */
export function adaptSockudoClient(
  client: SockudoClientPeer,
  options: AdaptSockudoClientOptions = {},
): ClientLike {
  const state = ensureClientState(client, options);
  return {
    channels: {
      get(name: string, getOptions?: GetChannelOptions): ChannelLike {
        const rewind = getOptions?.params?.rewind;
        const rawChannel = getOrSubscribeChannel(client, name, rewind);
        let channel = state.channels.get(name);
        if (!channel) {
          channel = adaptSockudoChannel(rawChannel, {
            ...options,
            client,
            rewind,
          });
          state.channels.set(name, channel);
        }
        return channel;
      },
    },
    connection: {
      get state(): ConnectionState | string {
        return client.connection?.state ?? "initialized";
      },
      get clientId(): string | undefined {
        return client.verifiedClientId?.() ?? client.connection?.clientId;
      },
    },
    close(): void {
      client.disconnect?.();
    },
  };
}

/**
 * Adapts an existing `@sockudo/client` channel to the SDK seam.
 */
export function adaptSockudoChannel(
  channel: SockudoChannelPeer,
  options: AdaptSockudoChannelOptions = {},
): ChannelLike {
  const state = ensureChannelState(channel, options);
  return {
    get name(): string {
      return channel.name;
    },
    get attachSerial(): Serial | undefined {
      return channel.attachSerial;
    },
    presence: adaptPresence(channel),
    publish(message: PublishMessage): Promise<MessageAck> {
      return callAck(
        "publish a message",
        channel.publishCreate,
        channel,
        normalizePublishMessage(message),
      );
    },
    appendMessage(
      messageSerial: string,
      data: string,
      mutationOptions: Omit<MessageMutation, "data"> = {},
    ): Promise<MessageAck> {
      return callAck(
        "append to a message",
        channel.appendMessage,
        channel,
        messageSerial,
        data,
        normalizeMutation(mutationOptions),
      );
    },
    updateMessage(
      messageSerial: string,
      mutationOptions: MessageMutation = {},
    ): Promise<MessageAck> {
      return callAck(
        "update a message",
        channel.updateMessage,
        channel,
        messageSerial,
        normalizeMutation(mutationOptions),
      );
    },
    deleteMessage(
      messageSerial: string,
      mutationOptions: MessageMutation = {},
    ): Promise<MessageAck> {
      return callAck(
        "delete a message",
        channel.deleteMessage,
        channel,
        messageSerial,
        normalizeMutation(mutationOptions),
      );
    },
    subscribe(listener: MessageListener, subscribeOptions?: SubscribeOptions): Unsubscribe {
      if (options.client && subscribeOptions?.rewind !== undefined) {
        options.client.subscribe?.(channel.name, { rewind: subscribeOptions.rewind });
      }
      const names = subscribeOptions?.names
        ? new Set(subscribeOptions.names)
        : undefined;
      const wrapped = (message: InboundMessage): void => {
        if (!names || names.has(message.name)) {
          listener(message);
        }
      };
      state.listeners.add(wrapped);
      return () => {
        state.listeners.delete(wrapped);
      };
    },
    history(historyOptions: HistoryOptions = {}): Promise<PaginatedResult<InboundMessage>> {
      return readHistory(channel, historyOptions, state.mutableMessageInfoReader);
    },
    on<K extends keyof ChannelEvents>(
      event: K,
      listener: (payload: ChannelEvents[K]) => void,
    ): Unsubscribe {
      return state.events.on(event, listener);
    },
  };
}

/**
 * Normalizes a raw Sockudo message into the AI Transport realtime seam shape.
 */
export function normalizeInboundMessage(
  raw: SockudoRawMessage,
  mutableMessageInfoReader?: MutableMessageInfoReader,
): InboundMessage {
  const mutableInfo = mutableMessageInfoReader?.({
    event: raw.event,
    extras: raw.extras,
  });
  const rawRecord = raw as Record<string, unknown>;
  const action = normalizeAction(raw, mutableInfo);
  const version = normalizeVersion(rawRecord, mutableInfo);
  let transportHeaders: HeaderMap | undefined;
  let codecHeaders: HeaderMap | undefined;
  const message: InboundMessage = {
    name: readString(rawRecord.name) ?? raw.event,
    data: raw.data,
    action,
    messageSerial:
      readString(rawRecord.message_serial) ??
      readString(rawRecord.messageSerial) ??
      mutableInfo?.messageSerial ??
      readString(raw.message_id) ??
      readTierValue(raw.extras, "transport", HEADER_CODEC_MESSAGE_ID) ??
      readTierValue(raw.extras, "transport", HEADER_EVENT_ID) ??
      "",
    historySerial:
      readSerial(rawRecord.history_serial) ??
      readSerial(rawRecord.historySerial) ??
      mutableInfo?.historySerial ??
      readSockudoHeaderSerial(raw.extras, "sockudo_history_serial") ??
      readSerial(raw.serial) ??
      "0",
    timestamp:
      readNumber(rawRecord.timestamp) ??
      readNumber(rawRecord.timestamp_ms) ??
      readNumber(rawRecord.timestampMs) ??
      mutableInfo?.versionTimestampMs ??
      version?.timestamp ??
      0,
    raw,
    getTransportHeaders(): HeaderMap {
      transportHeaders ??= getTransportHeaders(raw.extras);
      return transportHeaders;
    },
    getCodecHeaders(): HeaderMap {
      codecHeaders ??= getCodecHeaders(raw.extras);
      return codecHeaders;
    },
  };

  assignOptional(message, "deliverySerial", readSerial(rawRecord.delivery_serial));
  assignOptional(message, "deliverySerial", message.deliverySerial ?? readSerial(rawRecord.deliverySerial));
  assignOptional(message, "deliverySerial", message.deliverySerial ?? readSerial(raw.serial));
  assignOptional(message, "version", version);
  assignOptional(
    message,
    "clientId",
    readString(rawRecord.client_id) ??
      readString(rawRecord.clientId) ??
      readString(raw.user_id) ??
      version?.clientId,
  );
  assignOptional(message, "messageId", readString(raw.message_id));
  assignOptional(message, "extras", raw.extras);

  return message;
}

/**
 * Compares Sockudo serial values without truncating unsafe integer strings.
 */
export function compareSerial(a: Serial, b: Serial): -1 | 0 | 1 {
  const left = serialToBigInt(a);
  const right = serialToBigInt(b);
  if (left !== undefined && right !== undefined) {
    if (left < right) {
      return -1;
    }
    if (left > right) {
      return 1;
    }
    return 0;
  }
  const leftString = String(a);
  const rightString = String(b);
  if (leftString < rightString) {
    return -1;
  }
  if (leftString > rightString) {
    return 1;
  }
  return 0;
}

/**
 * Returns whether `a` is less than or equal to `b` using BigInt-safe serial comparison.
 */
export function serialLessThanOrEqual(a: Serial, b: Serial): boolean {
  return compareSerial(a, b) <= 0;
}

/**
 * Validates a local append rollup window option.
 */
export function validateAppendRollupWindow(
  value: unknown,
): asserts value is AppendRollupWindow {
  if (
    typeof value !== "number" ||
    !allowedAppendRollupWindows.has(value) ||
    !Number.isInteger(value)
  ) {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message:
        "unable to create realtime client; appendRollupWindow must be one of 0, 20, 40, 100, or 500",
    });
  }
}

function ensureClientState(
  client: SockudoClientPeer,
  options: AdaptSockudoClientOptions,
): ClientAdapterState {
  const existing = clientState.get(client);
  if (existing) {
    return existing;
  }
  const state: ClientAdapterState = {
    channels: new Map(),
    mutableMessageInfoReader: options.mutableMessageInfoReader,
  };
  clientState.set(client, state);
  bindRecoveryEvents(client, state);
  return state;
}

function ensureChannelState(
  channel: SockudoChannelPeer,
  options: AdaptSockudoChannelOptions,
): ChannelAdapterState {
  const existing = channelState.get(channel);
  if (existing) {
    return existing;
  }
  const state: ChannelAdapterState = {
    listeners: new Set(),
    events: new TinyEmitter<ChannelEvents>(),
    mutableMessageInfoReader: options.mutableMessageInfoReader,
  };
  channelState.set(channel, state);
  patchChannelHandleEvent(channel, state);
  bindChannelStateEvents(channel, state);
  return state;
}

function getOrSubscribeChannel(
  client: SockudoClientPeer,
  name: string,
  rewind?: RewindOption,
): SockudoChannelPeer {
  const existing = client.channel?.(name) ?? client.channels?.find?.(name);
  if (existing) {
    if (rewind !== undefined) {
      client.subscribe?.(name, { rewind });
    }
    return existing;
  }
  const subscribed = client.subscribe?.(name, rewind === undefined ? undefined : { rewind });
  if (subscribed) {
    return subscribed;
  }
  const added = client.channels?.add?.(name, client);
  if (!added) {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message: "unable to get channel; Sockudo client does not expose channels.add or subscribe",
    });
  }
  return added;
}

function patchChannelHandleEvent(
  channel: SockudoChannelPeer,
  state: ChannelAdapterState,
): void {
  const original = channel.handleEvent;
  if (!original) {
    const globalListener = (eventName: unknown, data: unknown): void => {
      if (typeof eventName !== "string") {
        return;
      }
      dispatchInbound(state, {
        event: eventName,
        channel: channel.name,
        data,
      });
    };
    channel.bind_global?.(globalListener);
    return;
  }
  channel.handleEvent = (event: SockudoRawMessage): unknown => {
    dispatchInbound(state, event);
    return original.call(channel, event);
  };
}

function bindChannelStateEvents(
  channel: SockudoChannelPeer,
  state: ChannelAdapterState,
): void {
  const attached = (): void => state.events.emit("attached", undefined);
  const failed = (payload: unknown): void => {
    state.events.emit(
      "failed",
      toErrorInfo(payload, {
        code: ErrorCode.TransportSubscriptionError,
        message: "unable to subscribe to channel; Sockudo reported a subscription failure",
      }),
    );
  };
  channel.bind?.("sockudo:subscription_succeeded", attached);
  channel.bind?.("pusher:subscription_succeeded", attached);
  channel.bind?.("subscription_succeeded", attached);
  channel.bind?.("sockudo:subscription_error", failed);
  channel.bind?.("pusher:subscription_error", failed);
  channel.bind?.("subscription_error", failed);
}

function bindRecoveryEvents(
  client: SockudoClientPeer,
  state: ClientAdapterState,
): void {
  const handleFailed = (payload: unknown): void => {
    const fail = objectRecord(payload);
    const channelName = readString(fail?.channel);
    const code = readString(fail?.code);
    if (
      !channelName ||
      (code !== "stream_reset" && code !== "position_expired")
    ) {
      return;
    }
    const channel = state.channels.get(channelName);
    const raw = client.channel?.(channelName) ?? client.channels?.find?.(channelName);
    const rawState = raw ? channelState.get(raw) : undefined;
    const error = new ErrorInfo({
      code: ErrorCode.ChannelContinuityLost,
      message: `unable to maintain channel continuity; ${code}`,
      statusCode: 409,
      detail: payload,
    });
    rawState?.events.emit("continuity_lost", error);
    channel?.on("continuity_lost", () => {
      return undefined;
    });
  };
  client.bind?.("sockudo:resume_failed", handleFailed);
  client.bind?.("pusher:resume_failed", handleFailed);
  client.bind?.("resume_failed", handleFailed);
}

function dispatchInbound(
  state: ChannelAdapterState,
  raw: SockudoRawMessage,
): void {
  if (state.listeners.size === 0) {
    return;
  }
  const message = normalizeInboundMessage(raw, state.mutableMessageInfoReader);
  for (const listener of state.listeners) {
    listener(message);
  }
}

async function callAck(
  operation: string,
  method: ((...args: readonly unknown[]) => Promise<unknown>) | undefined,
  context: unknown,
  ...args: readonly unknown[]
): Promise<MessageAck> {
  if (!method) {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message: `unable to ${operation}; Sockudo client does not expose this method`,
    });
  }
  try {
    return normalizeAck(await method.apply(context, args));
  } catch (error) {
    throw mapSockudoFailure(error, operation);
  }
}

async function readHistory(
  channel: SockudoChannelPeer,
  options: HistoryOptions,
  mutableMessageInfoReader?: MutableMessageInfoReader,
): Promise<PaginatedResult<InboundMessage>> {
  if (!channel.channelHistory) {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message: "unable to read channel history; Sockudo client does not expose channelHistory",
    });
  }
  try {
    const page = await channel.channelHistory(normalizeHistoryOptions(options));
    return normalizeHistoryPage(page, mutableMessageInfoReader);
  } catch (error) {
    throw mapSockudoFailure(error, "read channel history");
  }
}

function normalizeHistoryPage(
  page: unknown,
  mutableMessageInfoReader?: MutableMessageInfoReader,
): PaginatedResult<InboundMessage> {
  const record = objectRecord(page);
  const rawItems = Array.isArray(record?.items) ? record.items : [];
  const items = rawItems.map((item) =>
    normalizeInboundMessage(normalizeRawMessage(item), mutableMessageInfoReader),
  );
  return {
    items,
    hasNext(): boolean {
      const hasNext = record?.hasNext;
      return typeof hasNext === "function"
        ? Boolean(hasNext.call(page))
        : Boolean(record?.has_more && record.next_cursor);
    },
    async next(): Promise<PaginatedResult<InboundMessage>> {
      const next = record?.next;
      if (typeof next !== "function") {
        throw new ErrorInfo({
          code: ErrorCode.InvalidArgument,
          message: "unable to read next history page; no next page is available",
        });
      }
      try {
        return normalizeHistoryPage(
          await next.call(page),
          mutableMessageInfoReader,
        );
      } catch (error) {
        throw mapSockudoFailure(error, "read next history page");
      }
    },
  };
}

function normalizeRawMessage(value: unknown): SockudoRawMessage {
  const record = objectRecord(value);
  return {
    ...record,
    event: readString(record?.event) ?? readString(record?.name) ?? "",
  };
}

function normalizePublishMessage(message: PublishMessage): Record<string, unknown> {
  return compactRecord({
    name: message.name,
    data: message.data,
    extras: message.extras,
    messageSerial: message.messageSerial,
    messageId: message.messageId,
    opId: message.opId,
    clientId: message.clientId,
    socketId: message.socketId,
  });
}

function normalizeMutation(
  mutation: MessageMutation | Omit<MessageMutation, "data">,
): Record<string, unknown> {
  return compactRecord({
    data: "data" in mutation ? mutation.data : undefined,
    name: mutation.name,
    extras: mutation.extras,
    clearFields: mutation.clearFields,
    opId: mutation.opId,
    clientId: mutation.clientId,
    socketId: mutation.socketId,
    description: mutation.description,
    metadata: mutation.metadata,
  });
}

function normalizeHistoryOptions(options: HistoryOptions): Record<string, unknown> {
  return compactRecord({
    limit: options.limit,
    direction: options.direction,
    cursor: options.cursor,
    start: options.start,
    end: options.end,
    start_serial: options.startSerial,
    end_serial: options.endSerial,
    start_time_ms: options.startTimeMs,
    end_time_ms: options.endTimeMs,
    until_attach: options.untilAttach,
  });
}

function normalizeAck(value: unknown): MessageAck {
  const record = objectRecord(value);
  const messageSerial =
    readString(record?.messageSerial) ?? readString(record?.message_serial);
  const historySerial =
    readSerial(record?.historySerial) ?? readSerial(record?.history_serial);
  if (!messageSerial || historySerial === undefined) {
    throw new ErrorInfo({
      code: ErrorCode.TransportSendFailed,
      message: "unable to normalize acknowledgement; missing messageSerial or historySerial",
      detail: value,
    });
  }
  const ack: MessageAck = {
    messageSerial,
    historySerial,
  };
  assignOptional(
    ack,
    "deliverySerial",
    readSerial(record?.deliverySerial) ?? readSerial(record?.delivery_serial),
  );
  assignOptional(
    ack,
    "versionSerial",
    readString(record?.versionSerial) ?? readString(record?.version_serial),
  );
  assignOptional(ack, "status", readString(record?.status));
  return ack;
}

function normalizeAction(
  raw: SockudoRawMessage,
  mutableInfo: SockudoMutableMessageInfo | null | undefined,
): InboundMessageAction {
  const action =
    mutableInfo?.action ??
    readString((raw as Record<string, unknown>).action) ??
    readString(objectRecord(raw.data)?.action) ??
    raw.event.replace(/^sockudo:/, "").replace(/^pusher:/, "");
  switch (action) {
    case "message.append":
      return "append";
    case "message.update":
      return "update";
    case "message.delete":
      return "delete";
    case "message.summary":
      return "summary";
    case "message.create":
    default:
      return "create";
  }
}

function normalizeVersion(
  raw: Record<string, unknown>,
  mutableInfo: SockudoMutableMessageInfo | null | undefined,
): InboundMessageVersion | undefined {
  const source = objectRecord(raw.version);
  const version: InboundMessageVersion = {};
  assignOptional(
    version,
    "serial",
    readString(source?.serial) ??
      readString(raw.version_serial) ??
      readString(raw.versionSerial) ??
      mutableInfo?.versionSerial,
  );
  assignOptional(
    version,
    "clientId",
    readString(source?.client_id) ?? readString(source?.clientId),
  );
  assignOptional(
    version,
    "timestamp",
    readNumber(source?.timestamp_ms) ??
      readNumber(source?.timestampMs) ??
      mutableInfo?.versionTimestampMs,
  );
  assignOptional(version, "description", readString(source?.description));
  assignOptional(version, "metadata", source?.metadata);
  return Object.keys(version).length > 0 ? version : undefined;
}

function adaptPresence(channel: SockudoChannelPeer): PresenceLike {
  const presence = {
    async enter(data?: unknown): Promise<void> {
      try {
        if (channel.enter) {
          await channel.enter(data);
          return;
        }
        if (channel.members) {
          return;
        }
        throw new ErrorInfo({
          code: ErrorCode.InvalidArgument,
          message: "unable to enter presence; channel is not a presence channel",
        });
      } catch (error) {
        throw mapSockudoFailure(error, "enter presence");
      }
    },
    async update(data?: unknown): Promise<void> {
      try {
        if (!channel.update) {
          throw new ErrorInfo({
            code: ErrorCode.InvalidArgument,
            message: "unable to update presence; channel does not expose presence.update",
          });
        }
        await channel.update(data);
      } catch (error) {
        throw mapSockudoFailure(error, "update presence");
      }
    },
    async leave(data?: unknown): Promise<void> {
      try {
        if (channel.leave) {
          await channel.leave(data);
          return;
        }
        void data;
      } catch (error) {
        throw mapSockudoFailure(error, "leave presence");
      }
    },
    async get(): Promise<readonly PresenceMember[]> {
      const members = channel.members;
      if (!members) {
        return [];
      }
      return snapshotMembers(members);
    },
    subscribe(
      listener: (event: PresenceEventName, member: PresenceMember) => void,
    ): Unsubscribe {
      const added = (member: unknown): void => listener("enter", normalizeMember(member));
      const updated = (member: unknown): void => listener("update", normalizeMember(member));
      const removed = (member: unknown): void => listener("leave", normalizeMember(member));
      channel.bind?.("sockudo:member_added", added);
      channel.bind?.("pusher:member_added", added);
      channel.bind?.("member_added", added);
      channel.bind?.("sockudo:member_updated", updated);
      channel.bind?.("sockudo:presence_update", updated);
      channel.bind?.("member_updated", updated);
      channel.bind?.("presence_update", updated);
      channel.bind?.("sockudo:member_removed", removed);
      channel.bind?.("pusher:member_removed", removed);
      channel.bind?.("member_removed", removed);
      return () => {
        channel.unbind?.("sockudo:member_added", added);
        channel.unbind?.("pusher:member_added", added);
        channel.unbind?.("member_added", added);
        channel.unbind?.("sockudo:member_updated", updated);
        channel.unbind?.("sockudo:presence_update", updated);
        channel.unbind?.("member_updated", updated);
        channel.unbind?.("presence_update", updated);
        channel.unbind?.("sockudo:member_removed", removed);
        channel.unbind?.("pusher:member_removed", removed);
        channel.unbind?.("member_removed", removed);
      };
    },
  };
  return presence;
}

function snapshotMembers(members: SockudoMembersPeer): readonly PresenceMember[] {
  const snapshot: PresenceMember[] = [];
  if (typeof members.each === "function") {
    members.each((member) => {
      snapshot.push(normalizeMember(member));
    });
    return snapshot;
  }
  const rawMembers = objectRecord(members.members);
  if (!rawMembers) {
    return snapshot;
  }
  for (const [id, data] of Object.entries(rawMembers)) {
    snapshot.push({ id, data });
  }
  return snapshot;
}

function normalizeMember(member: unknown): PresenceMember {
  const record = objectRecord(member);
  return {
    id:
      readString(record?.id) ??
      readString(record?.user_id) ??
      readString(record?.clientId) ??
      "",
    data: record && "info" in record ? record.info : record?.user_info,
  };
}

function mapSockudoFailure(error: unknown, operation: string): ErrorInfo {
  const info = toErrorInfo(error, {
    code: ErrorCode.TransportSendFailed,
    message: `unable to ${operation}; Sockudo client operation failed`,
  });
  if (info.code === 93002) {
    return new ErrorInfo({
      code: ErrorCode.TransportSendFailed,
      message: `unable to ${operation}; ${info.message}`,
      cause: error,
      detail: { sockudoCode: info.code, sockudoDetail: info.detail },
      statusCode: info.statusCode,
    });
  }
  return info;
}

function normalizeSockudoClientOptions(
  options: CreateSockudoRealtimeClientOptions,
): Record<string, unknown> {
  const clientOptions = { ...(options.clientOptions ?? {}) };
  if (options.appendRollupWindow !== undefined) {
    validateAppendRollupWindow(options.appendRollupWindow);
    const existing = objectRecord(clientOptions.transportParams);
    clientOptions.transportParams = {
      ...existing,
      append_rollup_window: options.appendRollupWindow,
    };
  }
  return clientOptions;
}

async function loadSockudoClientConstructor(): Promise<
  new (appKey: string, options: Record<string, unknown>) => SockudoClientPeer
> {
  const dynamicImport = new Function(
    "specifier",
    "return import(specifier)",
  ) as (specifier: string) => Promise<unknown>;
  const module = objectRecord(await dynamicImport("@sockudo/client"));
  const constructor = module?.default;
  if (typeof constructor !== "function") {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message:
        "unable to create realtime client; @sockudo/client did not provide a default constructor",
    });
  }
  return constructor as new (
    appKey: string,
    options: Record<string, unknown>,
  ) => SockudoClientPeer;
}

function readTierValue(
  extras: unknown,
  tier: "transport" | "codec",
  key: string,
): string | undefined {
  const ai = objectRecord(objectRecord(extras)?.ai);
  const headers = objectRecord(ai?.[tier]);
  return readString(headers?.[key]);
}

function readSockudoHeaderSerial(
  extras: unknown,
  key: string,
): Serial | undefined {
  const headers = objectRecord(objectRecord(extras)?.headers);
  return readSerial(headers?.[key]);
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

function readNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function readSerial(value: unknown): Serial | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) ? parsed : value;
  }
  return undefined;
}

function serialToBigInt(value: Serial): bigint | undefined {
  if (typeof value === "number") {
    return Number.isFinite(value) && Number.isInteger(value)
      ? BigInt(value)
      : undefined;
  }
  return /^\d+$/.test(value) ? BigInt(value) : undefined;
}

function compactRecord(
  source: Record<string, unknown>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(source)) {
    if (value !== undefined) {
      result[key] = value;
    }
  }
  return result;
}

function objectRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : undefined;
}

function assignOptional<T extends object, K extends keyof T>(
  target: T,
  key: K,
  value: T[K] | undefined,
): void {
  if (value !== undefined) {
    target[key] = value;
  }
}

interface ChannelAdapterState {
  listeners: Set<MessageListener>;
  events: TinyEmitter<ChannelEvents>;
  mutableMessageInfoReader?: MutableMessageInfoReader;
}

interface ClientAdapterState {
  channels: Map<string, ChannelLike>;
  mutableMessageInfoReader?: MutableMessageInfoReader;
}

interface SockudoMembersPeer {
  members?: unknown;
  each?(listener: (member: unknown) => void): void;
}

class TinyEmitter<Events extends Record<string, unknown>> {
  private readonly listeners = new Map<keyof Events, Set<(payload: never) => void>>();

  public on<K extends keyof Events>(
    event: K,
    listener: (payload: Events[K]) => void,
  ): Unsubscribe {
    let listeners = this.listeners.get(event);
    if (!listeners) {
      listeners = new Set();
      this.listeners.set(event, listeners);
    }
    listeners.add(listener as (payload: never) => void);
    return () => {
      listeners?.delete(listener as (payload: never) => void);
    };
  }

  public emit<K extends keyof Events>(event: K, payload: Events[K]): void {
    const listeners = this.listeners.get(event);
    if (!listeners) {
      return;
    }
    for (const listener of listeners) {
      listener(payload as never);
    }
  }
}
