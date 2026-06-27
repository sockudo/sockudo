import {
  EVENT_AI_OUTPUT,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_EVENT_ID,
  HEADER_ROLE,
} from "../constants.js";
import { ErrorCode, ErrorInfo, toErrorInfo } from "../errors.js";
import { getCodecHeaders, getTransportHeaders, type HeaderMap } from "../utils.js";
import { RealtimeEmitter } from "./emitter.js";
import type {
  AppendRollupWindow,
  ChannelEvents,
  ChannelLike,
  ClientLike,
  GetChannelOptions,
  HistoryOptions,
  InboundMessage,
  InboundMessageAction,
  InboundMessageVersion,
  MessageAck,
  MessageListener,
  MessageMutation,
  PaginatedResult,
  PresenceLike,
  PresenceMember,
  PublishMessage,
  RewindOption,
  Serial,
  SubscribeOptions,
  Unsubscribe,
} from "./types.js";

/** Mutable-message information returned by `@sockudo/client`. */
export interface SockudoMutableMessageInfo {
  /** Sockudo action such as `message.append`. */
  action: string;
  /** Delivered event name. */
  event: string;
  /** Stable logical message serial. */
  messageSerial: string;
  /** Version serial. */
  versionSerial?: string;
  /** Durable history serial. */
  historySerial?: Serial;
  /** Version timestamp in milliseconds. */
  versionTimestampMs?: number;
}

/** Compatible signature for `@sockudo/client` `getMutableMessageInfo`. */
export type MutableMessageInfoReader = (
  event: Pick<SockudoRawMessage, "event" | "extras">,
) => SockudoMutableMessageInfo | null;

/** Raw Sockudo message shape consumed by the adapter. */
export interface SockudoRawMessage {
  /** Delivered event name. */
  event: string;
  /** Delivered channel name. */
  channel?: string;
  /** Opaque payload. */
  data?: unknown;
  /** Logical message name. */
  name?: string;
  /** Verified user/client identity. */
  user_id?: string;
  /** Recovery stream identity. */
  stream_id?: string;
  /** Idempotent message id. */
  message_id?: string;
  /** Delivery serial. */
  serial?: Serial;
  /** Sockudo extras. */
  extras?: unknown;
  /** Additive future fields. */
  [key: string]: unknown;
}

/** Structural subset required from a Sockudo channel object. */
export interface SockudoChannelPeer {
  /** Channel name. */
  name: string;
  /** Attach serial captured by Sockudo on subscribe. */
  attachSerial?: Serial;
  /** Publish-create helper. */
  publishCreate?(message: Record<string, unknown>): Promise<unknown>;
  /** Append helper. */
  appendMessage?(
    messageSerial: string,
    data: string,
    options?: Record<string, unknown>,
  ): Promise<unknown>;
  /** Update helper. */
  updateMessage?(messageSerial: string, options?: Record<string, unknown>): Promise<unknown>;
  /** Delete helper. */
  deleteMessage?(messageSerial: string, options?: Record<string, unknown>): Promise<unknown>;
  /** Channel history helper. */
  channelHistory?(params?: Record<string, unknown>): Promise<unknown>;
  /** Event binding helper. */
  bind?(event: string, listener: (...args: readonly unknown[]) => void): unknown;
  /** Event unbinding helper. */
  unbind?(event?: string, listener?: (...args: readonly unknown[]) => void): unknown;
  /** Global channel event binding helper. */
  bind_global?(listener: (...args: readonly unknown[]) => void): unknown;
  /** Raw event handler. */
  handleEvent?(event: SockudoRawMessage): unknown;
  /** Presence member store when this is a presence channel. */
  members?: SockudoMembersPeer;
  /** Presence update helper. */
  update?(data?: unknown): Promise<void>;
  /** Presence enter helper. */
  enter?(data?: unknown): Promise<void>;
  /** Presence leave helper. */
  leave?(data?: unknown): Promise<void>;
}

/** Structural subset required from a Sockudo client object. */
export interface SockudoClientPeer {
  /** Channel registry. */
  channels?: {
    /** Adds or returns a channel. */
    add?(name: string, client: SockudoClientPeer): SockudoChannelPeer;
    /** Finds a channel. */
    find?(name: string): SockudoChannelPeer | undefined;
  };
  /** Connection state. */
  connection?: {
    /** Current state. */
    state?: string;
    /** Verified server identity. */
    clientId?: string;
  };
  /** Subscribes to a channel. */
  subscribe?(name: string, options?: Record<string, unknown>): SockudoChannelPeer;
  /** Finds a channel. */
  channel?(name: string): SockudoChannelPeer | undefined;
  /** Binds a global client event. */
  bind?(event: string, listener: (payload: unknown) => void): unknown;
  /** Unbinds a global client event. */
  unbind?(event?: string, listener?: (payload: unknown) => void): unknown;
  /** Returns verified identity. */
  verifiedClientId?(): string | undefined;
  /** Disconnects the client. */
  disconnect?(): void;
}

/** Options for adapting a Sockudo client. */
export interface AdaptSockudoClientOptions {
  /** Mutable-message helper from `@sockudo/client`. */
  mutableMessageInfoReader?: MutableMessageInfoReader;
}

/** Options for adapting a Sockudo channel. */
export interface AdaptSockudoChannelOptions extends AdaptSockudoClientOptions {
  /** Optional parent client for subscribe/recovery passthrough. */
  client?: SockudoClientPeer;
}

/** Options for creating a Sockudo realtime client through the peer dependency. */
export interface CreateSockudoRealtimeClientOptions extends AdaptSockudoClientOptions {
  /** Options passed to `new Sockudo(appKey, options)`. */
  clientOptions?: Record<string, unknown>;
  /** Append delivery mode passed to `@sockudo/client` for V2 sockets. */
  appendMode?: "delta" | "full";
  /** Append rollup window passed as `transportParams.append_rollup_window`. */
  appendRollupWindow?: AppendRollupWindow;
}

const allowedRollupWindows = new Set<number>([0, 20, 40, 100, 500]);
const channelStates = new WeakMap<SockudoChannelPeer, ChannelState>();
const clientStates = new WeakMap<SockudoClientPeer, ClientState>();

/** Creates a `ClientLike` by dynamically loading the `@sockudo/client` peer. */
export async function createSockudoRealtimeClient(
  appKey: string,
  options: CreateSockudoRealtimeClientOptions = {},
): Promise<ClientLike> {
  const Sockudo = await loadSockudoConstructor();
  return adaptSockudoClient(new Sockudo(appKey, normalizeClientOptions(options)), options);
}

/** Adapts an existing Sockudo client into the realtime seam. */
export function adaptSockudoClient(
  client: SockudoClientPeer,
  options: AdaptSockudoClientOptions = {},
): ClientLike {
  const state = ensureClientState(client, options);
  return {
    channels: {
      get(name: string, getOptions?: GetChannelOptions): ChannelLike {
        const channel = getOrCreateChannel(client, name, getOptions?.params?.rewind);
        const cached = state.channels.get(name);
        if (cached) {
          return cached;
        }
        const adapted = adaptSockudoChannel(channel, { ...options, client });
        state.channels.set(name, adapted);
        return adapted;
      },
    },
    connection: {
      get state(): string {
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

/** Adapts an existing Sockudo channel into the realtime seam. */
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
      return callAck("publish a message", () => {
        if (!channel.publishCreate) {
          throw missingMethod("publish a message");
        }
        return channel.publishCreate(
          compact({
            name: message.name,
            data: message.data,
            extras: message.extras,
            messageSerial: message.messageSerial,
            messageId: message.messageId,
            opId: message.opId,
            clientId: message.clientId,
            socketId: message.socketId,
          }),
        );
      });
    },
    appendMessage(
      messageSerial: string,
      data: string,
      mutation: Omit<MessageMutation, "data"> = {},
    ): Promise<MessageAck> {
      return callAck("append to a message", () => {
        if (!channel.appendMessage) {
          throw missingMethod("append to a message");
        }
        return channel.appendMessage(messageSerial, data, normalizeMutation(mutation));
      });
    },
    updateMessage(messageSerial: string, mutation: MessageMutation = {}): Promise<MessageAck> {
      return callAck("update a message", () => {
        if (!channel.updateMessage) {
          throw missingMethod("update a message");
        }
        return channel.updateMessage(messageSerial, normalizeMutation(mutation));
      });
    },
    deleteMessage(messageSerial: string, mutation: MessageMutation = {}): Promise<MessageAck> {
      return callAck("delete a message", () => {
        if (!channel.deleteMessage) {
          throw missingMethod("delete a message");
        }
        return channel.deleteMessage(messageSerial, normalizeMutation(mutation));
      });
    },
    subscribe(listener: MessageListener, subscribeOptions?: SubscribeOptions): Unsubscribe {
      if (subscribeOptions?.rewind !== undefined) {
        options.client?.subscribe?.(channel.name, {
          rewind: subscribeOptions.rewind,
        });
      }
      const names = subscribeOptions?.names ? new Set(subscribeOptions.names) : undefined;
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
    async history(historyOptions: HistoryOptions = {}): Promise<PaginatedResult<InboundMessage>> {
      if (!channel.channelHistory) {
        throw new ErrorInfo({
          code: ErrorCode.InvalidArgument,
          message: "unable to read channel history; Sockudo channel does not expose channelHistory",
        });
      }
      try {
        const page = await channel.channelHistory(normalizeHistoryOptions(historyOptions));
        return normalizeHistoryPage(page, state.mutableMessageInfoReader);
      } catch (error) {
        throw mapFailure(error, "read channel history");
      }
    },
    on<K extends keyof ChannelEvents>(
      event: K,
      listener: (payload: ChannelEvents[K]) => void,
    ): Unsubscribe {
      return state.events.on(event, listener);
    },
  };
}

/** Normalizes one raw Sockudo message into the seam message shape. */
export function normalizeInboundMessage(
  raw: SockudoRawMessage,
  mutableMessageInfoReader?: MutableMessageInfoReader,
): InboundMessage {
  const info = mutableMessageInfoReader?.({
    event: raw.event,
    extras: raw.extras,
  });
  const record = raw as Record<string, unknown>;
  const version = normalizeVersion(record, info);
  let transportHeaders: HeaderMap | undefined;
  let codecHeaders: HeaderMap | undefined;
  const message: InboundMessage = {
    name: readString(record.name) ?? inferAiMutableName(raw) ?? raw.event,
    data: raw.data,
    action: normalizeAction(raw, info),
    messageSerial:
      readString(record.message_serial) ??
      readString(record.messageSerial) ??
      info?.messageSerial ??
      readString(raw.message_id) ??
      readAiHeader(raw.extras, "transport", HEADER_CODEC_MESSAGE_ID) ??
      readAiHeader(raw.extras, "transport", HEADER_EVENT_ID) ??
      "",
    historySerial:
      readSerial(record.history_serial) ??
      readSerial(record.historySerial) ??
      info?.historySerial ??
      readExtrasHeaderSerial(raw.extras, "sockudo_history_serial") ??
      readSerial(raw.serial) ??
      "0",
    timestamp:
      readNumber(record.timestamp) ??
      readNumber(record.timestamp_ms) ??
      readNumber(record.timestampMs) ??
      info?.versionTimestampMs ??
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
  setOptional(
    message,
    "deliverySerial",
    readSerial(record.delivery_serial) ??
      readSerial(record.deliverySerial) ??
      readSerial(raw.serial),
  );
  setOptional(message, "version", version);
  setOptional(
    message,
    "clientId",
    readString(record.client_id) ??
      readString(record.clientId) ??
      readString(raw.user_id) ??
      version?.clientId,
  );
  setOptional(message, "messageId", readString(raw.message_id));
  setOptional(message, "extras", raw.extras);
  return message;
}

/** Compares serials without truncating unsafe integer strings. */
export function compareSerial(a: Serial, b: Serial): -1 | 0 | 1 {
  const left = serialBigInt(a);
  const right = serialBigInt(b);
  if (left !== undefined && right !== undefined) {
    return left < right ? -1 : left > right ? 1 : 0;
  }
  return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
}

/** Returns whether serial `a` is less than or equal to serial `b`. */
export function serialLessThanOrEqual(a: Serial, b: Serial): boolean {
  return compareSerial(a, b) <= 0;
}

/** Validates an append rollup window. */
export function validateAppendRollupWindow(value: unknown): asserts value is AppendRollupWindow {
  if (typeof value !== "number" || !Number.isInteger(value) || !allowedRollupWindows.has(value)) {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message:
        "unable to create realtime client; appendRollupWindow must be one of 0, 20, 40, 100, or 500",
    });
  }
}

/** Validates an append delivery mode. */
export function validateAppendMode(value: unknown): asserts value is "delta" | "full" {
  if (value !== "delta" && value !== "full") {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message: "unable to create realtime client; appendMode must be delta or full",
    });
  }
}

function ensureClientState(
  client: SockudoClientPeer,
  options: AdaptSockudoClientOptions,
): ClientState {
  const existing = clientStates.get(client);
  if (existing) {
    return existing;
  }
  const state: ClientState = {
    channels: new Map(),
    mutableMessageInfoReader: options.mutableMessageInfoReader,
  };
  clientStates.set(client, state);
  bindRecovery(client, state);
  return state;
}

function ensureChannelState(
  channel: SockudoChannelPeer,
  options: AdaptSockudoChannelOptions,
): ChannelState {
  const existing = channelStates.get(channel);
  if (existing) {
    return existing;
  }
  const state: ChannelState = {
    listeners: new Set(),
    events: new RealtimeEmitter<ChannelEvents>(),
    mutableMessageInfoReader: options.mutableMessageInfoReader,
  };
  channelStates.set(channel, state);
  const original = channel.handleEvent?.bind(channel);
  if (original) {
    channel.handleEvent = (event: SockudoRawMessage): unknown => {
      dispatch(state, event);
      return original(event);
    };
  } else {
    channel.bind_global?.((eventName, data) => {
      if (typeof eventName === "string") {
        dispatch(state, { event: eventName, channel: channel.name, data });
      }
    });
  }
  bindChannelStates(channel, state);
  return state;
}

function bindRecovery(client: SockudoClientPeer, state: ClientState): void {
  const handler = (payload: unknown): void => {
    const record = asRecord(payload);
    const channelName = readString(record?.channel);
    const code = readString(record?.code);
    if (!channelName || (code !== "stream_reset" && code !== "position_expired")) {
      return;
    }
    const channel = client.channel?.(channelName) ?? client.channels?.find?.(channelName);
    const channelState = channel ? channelStates.get(channel) : undefined;
    const error = new ErrorInfo({
      code: ErrorCode.ChannelContinuityLost,
      message: `unable to maintain channel continuity; ${code}`,
      statusCode: 409,
      detail: payload,
    });
    channelState?.events.emit("continuity_lost", error);
    void state.channels.get(channelName);
  };
  client.bind?.("sockudo:resume_failed", handler);
  client.bind?.("pusher:resume_failed", handler);
  client.bind?.("resume_failed", handler);
}

function bindChannelStates(channel: SockudoChannelPeer, state: ChannelState): void {
  const attached = (): void => {
    state.events.emit("attached", undefined);
  };
  const failed = (payload: unknown): void => {
    state.events.emit(
      "failed",
      toErrorInfo(payload, {
        code: ErrorCode.TransportSubscriptionError,
        message: "unable to subscribe to channel; Sockudo reported an error",
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

function getOrCreateChannel(
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
  if (added) {
    return added;
  }
  throw new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    message: "unable to get channel; Sockudo client does not expose subscribe or channels.add",
  });
}

function dispatch(state: ChannelState, raw: SockudoRawMessage): void {
  if (state.listeners.size === 0) {
    return;
  }
  if (!isDeliverableRawMessage(raw)) {
    return;
  }
  const message = normalizeInboundMessage(raw, state.mutableMessageInfoReader);
  for (const listener of state.listeners) {
    listener(message);
  }
}

async function callAck(operation: string, run: () => Promise<unknown>): Promise<MessageAck> {
  try {
    return normalizeAck(await run());
  } catch (error) {
    throw mapFailure(error, operation);
  }
}

function missingMethod(operation: string): ErrorInfo {
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    message: `unable to ${operation}; Sockudo channel does not expose this method`,
  });
}

function normalizeAck(value: unknown): MessageAck {
  const record = asRecord(value);
  const messageSerial = readString(record?.messageSerial) ?? readString(record?.message_serial);
  const historySerial = readSerial(record?.historySerial) ?? readSerial(record?.history_serial);
  if (!messageSerial || historySerial === undefined) {
    throw new ErrorInfo({
      code: ErrorCode.TransportSendFailed,
      message: "unable to normalize acknowledgement; missing messageSerial or historySerial",
      detail: value,
    });
  }
  const ack: MessageAck = { messageSerial, historySerial };
  setOptional(
    ack,
    "deliverySerial",
    readSerial(record?.deliverySerial) ?? readSerial(record?.delivery_serial),
  );
  setOptional(
    ack,
    "versionSerial",
    readString(record?.versionSerial) ?? readString(record?.version_serial),
  );
  setOptional(ack, "status", readString(record?.status));
  return ack;
}

function normalizeHistoryPage(
  page: unknown,
  mutableMessageInfoReader?: MutableMessageInfoReader,
): PaginatedResult<InboundMessage> {
  const record = asRecord(page);
  const rawItems = Array.isArray(record?.items) ? record.items : [];
  return {
    items: rawItems
      .map((item) => normalizeRawMessage(item))
      .filter(isDeliverableRawMessage)
      .map((item) => normalizeInboundMessage(item, mutableMessageInfoReader)),
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
      return normalizeHistoryPage(
        await (next as () => Promise<unknown>).call(page),
        mutableMessageInfoReader,
      );
    },
  };
}

function normalizeRawMessage(value: unknown): SockudoRawMessage {
  const record = asRecord(value) ?? {};
  const nested = asRecord(record.message);
  const source = nested ?? record;
  return {
    ...source,
    event:
      readString(source.event) ?? readString(source.name) ?? readString(record.event_name) ?? "",
  };
}

function normalizeHistoryOptions(options: HistoryOptions): Record<string, unknown> {
  return compact({
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

function normalizeMutation(
  mutation: MessageMutation | Omit<MessageMutation, "data">,
): Record<string, unknown> {
  return compact({
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

function inferAiMutableName(raw: SockudoRawMessage): string | undefined {
  if (!raw.event.startsWith("sockudo:message.")) {
    return undefined;
  }
  const transport = getTransportHeaders(raw.extras);
  const codec = getCodecHeaders(raw.extras);
  return transport[HEADER_ROLE] === "assistant" || Object.keys(codec).length > 0
    ? EVENT_AI_OUTPUT
    : undefined;
}

function normalizeAction(
  raw: SockudoRawMessage,
  info: SockudoMutableMessageInfo | null | undefined,
): InboundMessageAction {
  const explicitAction =
    info?.action ??
    readString((raw as Record<string, unknown>).action) ??
    readString(asRecord(raw.data)?.action);
  if (explicitAction !== undefined) {
    return normalizeKnownAction(explicitAction) ?? "summary";
  }
  const eventAction = raw.event.replace(/^sockudo:/, "").replace(/^pusher:/, "");
  return normalizeKnownAction(eventAction) ?? (isInternalEvent(raw.event) ? "summary" : "create");
}

function normalizeKnownAction(action: string): InboundMessageAction | undefined {
  switch (action) {
    case "message.create":
      return "create";
    case "message.append":
      return "append";
    case "message.update":
      return "update";
    case "message.delete":
      return "delete";
    case "message.summary":
      return "summary";
    default:
      return undefined;
  }
}

function isInternalEvent(event: string): boolean {
  return (
    event.startsWith("sockudo:") ||
    event.startsWith("sockudo_internal:") ||
    event.startsWith("pusher:") ||
    event.startsWith("pusher_internal:")
  );
}

function normalizeVersion(
  raw: Record<string, unknown>,
  info: SockudoMutableMessageInfo | null | undefined,
): InboundMessageVersion | undefined {
  const source = asRecord(raw.version);
  const version: InboundMessageVersion = {};
  setOptional(
    version,
    "serial",
    readString(source?.serial) ??
      readString(raw.version_serial) ??
      readString(raw.versionSerial) ??
      info?.versionSerial,
  );
  setOptional(version, "clientId", readString(source?.client_id) ?? readString(source?.clientId));
  setOptional(
    version,
    "timestamp",
    readNumber(source?.timestamp_ms) ?? readNumber(source?.timestampMs) ?? info?.versionTimestampMs,
  );
  setOptional(version, "description", readString(source?.description));
  setOptional(version, "metadata", source?.metadata);
  return Object.keys(version).length === 0 ? undefined : version;
}

function adaptPresence(channel: SockudoChannelPeer): PresenceLike {
  return {
    async enter(data?: unknown): Promise<void> {
      if (channel.enter) {
        await channel.enter(data);
      }
    },
    async update(data?: unknown): Promise<void> {
      if (!channel.update) {
        throw new ErrorInfo({
          code: ErrorCode.InvalidArgument,
          message: "unable to update presence; Sockudo channel does not expose presence.update",
        });
      }
      await channel.update(data);
    },
    async leave(data?: unknown): Promise<void> {
      await channel.leave?.(data);
    },
    get(): Promise<readonly PresenceMember[]> {
      return Promise.resolve(snapshotMembers(channel.members));
    },
    subscribe(listener): Unsubscribe {
      const enter = (member: unknown): void => {
        const normalized = normalizeMember(member);
        if (normalized.id) {
          listener("enter", normalized);
        }
      };
      const update = (member: unknown): void => {
        const normalized = normalizeMember(member);
        if (normalized.id) {
          listener("update", normalized);
        }
      };
      const leave = (member: unknown): void => {
        const normalized = normalizeMember(member);
        if (normalized.id) {
          listener("leave", normalized);
        }
      };
      channel.bind?.("member_added", enter);
      channel.bind?.("sockudo:member_added", enter);
      channel.bind?.("member_updated", update);
      channel.bind?.("presence_update", update);
      channel.bind?.("sockudo:presence_update", update);
      channel.bind?.("member_removed", leave);
      channel.bind?.("sockudo:member_removed", leave);
      return () => {
        channel.unbind?.("member_added", enter);
        channel.unbind?.("sockudo:member_added", enter);
        channel.unbind?.("member_updated", update);
        channel.unbind?.("presence_update", update);
        channel.unbind?.("sockudo:presence_update", update);
        channel.unbind?.("member_removed", leave);
        channel.unbind?.("sockudo:member_removed", leave);
      };
    },
  };
}

function snapshotMembers(members: SockudoMembersPeer | undefined): readonly PresenceMember[] {
  if (!members) {
    return [];
  }
  const snapshot: PresenceMember[] = [];
  if (members.each) {
    members.each((member) => {
      const normalized = normalizeMember(member);
      if (normalized.id) {
        snapshot.push(normalized);
      }
    });
    return snapshot;
  }
  const rawMembers = asRecord(members.members);
  if (!rawMembers) {
    return snapshot;
  }
  for (const [id, data] of Object.entries(rawMembers)) {
    if (id) {
      snapshot.push({ id, data });
    }
  }
  return snapshot;
}

function normalizeMember(member: unknown): PresenceMember {
  const record = asRecord(member);
  return {
    id: readString(record?.id) ?? readString(record?.user_id) ?? readString(record?.clientId) ?? "",
    data: record && "info" in record ? record.info : record?.user_info,
  };
}

function mapFailure(error: unknown, operation: string): ErrorInfo {
  const mapped = toErrorInfo(error, {
    code: ErrorCode.TransportSendFailed,
    message: `unable to ${operation}; Sockudo client operation failed`,
  });
  if (mapped.code === 93002) {
    return new ErrorInfo({
      code: ErrorCode.TransportSendFailed,
      message: `unable to ${operation}; ${mapped.message}`,
      cause: error,
      statusCode: mapped.statusCode,
      detail: { sockudoCode: mapped.code, sockudoDetail: mapped.detail },
    });
  }
  return mapped;
}

function normalizeClientOptions(
  options: CreateSockudoRealtimeClientOptions,
): Record<string, unknown> {
  const clientOptions: Record<string, unknown> = Object.assign({}, options.clientOptions);
  if (options.appendMode !== undefined) {
    validateAppendMode(options.appendMode);
    clientOptions.appendMode = options.appendMode;
  }
  if (options.appendRollupWindow !== undefined) {
    validateAppendRollupWindow(options.appendRollupWindow);
    const transportParams = asRecord(clientOptions.transportParams);
    clientOptions.transportParams = transportParams
      ? {
          ...transportParams,
          append_rollup_window: options.appendRollupWindow,
        }
      : {
          append_rollup_window: options.appendRollupWindow,
        };
  }
  return clientOptions;
}

async function loadSockudoConstructor(): Promise<
  new (appKey: string, options: Record<string, unknown>) => SockudoClientPeer
> {
  const specifier = "@sockudo/client";
  const module = asRecord(await import(/* @vite-ignore */ specifier));
  if (typeof module?.default !== "function") {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message: "unable to create realtime client; @sockudo/client did not provide a constructor",
    });
  }
  return module.default as new (
    appKey: string,
    options: Record<string, unknown>,
  ) => SockudoClientPeer;
}

function readAiHeader(
  extras: unknown,
  tier: "transport" | "codec",
  key: string,
): string | undefined {
  return readString(asRecord(asRecord(asRecord(extras)?.ai)?.[tier])?.[key]);
}

function readExtrasHeaderSerial(extras: unknown, key: string): Serial | undefined {
  return readSerial(asRecord(asRecord(extras)?.headers)?.[key]);
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

function readNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function readSerial(value: unknown): Serial | undefined {
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) ? parsed : value;
  }
  return undefined;
}

function serialBigInt(value: Serial): bigint | undefined {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? BigInt(value) : undefined;
  }
  return /^\d+$/.test(value) ? BigInt(value) : undefined;
}

function compact(source: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(source)) {
    if (value !== undefined) {
      result[key] = value;
    }
  }
  return result;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : undefined;
}

function isDeliverableRawMessage(raw: SockudoRawMessage): boolean {
  return typeof raw.event === "string" && raw.event !== "";
}

function setOptional<T extends object, K extends keyof T>(
  target: T,
  key: K,
  value: T[K] | undefined,
): void {
  if (value !== undefined) {
    target[key] = value;
  }
}

interface ChannelState {
  listeners: Set<MessageListener>;
  events: RealtimeEmitter<ChannelEvents>;
  mutableMessageInfoReader: MutableMessageInfoReader | undefined;
}

interface ClientState {
  channels: Map<string, ChannelLike>;
  mutableMessageInfoReader: MutableMessageInfoReader | undefined;
}

interface SockudoMembersPeer {
  members?: unknown;
  each?(listener: (member: unknown) => void): void;
}
