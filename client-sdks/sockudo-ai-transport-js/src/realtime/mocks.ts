import { ErrorCode, ErrorInfo } from "../errors.js";
import { RealtimeEmitter } from "./emitter.js";
import { normalizeInboundMessage } from "./adapter.js";
import type {
  ChannelEvents,
  ChannelLike,
  ClientLike,
  HistoryOptions,
  InboundMessage,
  MessageAck,
  MessageListener,
  MessageMutation,
  PaginatedResult,
  PresenceEventName,
  PresenceLike,
  PresenceMember,
  PublishMessage,
  Serial,
  SubscribeOptions,
  Unsubscribe,
} from "./types.js";
import type { SockudoRawMessage } from "./adapter.js";

/** Deterministic providers used by the in-memory realtime mock. */
export interface MockRealtimeProviders {
  /** Returns the next message serial. */
  messageSerial?(): string;
  /** Returns the next history serial. */
  historySerial?(): Serial;
  /** Returns the next delivery serial. */
  deliverySerial?(): Serial;
  /** Returns the next version serial. */
  versionSerial?(): string;
  /** Returns the current timestamp in milliseconds. */
  now?(): number;
}

/** Options for {@link createMockClient}. */
export interface MockClientOptions {
  /** Verified client identity. */
  clientId?: string;
  /** Deterministic providers. */
  providers?: MockRealtimeProviders;
}

/** In-memory `ClientLike` test double with scripted serials and recovery injection. */
export class MockClient implements ClientLike {
  private readonly channelMap = new Map<string, MockChannel>();
  private readonly providers: Required<MockRealtimeProviders>;
  private closed = false;

  /** Creates a mock client. */
  public constructor(options: MockClientOptions = {}) {
    let messageCounter = 0;
    let historyCounter = 0;
    let deliveryCounter = 0;
    let versionCounter = 0;
    this.providers = {
      messageSerial: options.providers?.messageSerial
        ? () => options.providers?.messageSerial?.() ?? ""
        : () => {
            messageCounter += 1;
            return `msg-${String(messageCounter)}`;
          },
      historySerial: options.providers?.historySerial
        ? () => options.providers?.historySerial?.() ?? 0
        : () => {
            historyCounter += 1;
            return historyCounter;
          },
      deliverySerial: options.providers?.deliverySerial
        ? () => options.providers?.deliverySerial?.() ?? 0
        : () => {
            deliveryCounter += 1;
            return deliveryCounter;
          },
      versionSerial: options.providers?.versionSerial
        ? () => options.providers?.versionSerial?.() ?? ""
        : () => {
            versionCounter += 1;
            return `ver-${String(versionCounter)}`;
          },
      now: options.providers?.now
        ? () => options.providers?.now?.() ?? 0
        : () => 0,
    };
    this.connection = {
      state: "connected",
      clientId: options.clientId,
    };
    this.channels = {
      get: (name: string): ChannelLike => this.getMockChannel(name),
    };
  }

  /** Channel registry. */
  public readonly channels: ClientLike["channels"];

  /** Mock connection state. */
  public readonly connection: {
    state: string;
    clientId: string | undefined;
  };

  /** Returns the concrete mock channel by name. */
  public getMockChannel(name: string): MockChannel {
    let channel = this.channelMap.get(name);
    if (!channel) {
      channel = new MockChannel(name, this.providers);
      this.channelMap.set(name, channel);
    }
    return channel;
  }

  /** Injects a continuity loss event for `channelName`. */
  public injectContinuityLost(
    channelName: string,
    code: "stream_reset" | "position_expired" = "position_expired",
  ): void {
    this.getMockChannel(channelName).injectContinuityLost(code);
  }

  /** Closes the mock client. */
  public close(): void {
    this.closed = true;
    this.connection.state = "disconnected";
  }

  /** Returns whether this client has been closed. */
  public isClosed(): boolean {
    return this.closed;
  }
}

/** In-memory `ChannelLike` test double. */
export class MockChannel implements ChannelLike {
  private readonly messages: SockudoRawMessage[] = [];
  private readonly listeners = new Set<MessageListener>();
  private readonly events = new RealtimeEmitter<ChannelEvents>();
  private readonly presenceState = new MockPresence();

  /** Attach serial captured by the mock subscription. */
  public attachSerial: Serial | undefined = undefined;

  /** Presence API for the channel. */
  public readonly presence: PresenceLike = this.presenceState;

  /** Creates a mock channel. */
  public constructor(
    /** Channel name. */
    public readonly name: string,
    private readonly providers: Required<MockRealtimeProviders>,
  ) {}

  /** Publishes a mock mutable create and dispatches it to subscribers. */
  public publish(message: PublishMessage): Promise<MessageAck> {
    const ack = this.nextAck(message.messageSerial);
    const raw = this.rawMessage(
      message.name ?? "",
      message.data,
      "message.create",
      ack,
      message.extras,
      message.messageId,
    );
    this.messages.push(raw);
    this.dispatch(raw);
    return Promise.resolve(ack);
  }

  /** Appends to a mock mutable message. */
  public appendMessage(
    messageSerial: string,
    data: string,
    options: Omit<MessageMutation, "data"> = {},
  ): Promise<MessageAck> {
    return this.mutate(messageSerial, "message.append", data, options.extras);
  }

  /** Updates a mock mutable message. */
  public updateMessage(
    messageSerial: string,
    options: MessageMutation = {},
  ): Promise<MessageAck> {
    return this.mutate(
      messageSerial,
      "message.update",
      options.data,
      options.extras,
    );
  }

  /** Deletes a mock mutable message. */
  public deleteMessage(
    messageSerial: string,
    options: MessageMutation = {},
  ): Promise<MessageAck> {
    return this.mutate(
      messageSerial,
      "message.delete",
      options.data,
      options.extras,
    );
  }

  /** Subscribes to mock message delivery. */
  public subscribe(
    listener: MessageListener,
    options?: SubscribeOptions,
  ): Unsubscribe {
    const names = options?.names ? new Set(options.names) : undefined;
    const wrapped = (message: InboundMessage): void => {
      if (!names || names.has(message.name)) {
        listener(message);
      }
    };
    const latest = this.messages[this.messages.length - 1];
    if (latest) {
      this.attachSerial = normalizeInboundMessage(latest).historySerial;
    }
    this.listeners.add(wrapped);
    return () => {
      this.listeners.delete(wrapped);
    };
  }

  /** Reads mock channel history. */
  public history(
    options: HistoryOptions = {},
  ): Promise<PaginatedResult<InboundMessage>> {
    const limit = options.limit ?? 100;
    const attachSerial = options.untilAttach ? this.attachSerial : undefined;
    const source =
      attachSerial === undefined
        ? this.messages
        : this.messages.filter((message) => {
            const normalized = normalizeInboundMessage(message);
            return serialCompare(normalized.historySerial, attachSerial) <= 0;
          });
    return Promise.resolve(pageFrom(source.slice(0, limit)));
  }

  /** Subscribes to mock channel events. */
  public on<K extends keyof ChannelEvents>(
    event: K,
    listener: (payload: ChannelEvents[K]) => void,
  ): Unsubscribe {
    return this.events.on(event, listener);
  }

  /** Injects an arbitrary raw message. */
  public inject(raw: SockudoRawMessage): void {
    this.messages.push(raw);
    this.dispatch(raw);
  }

  /** Injects continuity loss. */
  public injectContinuityLost(code: "stream_reset" | "position_expired"): void {
    this.events.emit(
      "continuity_lost",
      new ErrorInfo({
        code: ErrorCode.ChannelContinuityLost,
        statusCode: 409,
        message: `unable to maintain channel continuity; ${code}`,
      }),
    );
  }

  private mutate(
    messageSerial: string,
    action: "message.append" | "message.update" | "message.delete",
    data: unknown,
    extras: unknown,
  ): Promise<MessageAck> {
    const ack = this.nextAck(messageSerial);
    const raw = this.rawMessage(action, data, action, ack, extras);
    this.messages.push(raw);
    this.dispatch(raw);
    return Promise.resolve(ack);
  }

  private nextAck(messageSerial?: string): MessageAck {
    return {
      messageSerial: messageSerial ?? this.providers.messageSerial(),
      historySerial: this.providers.historySerial(),
      deliverySerial: this.providers.deliverySerial(),
      versionSerial: this.providers.versionSerial(),
    };
  }

  private rawMessage(
    name: string,
    data: unknown,
    action: string,
    ack: MessageAck,
    extras: unknown,
    messageId?: string,
  ): SockudoRawMessage {
    const raw: SockudoRawMessage = {
      event: `sockudo:${action}`,
      channel: this.name,
      name,
      data,
      extras: mergeMockExtras(extras, action, ack),
      history_serial: ack.historySerial,
      version: {
        timestamp_ms: this.providers.now(),
      },
    };
    if (messageId !== undefined) {
      raw.message_id = messageId;
    }
    if (ack.deliverySerial !== undefined) {
      raw.serial = ack.deliverySerial;
      raw.delivery_serial = ack.deliverySerial;
    }
    const version = raw.version as Record<string, unknown>;
    if (ack.versionSerial !== undefined) {
      version.serial = ack.versionSerial;
    }
    return raw;
  }

  private dispatch(raw: SockudoRawMessage): void {
    const message = normalizeInboundMessage(raw);
    for (const listener of this.listeners) {
      listener(message);
    }
  }
}

/** Creates an in-memory realtime mock client. */
export function createMockClient(options?: MockClientOptions): MockClient {
  return new MockClient(options);
}

class MockPresence implements PresenceLike {
  private readonly members = new Map<string, PresenceMember>();
  private readonly listeners = new Set<
    (event: PresenceEventName, member: PresenceMember) => void
  >();

  public enter(data?: unknown): Promise<void> {
    const member = { id: "self", data };
    this.members.set(member.id, member);
    this.emit("enter", member);
    return Promise.resolve();
  }

  public update(data?: unknown): Promise<void> {
    const member = { id: "self", data };
    this.members.set(member.id, member);
    this.emit("update", member);
    return Promise.resolve();
  }

  public leave(): Promise<void> {
    const member = this.members.get("self") ?? { id: "self", data: undefined };
    this.members.delete("self");
    this.emit("leave", member);
    return Promise.resolve();
  }

  public get(): Promise<readonly PresenceMember[]> {
    return Promise.resolve(Array.from(this.members.values()));
  }

  public subscribe(
    listener: (event: PresenceEventName, member: PresenceMember) => void,
  ): Unsubscribe {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private emit(event: PresenceEventName, member: PresenceMember): void {
    for (const listener of this.listeners) {
      listener(event, member);
    }
  }
}

function mergeMockExtras(
  extras: unknown,
  action: string,
  ack: MessageAck,
): unknown {
  const root = isRecord(extras) ? { ...extras } : {};
  const headers = isRecord(root.headers) ? { ...root.headers } : {};
  headers.sockudo_action = action;
  headers.sockudo_message_serial = ack.messageSerial;
  headers.sockudo_history_serial = ack.historySerial;
  if (ack.versionSerial !== undefined) {
    headers.sockudo_version_serial = ack.versionSerial;
  }
  root.headers = headers;
  return root;
}

function pageFrom(
  messages: readonly SockudoRawMessage[],
): PaginatedResult<InboundMessage> {
  return {
    items: messages.map((message) => normalizeInboundMessage(message)),
    hasNext(): boolean {
      return false;
    },
    next(): Promise<PaginatedResult<InboundMessage>> {
      return Promise.reject(
        new ErrorInfo({
          code: ErrorCode.InvalidArgument,
          message:
            "unable to read next history page; no next page is available",
        }),
      );
    },
  };
}

function serialCompare(a: Serial, b: Serial): -1 | 0 | 1 {
  const left = serialBigInt(a);
  const right = serialBigInt(b);
  if (left !== undefined && right !== undefined) {
    return left < right ? -1 : left > right ? 1 : 0;
  }
  return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
}

function serialBigInt(value: Serial): bigint | undefined {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? BigInt(value) : undefined;
  }
  return /^\d+$/.test(value) ? BigInt(value) : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object";
}
