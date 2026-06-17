import {
  EVENT_AI_CANCEL,
  EVENT_AI_INPUT,
  EVENT_AI_TURN_END,
  EVENT_AI_TURN_START,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_TURN_REASON,
} from "../../constants.js";
import { ErrorCode, ErrorInfo, toErrorInfo } from "../../errors.js";
import { LogLevel, makeLogger, type Logger } from "../../logger.js";
import type {
  ChannelLike,
  ClientLike,
  InboundMessage,
  MessageAck,
} from "../../realtime/index.js";
import {
  buildTransportHeaders,
  getCodecHeaders,
  getTransportHeaders,
  mergeHeaders,
  type HeaderMap,
} from "../../utils.js";
import type {
  Codec,
  DecodedEvent,
  Encoder,
  EncoderOptions,
  WriteOptions,
} from "../codec/index.js";
import type { TurnEndReason } from "./tree.js";
import {
  createDefaultInvocationIdProvider,
  type InvocationIdProvider,
} from "./invocation.js";
import {
  pipeStream,
  type ResolveWriteOptions,
  type StreamResult,
} from "./pipe-stream.js";
import {
  TurnManager,
  type BufferedInputEvent,
  type CancelRequest,
  type ManagedTurn,
} from "./turn-manager.js";
import type { CancelFilter } from "./client-transport.js";

/** Message node accepted by server-side `addMessages`. */
export interface MessageNode<TMessage> {
  /** Node discriminator. */
  kind?: "message";
  /** Domain message. */
  message: TMessage;
  /** Codec message id override for optimistic reconciliation. */
  msgId?: string;
  /** Parent codec message id. */
  parentId?: string;
  /** Fork source codec message id. */
  forkOf?: string;
  /** Header overrides. */
  headers?: HeaderMap;
}

/** Events targeting an existing codec message. */
export interface EventsNode<TOutput> {
  /** Node discriminator. */
  kind?: "event";
  /** Target codec message id. */
  msgId: string;
  /** Events to apply. */
  events: readonly TOutput[];
}

/** Options for `addMessages`. */
export interface AddMessageOptions {
  /** Verified client id for attribution. */
  clientId?: string;
}

/** Result of `addMessages`. */
export interface AddMessagesResult {
  /** Published codec message ids in order. */
  msgIds: string[];
}

/** Options for `streamResponse`. */
export interface StreamResponseOptions<TOutput> {
  /** Parent codec message id. */
  parent?: string;
  /** Fork source codec message id. */
  forkOf?: string;
  /** Per-output write option resolver. */
  resolveWriteOptions?: ResolveWriteOptions<TOutput>;
}

/** Options for `loadConversation`. */
export interface LoadConversationOptions {
  /** History page size.
   *
   * @defaultValue `200`
   */
  pageLimit?: number;
  /** Maximum materialized messages.
   *
   * @defaultValue `2000`
   */
  maxMessages?: number;
}

/** Server-side turn construction options. */
export interface NewTurnOptions<TOutput> {
  /** Turn identity. */
  turnId: string;
  /** Owner client id. */
  clientId?: string;
  /** Parent codec message id. */
  parent?: string;
  /** Fork source codec message id. */
  forkOf?: string;
  /** Hook invoked before encoder writes. */
  onMessage?: EncoderOptions["onMessage"];
  /** Hook invoked when a stream aborts. */
  onAbort?(write: (event: TOutput) => Promise<void>): void | Promise<void>;
  /** Cancel authorization hook. */
  onCancel?(request: CancelRequest): Promise<boolean> | boolean;
  /** Turn-scoped non-fatal error hook. */
  onError?(error: ErrorInfo): void;
  /** External abort signal. */
  signal?: AbortSignal;
  /** Invocation id for input-event lookup. */
  invocationId?: string;
  /** Input event id for input-event lookup. */
  inputEventId?: string;
}

/** Server-side turn. */
export interface Turn<TOutput, TProjection, TMessage> {
  /** Turn identity. */
  readonly turnId: string;
  /** Abort signal scoped to this turn. */
  readonly abortSignal: AbortSignal;
  /** Lightweight view over loaded messages. */
  readonly view: { readonly messages: readonly TMessage[] };
  /** Loaded messages alias. */
  readonly messages: readonly TMessage[];
  /** Publishes turn start after optional input lookup. */
  start(): Promise<void>;
  /** Publishes discrete messages. */
  addMessages(
    nodes: readonly MessageNode<TMessage>[],
    options?: AddMessageOptions,
  ): Promise<AddMessagesResult>;
  /** Streams response outputs. */
  streamResponse(
    stream: ReadableStream<TOutput>,
    options?: StreamResponseOptions<TOutput>,
  ): Promise<StreamResult>;
  /** Publishes cross-turn events. */
  addEvents(nodes: readonly EventsNode<TOutput>[]): Promise<void>;
  /** Loads this turn projection from history and observed input. */
  loadProjection(): Promise<TProjection>;
  /** Loads conversation messages. */
  loadConversation(options?: LoadConversationOptions): Promise<TMessage[]>;
  /** Publishes turn-end. */
  end(reason: TurnEndReason): Promise<void>;
}

/** Server transport options. */
export interface ServerTransportOptions<
  TInput,
  TOutput,
  TProjection,
  TMessage,
> {
  /** Realtime client used with `channelName`. */
  client?: ClientLike;
  /** Realtime channel. */
  channel?: ChannelLike;
  /** Channel name used when `client` is supplied. */
  channelName?: string;
  /** Domain codec. */
  codec: Codec<TInput, TOutput, TProjection, TMessage>;
  /** Logger.
   *
   * @defaultValue Silent SDK logger.
   */
  logger?: Logger;
  /** Transport-level error hook. */
  onError?(error: ErrorInfo): void;
  /** Input event lookup timeout in milliseconds.
   *
   * @defaultValue `30000`
   */
  inputEventLookupTimeoutMs?: number;
  /** Input event buffer cap.
   *
   * @defaultValue `200`
   */
  inputEventBufferLimit?: number;
  /** Subscribe rewind window.
   *
   * @defaultValue `"2m"`
   */
  rewindWindow?: string;
  /** Deterministic id provider for generated assistant message ids.
   *
   * @defaultValue Uses `crypto.randomUUID()` through the default invocation id provider.
   */
  idProvider?: InvocationIdProvider;
}

/** Server-side transport. */
export interface ServerTransport<TOutput, TProjection, TMessage> {
  /** Creates and registers a turn synchronously. */
  newTurn(
    options: NewTurnOptions<TOutput>,
  ): Turn<TOutput, TProjection, TMessage>;
  /** Unsubscribes, aborts turns, and clears state. */
  close(): void;
}

/** Creates a server/agent transport. */
export function createServerTransport<TInput, TOutput, TProjection, TMessage>(
  options: ServerTransportOptions<TInput, TOutput, TProjection, TMessage>,
): ServerTransport<TOutput, TProjection, TMessage> {
  return new DefaultServerTransport(options);
}

class DefaultServerTransport<TInput, TOutput, TProjection, TMessage>
  implements ServerTransport<TOutput, TProjection, TMessage>
{
  private readonly channel: ChannelLike;
  private readonly manager: TurnManager;
  private readonly logger: Logger;
  private readonly inputLookupTimeoutMs: number;
  private readonly idProvider: InvocationIdProvider;
  private readonly unsubscribe: () => void;
  private readonly channelUnsubscribes: (() => void)[];
  private closed = false;

  public constructor(
    private readonly options: ServerTransportOptions<
      TInput,
      TOutput,
      TProjection,
      TMessage
    >,
  ) {
    this.channel =
      options.channel ??
      options.client?.channels.get(
        requireChannelName(options),
        channelOptions(options.rewindWindow ?? "2m"),
      ) ??
      missingChannel();
    this.manager = new TurnManager(
      options.inputEventBufferLimit === undefined
        ? {}
        : { inputEventBufferLimit: options.inputEventBufferLimit },
    );
    this.logger = (
      options.logger ?? makeLogger({ logLevel: LogLevel.Silent })
    ).withContext({ component: "ServerTransport" });
    this.inputLookupTimeoutMs = options.inputEventLookupTimeoutMs ?? 30_000;
    this.idProvider = options.idProvider ?? createDefaultInvocationIdProvider();
    this.unsubscribe = this.channel.subscribe((message) => {
      this.handleMessage(message);
    });
    this.channelUnsubscribes = [
      this.channel.on("continuity_lost", (error) => options.onError?.(error)),
      this.channel.on("failed", (error) => options.onError?.(error)),
    ];
  }

  public newTurn(
    options: NewTurnOptions<TOutput>,
  ): Turn<TOutput, TProjection, TMessage> {
    if (this.closed) {
      throw new ErrorInfo({
        code: ErrorCode.TransportClosed,
        statusCode: 400,
        message: "unable to create turn; transport is closed",
      });
    }
    const turn = new DefaultTurn<TInput, TOutput, TProjection, TMessage>({
      channel: this.channel,
      codec: this.options.codec,
      manager: this.manager,
      options,
      inputLookupTimeoutMs: this.inputLookupTimeoutMs,
      idProvider: this.idProvider,
      logger: this.logger,
    });
    this.manager.register(turn.managedTurn);
    return turn;
  }

  public close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.unsubscribe();
    for (const unsubscribe of this.channelUnsubscribes) {
      unsubscribe();
    }
    this.manager.close();
  }

  private handleMessage(message: InboundMessage): void {
    try {
      if (message.name === EVENT_AI_INPUT) {
        this.manager.observeInput(message);
      } else if (message.name === EVENT_AI_CANCEL) {
        this.manager.routeCancel(message);
      }
    } catch (error) {
      const info = toErrorInfo(error, {
        code: ErrorCode.TransportSubscriptionError,
        message: "unable to process server transport message",
      });
      this.options.onError?.(info);
      this.logger.error("server transport message handler failed", {
        error: info,
      });
    }
  }
}

interface TurnDeps<TInput, TOutput, TProjection, TMessage> {
  channel: ChannelLike;
  codec: Codec<TInput, TOutput, TProjection, TMessage>;
  manager: TurnManager;
  options: NewTurnOptions<TOutput>;
  inputLookupTimeoutMs: number;
  idProvider: InvocationIdProvider;
  logger: Logger;
}

class DefaultTurn<TInput, TOutput, TProjection, TMessage>
  implements Turn<TOutput, TProjection, TMessage>
{
  private readonly internalAbort = new AbortController();
  private readonly signal: AbortSignal;
  private started = false;
  private loadedProjection: TProjection | undefined;
  private loadedMessages: TMessage[] = [];
  private capturedInput: BufferedInputEvent | undefined;

  public readonly turnId: string;
  public readonly managedTurn: ManagedTurn;

  public constructor(
    private readonly deps: TurnDeps<TInput, TOutput, TProjection, TMessage>,
  ) {
    this.turnId = deps.options.turnId;
    this.signal = composeAbortSignal(
      this.internalAbort.signal,
      deps.options.signal,
    );
    this.managedTurn = {
      turnId: this.turnId,
      abort: () => {
        this.internalAbort.abort();
      },
      cancelled: false,
      ...(deps.options.clientId !== undefined
        ? { clientId: deps.options.clientId }
        : {}),
      ...(deps.options.onCancel !== undefined
        ? { onCancel: (request) => deps.options.onCancel?.(request) ?? true }
        : {}),
      ...(deps.options.onError !== undefined
        ? {
            onError: (error) => {
              deps.options.onError?.(error);
            },
          }
        : {}),
    };
  }

  public get abortSignal(): AbortSignal {
    return this.signal;
  }

  public get view(): { readonly messages: readonly TMessage[] } {
    return { messages: this.loadedMessages };
  }

  public get messages(): readonly TMessage[] {
    return this.loadedMessages;
  }

  public async start(): Promise<void> {
    if (this.started) {
      return;
    }
    let inputHeaders: HeaderMap = Object.create(null) as HeaderMap;
    if (this.deps.options.invocationId && this.deps.options.inputEventId) {
      this.capturedInput = await this.deps.manager.lookupInput(
        this.deps.options.invocationId,
        this.deps.inputLookupTimeoutMs,
      );
      inputHeaders = this.capturedInput.headers;
      this.foldInput(this.capturedInput.message);
    }
    const headers = mergeHeaders(
      inputHeaders,
      buildTransportHeaders({
        turnId: this.turnId,
        ...(this.deps.options.invocationId !== undefined
          ? { invocationId: this.deps.options.invocationId }
          : {}),
        ...(this.deps.options.clientId !== undefined
          ? { turnClientId: this.deps.options.clientId }
          : {}),
        ...(this.deps.options.parent !== undefined
          ? { parent: this.deps.options.parent }
          : {}),
        ...(this.deps.options.forkOf !== undefined
          ? { forkOf: this.deps.options.forkOf }
          : {}),
      }),
    );
    await this.publishLifecycle(EVENT_AI_TURN_START, headers);
    this.started = true;
  }

  public async addMessages(
    nodes: readonly MessageNode<TMessage>[],
    options: AddMessageOptions = {},
  ): Promise<AddMessagesResult> {
    const msgIds: string[] = [];
    const encoder = this.createEncoder(options.clientId);
    for (const node of nodes) {
      const input = this.deps.codec.createUserMessage(node.message).message;
      const headers = mergeHeaders(
        this.baseHeaders("user"),
        node.headers ?? emptyHeaders(),
        buildTransportHeaders({
          ...(node.msgId !== undefined ? { codecMessageId: node.msgId } : {}),
          ...(node.parentId !== undefined ? { parent: node.parentId } : {}),
          ...(node.forkOf !== undefined ? { forkOf: node.forkOf } : {}),
        }),
      );
      const ack = await publishInput(encoder, input as unknown as TInput, {
        extras: { ai: { transport: headers } },
        ...(node.msgId !== undefined ? { messageId: node.msgId } : {}),
        ...(options.clientId !== undefined
          ? { clientId: options.clientId }
          : {}),
      });
      msgIds.push(node.msgId ?? ack.messageSerial);
    }
    return { msgIds };
  }

  public streamResponse(
    stream: ReadableStream<TOutput>,
    options: StreamResponseOptions<TOutput> = {},
  ): Promise<StreamResult> {
    const messageId = this.deps.idProvider.messageId();
    const encoder = this.createEncoder(this.deps.options.clientId, {
      role: "assistant",
      codecMessageId: messageId,
      ...(options.parent !== undefined ? { parent: options.parent } : {}),
      ...(options.forkOf !== undefined ? { forkOf: options.forkOf } : {}),
    });
    return pipeStream(stream, encoder, this.signal, {
      resolveWriteOptions: (output) =>
        mergeWriteOptions(options.resolveWriteOptions?.(output), {
          extras: { ai: { transport: this.baseHeaders("assistant") } },
        }),
      ...(this.deps.options.onAbort !== undefined
        ? {
            onAbort: (write) => this.deps.options.onAbort?.(write),
          }
        : {}),
      ...(this.deps.options.onError !== undefined
        ? {
            onError: (error) => {
              this.deps.options.onError?.(error);
            },
          }
        : {}),
    });
  }

  public async addEvents(nodes: readonly EventsNode<TOutput>[]): Promise<void> {
    for (const node of nodes) {
      const encoder = this.createEncoder(this.deps.options.clientId, {
        codecMessageId: node.msgId,
      });
      for (const event of node.events) {
        await encoder.publishOutput(event, {
          extras: {
            ai: {
              transport: mergeHeaders(this.baseHeaders("assistant"), {
                [HEADER_CODEC_MESSAGE_ID]: node.msgId,
              }),
            },
          },
        });
      }
      await encoder.close();
    }
  }

  public async loadProjection(): Promise<TProjection> {
    if (this.loadedProjection !== undefined) {
      return this.loadedProjection;
    }
    let projection = this.deps.codec.init();
    const seen = new Set<string>();
    if (this.capturedInput) {
      projection = this.foldMessage(
        projection,
        this.capturedInput.message,
        seen,
      );
    }
    let page = await this.deps.channel.history({
      direction: "oldest_first",
      limit: 200,
    });
    for (;;) {
      for (const message of page.items) {
        projection = this.foldMessage(projection, message, seen);
      }
      if (!page.hasNext()) {
        break;
      }
      page = await page.next();
    }
    this.loadedProjection = projection;
    this.loadedMessages = this.deps.codec.getMessages(projection);
    return projection;
  }

  public async loadConversation(
    options: LoadConversationOptions = {},
  ): Promise<TMessage[]> {
    await this.loadProjection();
    const maxMessages = options.maxMessages ?? 2_000;
    return this.loadedMessages.slice(-maxMessages);
  }

  public async end(reason: TurnEndReason): Promise<void> {
    const headers = mergeHeaders(this.baseHeaders("assistant"), {
      [HEADER_TURN_REASON]: reason,
    });
    try {
      await this.publishLifecycle(EVENT_AI_TURN_END, headers);
    } catch (error) {
      throw toErrorInfo(error, {
        code: ErrorCode.TurnLifecycleError,
        message: "unable to end turn; turn-end publish failed",
      });
    }
    if (reason !== "suspended") {
      this.deps.manager.deregister(this.turnId);
    }
  }

  private foldInput(message: InboundMessage): void {
    const projection = this.loadedProjection ?? this.deps.codec.init();
    this.loadedProjection = this.foldMessage(projection, message, new Set());
    this.loadedMessages = this.deps.codec.getMessages(this.loadedProjection);
  }

  private foldMessage(
    projection: TProjection,
    message: InboundMessage,
    seen: Set<string>,
  ): TProjection {
    const key = `${String(message.deliverySerial ?? message.historySerial)}:${message.messageSerial}`;
    if (seen.has(key)) {
      return projection;
    }
    seen.add(key);
    const decoded = this.deps.codec.createDecoder().decode(message);
    const events = [...decoded.inputs, ...decoded.outputs] as DecodedEvent<
      TInput | TOutput
    >[];
    let current = projection;
    for (const event of events) {
      current = this.deps.codec.fold(current, event.event, event.meta);
    }
    return current;
  }

  private async publishLifecycle(
    name: typeof EVENT_AI_TURN_START | typeof EVENT_AI_TURN_END,
    headers: HeaderMap,
  ): Promise<MessageAck> {
    try {
      return await this.deps.channel.publish({
        name,
        extras: { ai: { transport: headers } },
      });
    } catch (error) {
      throw toErrorInfo(error, {
        code: ErrorCode.TurnLifecycleError,
        message: `unable to publish ${name}; channel publish failed`,
      });
    }
  }

  private createEncoder(
    clientId: string | undefined,
    headers: {
      parent?: string;
      forkOf?: string;
      codecMessageId?: string;
      role?: string;
    } = {},
  ): Encoder<TInput, TOutput> {
    return this.deps.codec.createEncoder(this.deps.channel, {
      ...(this.deps.options.onMessage !== undefined
        ? { onMessage: this.deps.options.onMessage }
        : {}),
      ...(clientId !== undefined ? { clientId } : {}),
      extras: {
        ai: {
          transport: this.baseHeaders(headers.role ?? "assistant", headers),
        },
      },
    });
  }

  private baseHeaders(
    role: string,
    overrides: {
      parent?: string;
      forkOf?: string;
      codecMessageId?: string;
    } = {},
  ): HeaderMap {
    return buildTransportHeaders({
      role,
      turnId: this.turnId,
      ...(this.deps.options.invocationId !== undefined
        ? { invocationId: this.deps.options.invocationId }
        : {}),
      ...(this.deps.options.clientId !== undefined
        ? {
            turnClientId: this.deps.options.clientId,
            inputClientId: this.deps.options.clientId,
          }
        : {}),
      ...(this.deps.options.parent !== undefined
        ? { parent: this.deps.options.parent }
        : {}),
      ...(this.deps.options.forkOf !== undefined
        ? { forkOf: this.deps.options.forkOf }
        : {}),
      ...(overrides.parent !== undefined ? { parent: overrides.parent } : {}),
      ...(overrides.forkOf !== undefined ? { forkOf: overrides.forkOf } : {}),
      ...(overrides.codecMessageId !== undefined
        ? { codecMessageId: overrides.codecMessageId }
        : {}),
    });
  }
}

function publishInput<TInput, TOutput>(
  encoder: Encoder<TInput, TOutput>,
  input: TInput,
  options: WriteOptions,
): Promise<MessageAck> {
  return encoder.publishInput(input, options);
}

function mergeWriteOptions(
  left: WriteOptions | undefined,
  right: WriteOptions,
): WriteOptions {
  return {
    ...left,
    ...right,
    extras: mergeExtras(left?.extras, right.extras),
  };
}

function mergeExtras(left: unknown, right: unknown): unknown {
  const leftRecord = record(left);
  const rightRecord = record(right);
  const leftAi = record(leftRecord.ai);
  const rightAi = record(rightRecord.ai);
  return {
    ...leftRecord,
    ...rightRecord,
    ai: {
      ...leftAi,
      ...rightAi,
      transport: mergeHeaders(
        getTransportHeaders(left),
        getTransportHeaders(right),
      ),
      codec: mergeHeaders(getCodecHeaders(left), getCodecHeaders(right)),
    },
  };
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : {};
}

function emptyHeaders(): HeaderMap {
  return Object.create(null) as HeaderMap;
}

function composeAbortSignal(
  internal: AbortSignal,
  external: AbortSignal | undefined,
): AbortSignal {
  if (!external) {
    return internal;
  }
  if ("any" in AbortSignal && typeof AbortSignal.any === "function") {
    return AbortSignal.any([internal, external]);
  }
  const controller = new AbortController();
  const abort = (): void => {
    controller.abort();
  };
  internal.addEventListener("abort", abort, { once: true });
  external.addEventListener("abort", abort, { once: true });
  return controller.signal;
}

function requireChannelName(
  options: ServerTransportOptions<unknown, unknown, unknown, unknown>,
): string {
  if (!options.channelName) {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message:
        "unable to create server transport; channelName is required with client",
    });
  }
  return options.channelName;
}

function channelOptions(value: string): {
  params?: { rewind?: { seconds: number } };
} {
  const rewind = normalizeRewind(value);
  return rewind === undefined ? {} : { params: { rewind } };
}

function normalizeRewind(value: string): { seconds: number } | undefined {
  const match = /^(\d+)m$/u.exec(value);
  if (!match) {
    return undefined;
  }
  return { seconds: Number(match[1]) * 60 };
}

function missingChannel(): ChannelLike {
  throw new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    message: "unable to create server transport; channel could not be resolved",
  });
}

export type { CancelFilter, CancelRequest, StreamResult };
