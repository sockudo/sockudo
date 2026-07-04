import {
  EVENT_AI_CANCEL,
  EVENT_AI_INPUT,
  EVENT_AI_LEGACY_TURN_END,
  EVENT_AI_LEGACY_TURN_START,
  EVENT_AI_RUN_END,
  EVENT_AI_RUN_RESUME,
  EVENT_AI_RUN_START,
  EVENT_AI_RUN_SUSPEND,
  HEADER_INVOCATION_ID,
  HEADER_RUN_ID,
  HEADER_RUN_REASON,
  HEADER_TURN_CONTINUE,
} from "../../constants.js";
import { ErrorCode, ErrorInfo, toErrorInfo } from "../../errors.js";
import { EventEmitter, type EventUnsubscribe } from "../../event-emitter.js";
import { LogLevel, makeLogger, type Logger } from "../../logger.js";
import type { ChannelLike, ClientLike, InboundMessage } from "../../realtime/index.js";
import {
  buildTransportHeaders,
  mergeHeaders,
  stripUndefined,
  type HeaderMap,
} from "../../utils.js";
import type { Codec, DecodedBatch, DecodedEvent } from "../codec/index.js";
import { createConversationTree, type ConversationTree } from "./tree.js";
import { createView, type View, type ViewSendExecutor } from "./view.js";
import { createDefaultInvocationIdProvider, type InvocationIdProvider } from "./invocation.js";
import { createStreamRouter, type StreamRouter } from "./stream-router.js";

/**
 * Cancellation scope for client-side turn cancellation.
 *
 * @defaultValue `cancel()` and `waitForTurn()` default to `{ own: true }`.
 */
export type CancelFilter =
  | { turnId: string; own?: never; clientId?: never; all?: never }
  | { own: boolean; turnId?: never; clientId?: never; all?: never }
  | { clientId: string; turnId?: never; own?: never; all?: never }
  | { all: boolean; turnId?: never; own?: never; clientId?: never };

/**
 * Per-send options for HTTP body/header merging and branch metadata.
 */
export interface SendOptions {
  /** Additional POST body fields. Invocation fields always win. */
  body?: Record<string, unknown>;
  /** Additional POST headers. */
  headers?: Record<string, string>;
  /** Return the active stream immediately instead of waiting for `ai-run-start`.
   *
   * @defaultValue `false`.
   */
  waitForTurnStart?: boolean;
  /** Existing run id for suspended-run continuation. */
  turnId?: string;
  /** Message id this send replaces. */
  forkOf?: string;
  /** Parent message id. Defaults to the selected branch tail. */
  parent?: string;
  /** Trigger label for the default POST body.
   *
   * @defaultValue `"message"`
   */
  trigger?: string;
  /** Message id associated with edit/regenerate requests. */
  messageId?: string;
}

/**
 * A handle to an active client-side turn.
 */
export interface ActiveTurn<TOutput> {
  /** Decoded output stream for this turn. */
  stream: ReadableStream<TOutput>;
  /** Turn identity. */
  turnId: string;
  /** Invocation identity for this send or continuation. */
  invocationId: string;
  /** Primary input event id. */
  inputEventId: string;
  /** Cancels this turn and closes the local stream. */
  cancel(): Promise<void>;
  /** Optimistically inserted codec message ids. */
  optimisticMsgIds: readonly string[];
}

/**
 * Options for closing a client transport.
 */
export interface CloseOptions {
  /** Optional cancel filter to publish before local teardown. */
  cancel?: CancelFilter;
}

/**
 * Client transport event map.
 */
export interface ClientTransportEvents {
  /** Non-fatal transport error. */
  error: ErrorInfo;
  /** Raw normalized inbound channel message before codec folding. */
  message: InboundMessage;
}

/**
 * Options for creating a client transport.
 */
export interface ClientTransportOptions<TInput, TOutput, TProjection, TMessage> {
  /** Realtime client used with `channelName`. */
  client?: ClientLike;
  /** Realtime channel. */
  channel?: ChannelLike;
  /** Channel name used when `client` is supplied.
   *
   * @defaultValue `channel.name` when `channel` is supplied.
   */
  channelName?: string;
  /** Domain codec. */
  codec: Codec<TInput, TOutput, TProjection, TMessage>;
  /** Server endpoint URL for the HTTP poke. */
  api: string;
  /** Verified client identity.
   *
   * @defaultValue `client.connection.clientId` when available.
   */
  clientId?: string;
  /** Static or per-send POST headers. */
  headers?: Record<string, string> | (() => Record<string, string>);
  /** Static or per-send POST body fields. */
  body?: Record<string, unknown> | (() => Record<string, unknown>);
  /** Fetch credentials mode. */
  credentials?: RequestCredentials;
  /** Fetch implementation.
   *
   * @defaultValue `globalThis.fetch`.
   */
  fetch?: typeof globalThis.fetch;
  /** Initial messages seeded as a linear chain. */
  messages?: readonly TMessage[];
  /** Logger.
   *
   * @defaultValue Silent SDK logger.
   */
  logger?: Logger;
  /** Turn-start wait deadline in milliseconds.
   *
   * @defaultValue `30000`.
   */
  turnStartDeadlineMs?: number;
  /** Deterministic id provider for tests. */
  idProvider?: InvocationIdProvider;
  /** Maximum queued stream chunks before local stream error.
   *
   * @defaultValue `1024`.
   */
  streamQueueLimit?: number;
}

/**
 * Client-side transport that owns tree, views, sends, streams, and cancellation.
 */
export interface ClientTransport<TInput, TOutput, TProjection, TMessage> {
  /** Complete conversation tree. */
  readonly tree: ConversationTree<TInput | TOutput, TProjection>;
  /** Default branch-aware view. */
  readonly view: View<TInput, TMessage>;
  /** Creates an additional branch-aware view. */
  createView(): View<TInput, TMessage>;
  /** Cancels turns matching a filter. Rejects with {@link ErrorInfo} on publish failure. */
  cancel(filter?: CancelFilter): Promise<void>;
  /** Resolves when matching active turns complete. */
  waitForTurn(filter?: CancelFilter): Promise<void>;
  /** Locally applies events and queues them for the next send POST body. */
  stageEvents(msgId: string, events: readonly TOutput[]): void;
  /** Locally replaces a message projection while preserving known headers/serial. */
  stageMessage(msgId: string, message: TMessage): void;
  /** Subscribes to transport events. */
  on<K extends keyof ClientTransportEvents>(
    event: K,
    handler: (payload: ClientTransportEvents[K]) => void,
  ): EventUnsubscribe;
  /** Closes local resources, optionally publishing cancel first. */
  close(options?: CloseOptions): Promise<void>;
}

/**
 * Creates a Sockudo client transport.
 *
 * Async public methods reject with {@link ErrorInfo}. Synchronous misuse throws
 * {@link ErrorInfo} with {@link ErrorCode.InvalidArgument} or
 * {@link ErrorCode.TransportClosed}.
 */
export function createClientTransport<TInput, TOutput, TProjection, TMessage>(
  options: ClientTransportOptions<TInput, TOutput, TProjection, TMessage>,
): ClientTransport<TInput, TOutput, TProjection, TMessage> {
  return new DefaultClientTransport(options);
}

interface PendingTurnStart<TOutput> {
  invocationId: string;
  resolve(turn: ActiveTurn<TOutput>): void;
  reject(error: ErrorInfo): void;
  timer: ReturnType<typeof setTimeout> | undefined;
  turn: ActiveTurn<TOutput>;
}

interface OwnTurn {
  clientId: string;
  invocationId: string;
}

interface StagedEvents<TOutput> {
  msgId: string;
  events: readonly TOutput[];
}

class DefaultClientTransport<TInput, TOutput, TProjection, TMessage> implements ClientTransport<
  TInput,
  TOutput,
  TProjection,
  TMessage
> {
  private readonly channel: ChannelLike;
  private readonly clientId: string | undefined;
  private readonly logger: Logger;
  private readonly idProvider: InvocationIdProvider;
  private readonly decoder;
  private readonly router: StreamRouter<TOutput>;
  private readonly emitter = new EventEmitter<ClientTransportEvents>();
  private readonly views = new Set<View<TInput, TMessage>>();
  private readonly ownTurns = new Map<string, OwnTurn>();
  private readonly pendingTurnStarts = new Map<string, PendingTurnStart<TOutput>>();
  private readonly closeResolvers = new Set<() => void>();
  private readonly unsubscribes: EventUnsubscribe[] = [];
  private readonly turnStartDeadlineMs: number;
  private readonly fetchFn: typeof globalThis.fetch;
  private readonly headerProvider: () => Record<string, string>;
  private readonly bodyProvider: () => Record<string, unknown>;
  private stagedEvents: StagedEvents<TOutput>[] = [];
  private closed = false;
  private connected = false;

  public readonly tree: ConversationTree<TInput | TOutput, TProjection>;
  public readonly view: View<TInput, TMessage>;

  public constructor(
    private readonly options: ClientTransportOptions<TInput, TOutput, TProjection, TMessage>,
  ) {
    if (!options.channel && !options.client) {
      throw new ErrorInfo({
        code: ErrorCode.InvalidArgument,
        message: "unable to create client transport; channel or client is required",
      });
    }
    if (options.client && !options.channel && !options.channelName) {
      throw new ErrorInfo({
        code: ErrorCode.InvalidArgument,
        message: "unable to create client transport; channelName is required with client",
      });
    }
    this.channel =
      options.channel ??
      options.client?.channels.get(options.channelName ?? "") ??
      missingChannel();
    this.clientId = options.clientId ?? options.client?.connection.clientId;
    this.logger = (options.logger ?? makeLogger({ logLevel: LogLevel.Silent })).withContext({
      component: "ClientTransport",
    });
    assertAiTransportFeature(readConnectionFeatures(options.client), this.logger);
    this.idProvider = options.idProvider ?? createDefaultInvocationIdProvider();
    this.decoder = options.codec.createDecoder();
    this.fetchFn = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.turnStartDeadlineMs = options.turnStartDeadlineMs ?? 30_000;
    this.headerProvider = normalizeProvider(options.headers);
    this.bodyProvider = normalizeProvider(options.body);
    this.tree = createConversationTree<TInput | TOutput, TProjection>(options.codec);
    this.router = createStreamRouter<TOutput>({
      isTerminal: (output) => options.codec.isTerminal(output),
      ...(options.streamQueueLimit !== undefined
        ? { maxQueuedChunks: options.streamQueueLimit }
        : {}),
    });
    const executor = this.createSendExecutor();
    this.view = createView({
      tree: this.tree,
      codec: options.codec,
      decoder: this.decoder,
      history: this.channel,
      sendExecutor: executor,
    });
    this.views.add(this.view);
    this.seedMessages(options.messages ?? []);
  }

  public createView(): View<TInput, TMessage> {
    this.assertOpen("create view");
    const view = createView({
      tree: this.tree,
      codec: this.options.codec,
      decoder: this.decoder,
      history: this.channel,
      sendExecutor: this.createSendExecutor(),
    });
    this.views.add(view);
    return view;
  }

  public async cancel(filter: CancelFilter = { own: true }): Promise<void> {
    if (this.closed) {
      return;
    }
    try {
      await this.channel.publish({
        name: EVENT_AI_CANCEL,
        data: filter,
        extras: {
          ai: {
            transport: cancelHeaders(filter, this.clientId),
          },
        },
      });
      this.closeMatchingTurnStreams(filter);
    } catch (error) {
      throw toErrorInfo(error, {
        code: ErrorCode.TransportSendFailed,
        message: "unable to cancel turns; cancel publish failed",
      });
    }
  }

  public waitForTurn(filter: CancelFilter = { own: true }): Promise<void> {
    if (this.closed) {
      return Promise.resolve();
    }
    const remaining = this.getMatchingTurnIds(filter);
    if (remaining.size === 0) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const done = (): void => {
        unsubscribe();
        this.closeResolvers.delete(done);
        resolve();
      };
      const unsubscribe = this.tree.on("turn", (node) => {
        if (node.status === "active" || node.status === "suspended") {
          return;
        }
        remaining.delete(node.turnId);
        if (remaining.size === 0) {
          done();
        }
      });
      this.closeResolvers.add(done);
    });
  }

  public stageEvents(msgId: string, events: readonly TOutput[]): void {
    if (this.closed || events.length === 0) {
      return;
    }
    const headers = this.tree.getHeaders(msgId);
    const node = this.tree.getTurnByCodecMessageId(msgId);
    if (!headers || !node) {
      this.logger.warn("stageEvents ignored unknown message id", { msgId });
      return;
    }
    const decoded = events.map((event, index) =>
      decodedEvent<TInput | TOutput>(
        event,
        msgId,
        `${String(node.startSerial ?? "local")}:stage:${String(index)}`,
      ),
    );
    this.tree.applyMessage(decoded, headers, node.startSerial ?? "local");
    this.stagedEvents.push({ msgId, events: [...events] });
  }

  public stageMessage(msgId: string, message: TMessage): void {
    if (this.closed) {
      return;
    }
    const headers = this.tree.getHeaders(msgId);
    const node = this.tree.getTurnByCodecMessageId(msgId);
    if (!headers || !node) {
      this.logger.warn("stageMessage ignored unknown message id", { msgId });
      return;
    }
    this.tree.applyMessage(
      [decodedEvent(message as unknown as TInput | TOutput, msgId, node.startSerial ?? "local")],
      headers,
      node.startSerial ?? "local",
    );
  }

  public on<K extends keyof ClientTransportEvents>(
    event: K,
    handler: (payload: ClientTransportEvents[K]) => void,
  ): EventUnsubscribe {
    if (this.closed) {
      return () => undefined;
    }
    if (event === "message") {
      this.connect();
    }
    return this.emitter.on(event, handler);
  }

  public async close(options: CloseOptions = {}): Promise<void> {
    if (this.closed) {
      return;
    }
    if (options.cancel) {
      try {
        await this.cancel(options.cancel);
      } catch {
        // Best-effort shutdown path.
      }
    }
    this.closed = true;
    for (const pending of this.pendingTurnStarts.values()) {
      if (pending.timer) {
        clearTimeout(pending.timer);
      }
      pending.reject(
        new ErrorInfo({
          code: ErrorCode.TransportClosed,
          statusCode: 400,
          message: "unable to wait for turn start; transport is closed",
        }),
      );
    }
    this.pendingTurnStarts.clear();
    this.router.closeAll();
    for (const view of this.views) {
      view.close();
    }
    this.views.clear();
    for (const unsubscribe of this.unsubscribes) {
      unsubscribe();
    }
    this.unsubscribes.length = 0;
    for (const resolve of Array.from(this.closeResolvers)) {
      resolve();
    }
  }

  private createSendExecutor(): ViewSendExecutor<TInput, TMessage> {
    return {
      send: (message, options = {}) => this.internalSend([message], options),
      sendInput: (input, options = {}) => this.internalSendInput(normalizeInputs(input), options),
      regenerate: (target, parent, options = {}) =>
        this.internalSendInput([this.options.codec.createRegenerate(target, parent) as TInput], {
          ...options,
          forkOf: target,
          parent,
          messageId: target,
          trigger: "regenerate",
        }),
      edit: (messageId, message, options = {}) =>
        this.internalSend([message], {
          ...options,
          forkOf: messageId,
          messageId,
          trigger: "edit",
        }),
      update: (messageId, patch, options = {}) => {
        const events = Array.isArray(patch)
          ? (patch as readonly TOutput[])
          : ([patch] as readonly unknown[] as readonly TOutput[]);
        this.stageEvents(messageId, events);
        return this.internalSend([], {
          ...options,
          messageId,
          trigger: "update",
        });
      },
    };
  }

  private async internalSend(
    messages: readonly TMessage[],
    sendOptions: SendOptions,
  ): Promise<ActiveTurn<TOutput>> {
    const inputs = messages.map((message) => this.options.codec.createUserMessage(message).message);
    return this.sendPipeline(
      inputs.map((message) => message as unknown as TInput),
      messages,
      sendOptions,
    );
  }

  private internalSendInput(
    inputs: readonly TInput[],
    sendOptions: SendOptions,
  ): Promise<ActiveTurn<TOutput>> {
    return this.sendPipeline(inputs, [], sendOptions);
  }

  private async sendPipeline(
    inputs: readonly TInput[],
    messages: readonly TMessage[],
    sendOptions: SendOptions,
  ): Promise<ActiveTurn<TOutput>> {
    this.assertOpen("send");
    this.connect();

    const turnId = sendOptions.turnId ?? this.idProvider.turnId();
    const invocationId = this.idProvider.invocationId();
    const eventIds = inputs.map(() => this.idProvider.inputEventId());
    const inputEventId = eventIds[eventIds.length - 1] ?? this.idProvider.inputEventId();
    const parent = sendOptions.parent ?? this.currentParent();
    const isContinuation = sendOptions.turnId !== undefined;
    const regenerateOf =
      sendOptions.trigger === "regenerate"
        ? (sendOptions.messageId ?? sendOptions.forkOf)
        : undefined;
    const wireForkOf = sendOptions.trigger === "regenerate" ? undefined : sendOptions.forkOf;
    const optimisticMsgIds: string[] = [];
    const staged = this.stagedEvents;
    this.stagedEvents = [];

    for (let index = 0; index < messages.length; index += 1) {
      const message = messages[index];
      if (message === undefined) {
        continue;
      }
      const messageId = messageIdOf(message) ?? this.idProvider.messageId();
      optimisticMsgIds.push(messageId);
      const headers = this.inputHeaders({
        turnId,
        invocationId,
        inputEventId: eventIds[index] ?? inputEventId,
        codecMessageId: messageId,
        ...(index === 0
          ? parent !== undefined
            ? { parent }
            : {}
          : optimisticMsgIds[index - 1] !== undefined
            ? { parent: optimisticMsgIds[index - 1] }
            : {}),
        ...(wireForkOf !== undefined ? { forkOf: wireForkOf } : {}),
        turnContinue: isContinuation,
        ...(regenerateOf !== undefined ? { regenerates: regenerateOf } : {}),
      });
      this.tree.applyMessage(
        [
          decodedEvent<TInput | TOutput>(
            message as unknown as TInput | TOutput,
            messageId,
            "optimistic",
          ),
        ],
        headers,
        "optimistic",
      );
    }

    try {
      for (let index = 0; index < inputs.length; index += 1) {
        const input = inputs[index];
        if (input === undefined) {
          continue;
        }
        const inputCodecMessageId = optimisticMsgIds[index] ?? sendOptions.messageId;
        await this.channel.publish({
          name: EVENT_AI_INPUT,
          data: input,
          ...(inputCodecMessageId !== undefined
            ? {
                messageSerial: inputCodecMessageId,
                messageId: inputCodecMessageId,
              }
            : {}),
          extras: {
            ai: {
              transport: this.inputHeaders({
                turnId,
                invocationId,
                inputEventId: eventIds[index] ?? inputEventId,
                ...(inputCodecMessageId !== undefined
                  ? { codecMessageId: inputCodecMessageId }
                  : {}),
                ...(index === 0
                  ? parent !== undefined
                    ? { parent }
                    : {}
                  : optimisticMsgIds[index - 1] !== undefined
                    ? { parent: optimisticMsgIds[index - 1] }
                    : {}),
                ...(wireForkOf !== undefined ? { forkOf: wireForkOf } : {}),
                turnContinue: isContinuation,
                ...(regenerateOf !== undefined ? { regenerates: regenerateOf } : {}),
              }),
            },
          },
        });
      }
    } catch (error) {
      for (const msgId of optimisticMsgIds) {
        this.tree.delete(msgId);
      }
      throw mapPublishFailure(error, {
        code: ErrorCode.TransportSendFailed,
        message: "unable to send; channel publish failed",
      });
    }

    const stream = this.router.has(turnId)
      ? this.rebindContinuation(turnId, invocationId)
      : this.router.createStream(turnId, invocationId);
    this.ownTurns.set(turnId, {
      invocationId,
      clientId: this.clientId ?? "",
    });
    const activeTurn: ActiveTurn<TOutput> = {
      stream,
      turnId,
      invocationId,
      inputEventId,
      cancel: () => this.cancel({ turnId }),
      optimisticMsgIds,
    };

    const waiter =
      sendOptions.waitForTurnStart === false ? undefined : this.waitForTurnStart(activeTurn);
    this.poke(
      {
        turnId,
        invocationId,
        inputEventId,
        parent,
        forkOf: sendOptions.forkOf,
        trigger: sendOptions.trigger ?? "message",
        messageId: sendOptions.messageId ?? optimisticMsgIds.at(-1),
        messages,
        inputs,
        staged,
      },
      sendOptions,
    );
    return waiter ?? activeTurn;
  }

  private connect(): void {
    if (this.connected) {
      return;
    }
    this.connected = true;
    this.unsubscribes.push(
      this.channel.subscribe((message) => {
        this.handleInbound(message);
      }),
      this.channel.on("continuity_lost", (error) => {
        this.handleContinuityLost(error);
      }),
    );
  }

  private handleInbound(message: InboundMessage): void {
    if (this.closed) {
      return;
    }
    try {
      this.emitter.emit("message", message);
      const transportHeaders = message.getTransportHeaders();
      if (isRunStartMessage(message.name)) {
        this.handleTurnStart(message, transportHeaders);
        return;
      }
      if (message.name === EVENT_AI_RUN_RESUME) {
        this.handleTurnStart(
          message,
          mergeHeaders(transportHeaders, { [HEADER_TURN_CONTINUE]: "true" }),
        );
        return;
      }
      if (message.name === EVENT_AI_RUN_SUSPEND) {
        this.handleTurnEnd(
          message,
          mergeHeaders(transportHeaders, { [HEADER_RUN_REASON]: "suspended" }),
        );
        return;
      }
      if (isRunEndMessage(message.name)) {
        this.handleTurnEnd(message, transportHeaders);
        return;
      }
      if (message.action === "summary") {
        return;
      }
      const batch = this.decoder.decode(message);
      const folded = decodedForFold<TInput, TOutput>(batch);
      if (folded.length > 0) {
        this.tree.applyMessage(
          folded,
          transportHeaders,
          message.deliverySerial ?? message.historySerial,
        );
      }
      const turnId = transportHeaders[HEADER_RUN_ID];
      if (!turnId) {
        return;
      }
      const invocationId = transportHeaders[HEADER_INVOCATION_ID];
      for (const output of batch.outputs) {
        this.router.route(turnId, invocationId, output.event);
      }
    } catch (error) {
      this.emitError(
        toErrorInfo(error, {
          code: ErrorCode.TransportSubscriptionError,
          message: "unable to process channel message; subscription failed",
        }),
      );
    }
  }

  private handleTurnStart(message: InboundMessage, headers: HeaderMap): void {
    const node = this.tree.applyTurnLifecycle({
      type: "turn-start",
      headers,
      serial: message.deliverySerial ?? message.historySerial,
    });
    const turnId = headers[HEADER_RUN_ID];
    const invocationId = headers[HEADER_INVOCATION_ID];
    if (turnId && invocationId) {
      const own = this.ownTurns.get(turnId);
      if (own && own.invocationId !== invocationId) {
        own.invocationId = invocationId;
        this.router.rebindStream(turnId, invocationId);
      }
      const pending = this.pendingTurnStarts.get(invocationId);
      if (pending) {
        this.resolvePendingTurnStart(invocationId);
      }
    }
    if (node?.status === "suspended" && turnId && invocationId) {
      this.router.rebindStream(turnId, invocationId);
    }
  }

  private handleTurnEnd(message: InboundMessage, headers: HeaderMap): void {
    const turnId = headers[HEADER_RUN_ID];
    const invocationId = headers[HEADER_INVOCATION_ID];
    if (
      turnId &&
      invocationId &&
      this.router.activeInvocation(turnId) !== undefined &&
      this.router.activeInvocation(turnId) !== invocationId
    ) {
      return;
    }
    const node = this.tree.applyTurnLifecycle({
      type: "turn-end",
      headers,
      serial: message.deliverySerial ?? message.historySerial,
    });
    if (!turnId) {
      return;
    }
    const reason = headers[HEADER_RUN_REASON];
    if (reason !== "suspended") {
      this.router.closeStream(turnId);
      this.ownTurns.delete(turnId);
      for (const pending of Array.from(this.pendingTurnStarts.values())) {
        if (pending.turn.turnId === turnId) {
          this.resolvePendingTurnStart(pending.invocationId);
        }
      }
    }
    if (node && reason === "suspended" && invocationId) {
      this.router.rebindStream(turnId, invocationId);
    }
  }

  private handleContinuityLost(error: ErrorInfo): void {
    for (const turnId of this.ownTurns.keys()) {
      this.router.errorStream(turnId, error);
    }
    this.emitError(error);
  }

  private waitForTurnStart(turn: ActiveTurn<TOutput>): Promise<ActiveTurn<TOutput>> {
    if (this.turnStartDeadlineMs === 0) {
      return Promise.resolve(turn);
    }
    return new Promise((resolve, reject) => {
      const pending: PendingTurnStart<TOutput> = {
        invocationId: turn.invocationId,
        resolve,
        reject,
        turn,
        timer: setTimeout(() => {
          this.pendingTurnStarts.delete(turn.invocationId);
          reject(
            new ErrorInfo({
              code: ErrorCode.TurnStartDeadlineExceeded,
              statusCode: 504,
              message: "unable to send; turn start deadline exceeded",
            }),
          );
        }, this.turnStartDeadlineMs),
      };
      this.pendingTurnStarts.set(turn.invocationId, pending);
    });
  }

  private resolvePendingTurnStart(invocationId: string): void {
    const pending = this.pendingTurnStarts.get(invocationId);
    if (!pending) {
      return;
    }
    this.pendingTurnStarts.delete(invocationId);
    if (pending.timer) {
      clearTimeout(pending.timer);
    }
    pending.resolve(pending.turn);
  }

  private poke(context: PokeContext<TInput, TMessage, TOutput>, sendOptions: SendOptions): void {
    const body = {
      id: context.invocationId,
      messages: context.messages,
      inputs: context.inputs,
      history: this.view.getMessages(),
      clientId: this.clientId,
      parent: context.parent,
      forkOf: context.forkOf,
      trigger: context.trigger,
      messageId: context.messageId,
      events: context.staged,
      ...this.bodyProvider(),
      ...sendOptions.body,
      sessionName: this.channel.name,
      turnId: context.turnId,
      invocationId: context.invocationId,
      inputEventId: context.inputEventId,
    };
    const headers = {
      "Content-Type": "application/json",
      ...this.headerProvider(),
      ...sendOptions.headers,
    };
    void this.fetchFn(this.options.api, {
      method: "POST",
      headers,
      body: JSON.stringify(stripUndefined(body)),
      ...(this.options.credentials ? { credentials: this.options.credentials } : {}),
    })
      .then((response) => {
        if (!response.ok) {
          const error = new ErrorInfo({
            code: ErrorCode.TransportSendFailed,
            statusCode: response.status,
            message: `unable to send; HTTP POST returned ${String(response.status)}`,
          });
          this.rejectTurnStart(context.invocationId, error);
          this.router.errorStream(context.turnId, error);
          this.emitError(error);
        }
      })
      .catch((error: unknown) => {
        const info = toErrorInfo(error, {
          code: ErrorCode.TransportSendFailed,
          message: "unable to send; HTTP POST failed",
        });
        this.rejectTurnStart(context.invocationId, info);
        this.router.errorStream(context.turnId, info);
        this.emitError(info);
      });
  }

  private rejectTurnStart(invocationId: string, error: ErrorInfo): void {
    const pending = this.pendingTurnStarts.get(invocationId);
    if (!pending) {
      return;
    }
    this.pendingTurnStarts.delete(invocationId);
    if (pending.timer) {
      clearTimeout(pending.timer);
    }
    pending.reject(error);
  }

  private inputHeaders(options: {
    turnId: string;
    invocationId: string;
    inputEventId: string;
    codecMessageId?: string;
    parent?: string;
    forkOf?: string;
    turnContinue?: boolean;
    regenerates?: string | boolean;
  }): HeaderMap {
    const headers = buildTransportHeaders({
      role: "user",
      turnId: options.turnId,
      invocationId: options.invocationId,
      inputEventId: options.inputEventId,
      ...(options.codecMessageId !== undefined ? { codecMessageId: options.codecMessageId } : {}),
      ...(this.clientId !== undefined
        ? { turnClientId: this.clientId, inputClientId: this.clientId }
        : {}),
      ...(options.parent !== undefined ? { parent: options.parent } : {}),
      ...(options.forkOf !== undefined ? { forkOf: options.forkOf } : {}),
      ...(options.turnContinue !== undefined ? { turnContinue: options.turnContinue } : {}),
      ...(options.regenerates !== undefined ? { regenerates: options.regenerates } : {}),
    });
    return headers;
  }

  private currentParent(): string | undefined {
    const messages = this.view.getMessages();
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const id = messageIdOf(messages[index]);
      if (id !== undefined) {
        return id;
      }
    }
    return undefined;
  }

  private seedMessages(messages: readonly TMessage[]): void {
    let parent: string | undefined;
    for (const message of messages) {
      const msgId = messageIdOf(message) ?? this.idProvider.messageId();
      const turnId = this.idProvider.turnId();
      const headers = buildTransportHeaders({
        role: "user",
        turnId,
        codecMessageId: msgId,
        ...(parent !== undefined ? { parent } : {}),
        ...(this.clientId !== undefined ? { turnClientId: this.clientId } : {}),
      });
      this.tree.applyMessage(
        [decodedEvent<TInput | TOutput>(message as unknown as TInput | TOutput, msgId, "seed")],
        headers,
        "seed",
      );
      parent = msgId;
    }
  }

  private closeMatchingTurnStreams(filter: CancelFilter): void {
    for (const turnId of this.getMatchingTurnIds(filter)) {
      this.router.closeStream(turnId);
    }
  }

  private getMatchingTurnIds(filter: CancelFilter): Set<string> {
    const matched = new Set<string>();
    const active = this.tree.getActiveTurnIds();
    if ("all" in filter && filter.all) {
      for (const turns of active.values()) {
        for (const turnId of turns) {
          matched.add(turnId);
        }
      }
    } else if ("turnId" in filter && filter.turnId) {
      matched.add(filter.turnId);
    } else if ("clientId" in filter && filter.clientId) {
      for (const turnId of active.get(filter.clientId) ?? []) {
        matched.add(turnId);
      }
    } else if ("own" in filter && filter.own) {
      for (const turnId of active.get(this.clientId ?? "") ?? []) {
        matched.add(turnId);
      }
    }
    return matched;
  }

  private rebindContinuation(turnId: string, invocationId: string): ReadableStream<TOutput> {
    this.router.rebindStream(turnId, invocationId);
    return this.router.getStream(turnId) ?? this.router.createStream(turnId, invocationId);
  }

  private emitError(error: ErrorInfo): void {
    this.emitter.emit("error", error);
  }

  private assertOpen(operation: string): void {
    if (this.closed) {
      throw new ErrorInfo({
        code: ErrorCode.TransportClosed,
        statusCode: 400,
        message: `unable to ${operation}; transport is closed`,
      });
    }
  }
}

interface PokeContext<TInput, TMessage, TOutput> {
  turnId: string;
  invocationId: string;
  inputEventId: string;
  parent: string | undefined;
  forkOf: string | undefined;
  trigger: string;
  messageId: string | undefined;
  messages: readonly TMessage[];
  inputs: readonly TInput[];
  staged: readonly StagedEvents<TOutput>[];
}

function normalizeProvider<T extends Record<string, unknown>>(
  value: T | (() => T) | undefined,
): () => T {
  if (typeof value === "function") {
    return value;
  }
  return () => value ?? ({} as T);
}

function assertAiTransportFeature(features: readonly string[] | undefined, logger: Logger): void {
  if (features === undefined) {
    return;
  }
  if (features.includes("ai-transport")) {
    return;
  }
  const error = new ErrorInfo({
    code: ErrorCode.ChannelNotReady,
    statusCode: 501,
    message:
      "unable to create client transport; Sockudo server does not advertise the ai-transport feature",
    detail: { requiredFeature: "ai-transport", features },
  });
  logger.error(error.message, {
    code: error.code,
    requiredFeature: "ai-transport",
    advertisedFeatures: features,
  });
  throw error;
}

function readConnectionFeatures(client: ClientLike | undefined): readonly string[] | undefined {
  const connection = client?.connection as
    | (ClientLike["connection"] & { features?: unknown })
    | undefined;
  if (!Array.isArray(connection?.features)) {
    return undefined;
  }
  return connection.features.filter((feature) => typeof feature === "string");
}

function decodedEvent<TEvent>(
  event: TEvent,
  messageId: string,
  serial: string | number,
): DecodedEvent<TEvent> {
  return { event, messageId, meta: { serial, messageId } };
}

function decodedForFold<TInput, TOutput>(
  batch: DecodedBatch<TInput, TOutput>,
): readonly DecodedEvent<TInput | TOutput>[] {
  if (batch.inputs.length === 0) {
    return batch.outputs;
  }
  if (batch.outputs.length === 0) {
    return batch.inputs;
  }
  return [...batch.inputs, ...batch.outputs] as DecodedEvent<TInput | TOutput>[];
}

function messageIdOf(message: unknown): string | undefined {
  if (message !== null && typeof message === "object" && "id" in message) {
    const id = (message as { id?: unknown }).id;
    return typeof id === "string" ? id : undefined;
  }
  return undefined;
}

function normalizeInputs<TInput>(input: TInput | readonly TInput[]): readonly TInput[] {
  return Array.isArray(input) ? (input as readonly TInput[]) : [input as TInput];
}

function cancelHeaders(filter: CancelFilter, clientId: string | undefined): HeaderMap {
  if ("turnId" in filter && filter.turnId) {
    return buildTransportHeaders({ runId: filter.turnId });
  }
  if ("clientId" in filter && filter.clientId) {
    return buildTransportHeaders({ runClientId: filter.clientId });
  }
  if ("own" in filter && filter.own) {
    return buildTransportHeaders(clientId === undefined ? {} : { inputClientId: clientId });
  }
  return buildTransportHeaders({});
}

function isRunStartMessage(name: string): boolean {
  return name === EVENT_AI_RUN_START || name === EVENT_AI_LEGACY_TURN_START;
}

function isRunEndMessage(name: string): boolean {
  return name === EVENT_AI_RUN_END || name === EVENT_AI_LEGACY_TURN_END;
}

function missingChannel(): ChannelLike {
  throw new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    message: "unable to create client transport; channel could not be resolved",
  });
}

function mapPublishFailure(
  error: unknown,
  fallback: { code: ErrorCode; message: string },
): ErrorInfo {
  const mapped = toErrorInfo(error, fallback);
  const status = statusLike(error) ?? mapped.statusCode;
  if (status === 401 || status === 403 || mapped.code === 401 || mapped.code === 403) {
    return new ErrorInfo({
      code: ErrorCode.InsufficientCapability,
      statusCode: status,
      message: mapped.message,
      cause: error,
      detail: mapped.detail,
    });
  }
  return mapped;
}

function statusLike(value: unknown): number | undefined {
  const record =
    value !== null && typeof value === "object" ? (value as Record<string, unknown>) : undefined;
  const status = record?.status ?? record?.statusCode;
  return typeof status === "number" ? status : undefined;
}
