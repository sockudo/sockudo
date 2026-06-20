import { ErrorCode, ErrorInfo, toErrorInfo } from "../../errors.js";
import type {
  ActiveTurn,
  ClientTransport,
  CloseOptions,
  SendOptions,
} from "../../core/transport/index.js";
import type {
  AI,
  ToolApprovalResponse,
  ToolResult,
  ToolResultError,
  VercelInput,
  VercelOutput,
  VercelProjection,
} from "../codec/index.js";

/**
 * Context passed to `prepareSendMessagesRequest`.
 */
export interface SendMessagesRequestContext {
  /** Chat id from Vercel `useChat`, when supplied. */
  chatId?: string;
  /** Vercel send trigger. */
  trigger: "submit-message" | "regenerate-message";
  /** Message id for edit or regenerate requests. */
  messageId?: string;
  /** Historical messages sent in the default POST body. */
  history: readonly AI.UIMessage[];
  /** New messages sent in the default POST body. */
  messages: readonly AI.UIMessage[];
  /** Codec message id that the new branch forks from. */
  forkOf?: string;
  /** Parent codec message id for the new branch. */
  parent?: string;
}

/**
 * Request override returned by `prepareSendMessagesRequest`.
 */
export interface PreparedSendMessagesRequest {
  /** POST body fields merged over the default Vercel/Sockudo body. */
  body?: Record<string, unknown>;
  /** POST headers merged over Vercel request headers. */
  headers?: Record<string, string> | HeadersInit;
}

/**
 * Optional Vercel chat adapter hooks.
 */
export interface ChatTransportOptions {
  /** Customizes the POST body/headers after Sockudo derives branch metadata. */
  prepareSendMessagesRequest?(ctx: SendMessagesRequestContext): PreparedSendMessagesRequest;
}

/**
 * Vercel `useChat` send options consumed structurally by this adapter.
 */
export interface ChatTransportSendMessagesOptions {
  /** Vercel send trigger. */
  trigger: "submit-message" | "regenerate-message";
  /** Chat id supplied by `useChat`. */
  chatId?: string;
  /** Message id for edit or regenerate requests. */
  messageId?: string;
  /** Current Vercel UI message overlay. */
  messages: AI.UIMessage[];
  /** Abort signal owned by `useChat.stop()`. */
  abortSignal?: AbortSignal;
  /** Additional POST headers from `useChat`. */
  headers?: Record<string, string> | HeadersInit;
  /** Additional POST body fields from `useChat`. */
  body?: Record<string, unknown>;
  /** Request metadata passed through by Vercel; not sent by default. */
  metadata?: unknown;
}

/**
 * Vercel `reconnectToStream` options consumed structurally by this adapter.
 */
export interface ChatTransportReconnectOptions {
  /** Chat id supplied by `useChat`. */
  chatId?: string;
  /** Additional POST headers from `useChat`. */
  headers?: Record<string, string> | HeadersInit;
  /** Additional POST body fields from `useChat`. */
  body?: Record<string, unknown>;
  /** Request metadata passed through by Vercel; not sent by default. */
  metadata?: unknown;
}

/**
 * Chat transport that structurally satisfies Vercel AI SDK `ChatTransport` and
 * exposes Ably-compatible Sockudo streaming lifecycle helpers.
 */
export interface ChatTransport {
  /** Whether this transport currently owns an active stream. */
  readonly streaming: boolean;
  /** Sends a Vercel chat request and returns Sockudo-routed UI chunks. */
  sendMessages(
    options: ChatTransportSendMessagesOptions,
  ): Promise<ReadableStream<AI.UIMessageChunk>>;
  /** Returns `null`; Sockudo channel observation handles in-progress streams. */
  reconnectToStream(
    options: ChatTransportReconnectOptions,
  ): Promise<ReadableStream<AI.UIMessageChunk> | null>;
  /** Closes the underlying client transport. */
  close(options?: CloseOptions): Promise<void>;
  /** Subscribes to owned-streaming flag changes. */
  onStreamingChange(cb: (streaming: boolean) => void): () => void;
}

type VercelClientTransport = ClientTransport<
  VercelInput,
  VercelOutput,
  VercelProjection,
  AI.UIMessage
>;

interface SendDecision {
  history: readonly AI.UIMessage[];
  messages: readonly AI.UIMessage[];
  parent?: string;
  forkOf?: string;
  active: Promise<ActiveTurn<VercelOutput>>;
}

const unresolvedToolStates = new Set<AI.DynamicToolState>([
  "input-streaming",
  "input-available",
  "approval-requested",
]);

/**
 * Creates a Vercel `useChat` transport over a Sockudo client transport.
 */
export function createChatTransport(
  transport: VercelClientTransport,
  chatOptions: ChatTransportOptions = {},
): ChatTransport {
  return new SockudoChatTransport(transport, chatOptions);
}

/**
 * Derives continuation inputs by diffing Vercel's optimistic overlay against
 * the assistant message currently materialized by Sockudo's conversation tree.
 */
export function deriveContinuationInputs(
  overlay: AI.UIMessage,
  treeMessage: AI.UIMessage,
): VercelInput[] {
  const baseParts = dynamicToolPartsByCallId(treeMessage);
  const inputs: VercelInput[] = [];
  for (const part of overlay.parts) {
    if (part.type !== "dynamic-tool") {
      continue;
    }
    const base = baseParts.get(part.toolCallId);
    if (part.state === "output-available") {
      if (base?.state !== "output-available") {
        inputs.push({
          type: "tool-result",
          toolCallId: part.toolCallId,
          output: part.output,
        } satisfies ToolResult);
      }
      continue;
    }
    if (part.state === "output-error") {
      if (base?.state !== "output-error") {
        inputs.push({
          type: "tool-result-error",
          toolCallId: part.toolCallId,
          message: part.errorText ?? "tool result failed",
        } satisfies ToolResultError);
      }
      continue;
    }
    if (part.state === "approval-responded") {
      const overlayApproval = approvalRecord(part.approval);
      const baseApproval = approvalRecord(base?.approval);
      if (
        overlayApproval &&
        (base?.state !== "approval-responded" || approvalDiffers(baseApproval, overlayApproval))
      ) {
        const response: ToolApprovalResponse = {
          type: "tool-approval-response",
          toolCallId: part.toolCallId,
          approved: overlayApproval.approved,
        };
        if (overlayApproval.id !== undefined) {
          response.approvalId = overlayApproval.id;
        }
        if (overlayApproval.reason !== undefined) {
          response.reason = overlayApproval.reason;
        }
        inputs.push(response);
      }
    }
  }
  return inputs;
}

class SockudoChatTransport implements ChatTransport {
  private readonly streamingListeners = new Set<(streaming: boolean) => void>();
  private streamingValue = false;

  public constructor(
    private readonly transport: VercelClientTransport,
    private readonly options: ChatTransportOptions,
  ) {}

  public get streaming(): boolean {
    return this.streamingValue;
  }

  public async sendMessages(
    options: ChatTransportSendMessagesOptions,
  ): Promise<ReadableStream<AI.UIMessageChunk>> {
    if (options.abortSignal?.aborted) {
      throw new ErrorInfo({
        code: ErrorCode.TransportSendFailed,
        statusCode: 499,
        message: "unable to send; request was already aborted",
      });
    }

    this.setStreaming(true);
    let active: ActiveTurn<VercelOutput>;
    try {
      const decision = this.decide(options);
      active = await decision.active;
    } catch (error) {
      this.setStreaming(false);
      throw error;
    }
    const abort = (): void => {
      void active.cancel();
    };
    if (options.abortSignal?.aborted) {
      await active.cancel();
    } else {
      options.abortSignal?.addEventListener("abort", abort, { once: true });
    }
    return this.wrapOwnStream(active.stream, () => {
      options.abortSignal?.removeEventListener("abort", abort);
    });
  }

  public reconnectToStream(): Promise<ReadableStream<AI.UIMessageChunk> | null> {
    return Promise.resolve(null);
  }

  public close(options?: CloseOptions): Promise<void> {
    return this.transport.close(options);
  }

  public onStreamingChange(cb: (streaming: boolean) => void): () => void {
    this.streamingListeners.add(cb);
    return () => {
      this.streamingListeners.delete(cb);
    };
  }

  private decide(options: ChatTransportSendMessagesOptions): SendDecision {
    try {
      if (options.trigger === "regenerate-message") {
        return this.decideRegenerate(options);
      }
      return this.decideSubmit(options);
    } catch (error) {
      throw toErrorInfo(error, {
        code: ErrorCode.InvalidArgument,
        message: "unable to send; invalid Vercel chat request",
      });
    }
  }

  private decideRegenerate(options: ChatTransportSendMessagesOptions): SendDecision {
    const target = options.messageId ?? lastAssistantId(options.messages);
    if (target === undefined) {
      throw invalid("unable to regenerate; messageId is required");
    }
    const parent = predecessorId(options.messages, target) ?? target;
    return {
      history: options.messages,
      messages: [],
      parent,
      forkOf: target,
      active: this.transport.view.regenerate(
        target,
        parent,
        this.sendOptions(options, {
          messageId: target,
          parent,
          forkOf: target,
          trigger: "regenerate-message",
          messages: [],
          history: options.messages,
        }),
      ) as Promise<ActiveTurn<VercelOutput>>,
    };
  }

  private decideSubmit(options: ChatTransportSendMessagesOptions): SendDecision {
    const last = options.messages.at(-1);
    if (last === undefined) {
      throw invalid("unable to submit; at least one message is required");
    }
    if (last.role === "assistant") {
      const treeMessage = messageById(this.transport.view.getMessages(), last.id);
      const metadata = this.transport.view.getMessageMetadata(last.id);
      if (treeMessage && metadata) {
        const inputs = deriveContinuationInputs(last, treeMessage);
        return {
          history: options.messages,
          messages: [],
          parent: last.id,
          forkOf: last.id,
          active: this.transport.view.sendInput(
            inputs,
            this.sendOptions(options, {
              messageId: metadata.codecMessageId,
              turnId: metadata.turnId,
              parent: last.id,
              forkOf: last.id,
              trigger: "submit-message",
              messages: [],
              history: options.messages,
            }),
          ) as Promise<ActiveTurn<VercelOutput>>,
        };
      }
    }
    if (last.role !== "user") {
      throw invalid("unable to submit; last message must be a user message");
    }

    const fork = forkOnUnresolvedTool(options.messages);
    const history = fork ? options.messages.slice(0, -2) : options.messages.slice(0, -1);
    const parent = options.messageId
      ? predecessorId(options.messages, options.messageId)
      : fork?.parent;
    const forkOf = options.messageId ?? fork?.forkOf;
    const sendOptions = this.sendOptions(options, {
      ...(options.messageId !== undefined ? { messageId: options.messageId } : {}),
      ...(parent !== undefined ? { parent } : {}),
      ...(forkOf !== undefined ? { forkOf } : {}),
      trigger: "submit-message",
      messages: [last],
      history,
    });
    const active =
      options.messageId !== undefined
        ? this.transport.view.edit(options.messageId, last, sendOptions)
        : this.transport.view.send(last, sendOptions);
    return {
      history,
      messages: [last],
      ...(parent !== undefined ? { parent } : {}),
      ...(forkOf !== undefined ? { forkOf } : {}),
      active: active as Promise<ActiveTurn<VercelOutput>>,
    };
  }

  private sendOptions(
    options: ChatTransportSendMessagesOptions,
    context: SendMessagesRequestContext & { turnId?: string },
  ): SendOptions {
    const preparedContext = stripTurnId({
      ...context,
      ...(options.chatId !== undefined ? { chatId: options.chatId } : {}),
    });
    const prepared = this.options.prepareSendMessagesRequest?.(preparedContext) ?? {};
    const body: Record<string, unknown> = {
      ...(options.chatId !== undefined ? { id: options.chatId } : {}),
      messages: context.messages,
      history: context.history,
      ...(context.parent !== undefined ? { parent: context.parent } : {}),
      ...(context.forkOf !== undefined ? { forkOf: context.forkOf } : {}),
      trigger: context.trigger,
      ...(context.messageId !== undefined ? { messageId: context.messageId } : {}),
      ...options.body,
      ...prepared.body,
    };
    const headers = {
      ...headersToRecord(options.headers),
      ...headersToRecord(prepared.headers),
    };
    return {
      body,
      headers,
      waitForTurnStart: false,
      ...(context.turnId !== undefined ? { turnId: context.turnId } : {}),
      ...(context.parent !== undefined ? { parent: context.parent } : {}),
      ...(context.forkOf !== undefined ? { forkOf: context.forkOf } : {}),
      trigger: context.trigger,
      ...(context.messageId !== undefined ? { messageId: context.messageId } : {}),
    };
  }

  private wrapOwnStream(
    source: ReadableStream<VercelOutput>,
    cleanup: () => void,
  ): ReadableStream<AI.UIMessageChunk> {
    this.setStreaming(true);
    let reader: ReadableStreamDefaultReader<VercelOutput> | undefined;
    return new ReadableStream<AI.UIMessageChunk>({
      start: async (controller) => {
        reader = source.getReader();
        try {
          for (;;) {
            const next = await reader.read();
            if (next.done) {
              controller.close();
              return;
            }
            controller.enqueue(next.value);
          }
        } catch (error) {
          controller.error(
            toErrorInfo(error, {
              code: ErrorCode.StreamError,
              message: "unable to stream chat response; stream failed",
            }),
          );
        } finally {
          reader.releaseLock();
          cleanup();
          this.setStreaming(false);
        }
      },
      cancel: async (reason) => {
        cleanup();
        this.setStreaming(false);
        await reader?.cancel(reason);
      },
    });
  }

  private setStreaming(streaming: boolean): void {
    if (this.streamingValue === streaming) {
      return;
    }
    this.streamingValue = streaming;
    for (const listener of this.streamingListeners) {
      listener(streaming);
    }
  }
}

function stripTurnId(
  context: SendMessagesRequestContext & { turnId?: string },
): SendMessagesRequestContext {
  const result: SendMessagesRequestContext = {
    trigger: context.trigger,
    history: context.history,
    messages: context.messages,
  };
  if (context.chatId !== undefined) {
    result.chatId = context.chatId;
  }
  if (context.messageId !== undefined) {
    result.messageId = context.messageId;
  }
  if (context.parent !== undefined) {
    result.parent = context.parent;
  }
  if (context.forkOf !== undefined) {
    result.forkOf = context.forkOf;
  }
  return result;
}

function forkOnUnresolvedTool(
  messages: readonly AI.UIMessage[],
): { forkOf: string; parent?: string } | undefined {
  const previous = messages.at(-2);
  if (previous?.role !== "assistant" || !hasUnresolvedToolPart(previous)) {
    return undefined;
  }
  const parent = predecessorId(messages, previous.id);
  return parent === undefined ? { forkOf: previous.id } : { forkOf: previous.id, parent };
}

function hasUnresolvedToolPart(message: AI.UIMessage): boolean {
  return message.parts.some(
    (part) => part.type === "dynamic-tool" && unresolvedToolStates.has(part.state),
  );
}

function predecessorId(messages: readonly AI.UIMessage[], messageId: string): string | undefined {
  const index = messages.findIndex((message) => message.id === messageId);
  if (index <= 0) {
    return undefined;
  }
  return messages[index - 1]?.id;
}

function lastAssistantId(messages: readonly AI.UIMessage[]): string | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.role === "assistant") {
      return message.id;
    }
  }
  return undefined;
}

function messageById(
  messages: readonly AI.UIMessage[],
  messageId: string,
): AI.UIMessage | undefined {
  return messages.find((message) => message.id === messageId);
}

function dynamicToolPartsByCallId(
  message: AI.UIMessage,
): Map<string, Extract<AI.UIMessagePart, { type: "dynamic-tool" }>> {
  const parts = new Map<string, Extract<AI.UIMessagePart, { type: "dynamic-tool" }>>();
  for (const part of message.parts) {
    if (part.type === "dynamic-tool") {
      parts.set(part.toolCallId, part);
    }
  }
  return parts;
}

function approvalRecord(
  value: unknown,
): { id?: string; approved: boolean; reason?: string } | undefined {
  if (value === undefined || value === null || typeof value !== "object") {
    return undefined;
  }
  const record = value as Record<string, unknown>;
  if (typeof record.approved !== "boolean") {
    return undefined;
  }
  const approval = { approved: record.approved };
  const id = record.id ?? record.approvalId;
  return {
    ...approval,
    ...(typeof id === "string" ? { id } : {}),
    ...(typeof record.reason === "string" ? { reason: record.reason } : {}),
  };
}

function approvalDiffers(
  left: { approved: boolean; reason?: string } | undefined,
  right: { approved: boolean; reason?: string },
): boolean {
  if (left === undefined) {
    return true;
  }
  return left.approved !== right.approved || left.reason !== right.reason;
}

function headersToRecord(
  headers: Record<string, string> | HeadersInit | undefined,
): Record<string, string> {
  const result: Record<string, string> = {};
  if (headers === undefined) {
    return result;
  }
  if (headers instanceof Headers) {
    headers.forEach((value, key) => {
      result[key] = value;
    });
    return result;
  }
  if (Array.isArray(headers)) {
    for (const [key, value] of headers) {
      result[key] = value;
    }
    return result;
  }
  for (const [key, value] of Object.entries(headers)) {
    result[key] = value;
  }
  return result;
}

function invalid(message: string): ErrorInfo {
  return new ErrorInfo({
    code: ErrorCode.InvalidArgument,
    statusCode: 400,
    message,
  });
}
