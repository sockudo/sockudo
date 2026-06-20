import type { HeaderMap } from "../../utils.js";
import type {
  InboundMessage,
  MessageAck,
  MessageMutation,
  PublishMessage,
  Serial,
} from "../../realtime/types.js";

/**
 * Metadata supplied to codec reducers.
 */
export interface ReducerMeta {
  /** Serial used for deterministic idempotency. */
  serial: Serial;
  /** Optional codec message id, equal to Sockudo `message_serial` for streams. */
  messageId?: string;
}

/**
 * Deterministic event reducer used by codec projections.
 *
 * Implementations may mutate `state` and return the same object. Callers must
 * treat the returned value as authoritative.
 */
export interface Reducer<TEvent, TProjection> {
  /** Creates an empty projection. */
  init(): TProjection;
  /** Folds one event into `state`. */
  fold(state: TProjection, event: TEvent, meta: ReducerMeta): TProjection;
}

/**
 * Public codec contract for framework-agnostic AI Transport event handling.
 *
 * @defaultValue Reducers may mutate their projection for hot-path efficiency.
 */
export interface Codec<TInput, TOutput, TProjection, TMessage> extends Reducer<
  TInput | TOutput,
  TProjection
> {
  /** Creates an encoder bound to a channel writer. */
  createEncoder(channel: ChannelWriter, options?: EncoderOptions): Encoder<TInput, TOutput>;
  /** Creates a decoder for inbound Sockudo messages. */
  createDecoder(): Decoder<TInput, TOutput>;
  /** Returns the user-visible messages from a projection. */
  getMessages(projection: TProjection): TMessage[];
  /** Wraps a user-authored message for transport submission. */
  createUserMessage(message: TMessage): UserMessage<TMessage>;
  /** Creates a regenerate command for a target and parent message. */
  createRegenerate(target: string, parent: string): Regenerate;
  /** Resolves a tool-call target from an output and current projection. */
  resolveToolTarget(output: TOutput, projection: TProjection): string | undefined;
  /** Returns whether an output terminates its stream or message. */
  isTerminal(output: TOutput): boolean;
}

/**
 * Docs-compatible two-generic codec alias.
 */
export type Codec2<TEvent, TMessage> = Codec<TEvent, TEvent, MessageProjection<TMessage>, TMessage>;

/**
 * Projection shape used by the docs-compatible codec alias.
 */
export interface MessageProjection<TMessage> {
  /** Mutable message list owned by the codec reducer. */
  messages: TMessage[];
}

/**
 * Encoded user message command.
 */
export interface UserMessage<TMessage> {
  /** Message payload. */
  message: TMessage;
}

/**
 * Encoded regenerate command.
 */
export interface Regenerate {
  /** Target message id. */
  target: string;
  /** Parent message id. */
  parent: string;
}

/**
 * Channel writer consumed by codec encoders.
 *
 * `ChannelLike` satisfies this interface structurally.
 */
export interface ChannelWriter {
  /** Publishes a create/discrete message. */
  publish(message: PublishMessage): Promise<MessageAck>;
  /** Appends string data to a mutable message. */
  appendMessage(
    messageSerial: string,
    data: string,
    options?: Omit<MessageMutation, "data">,
  ): Promise<MessageAck>;
  /** Updates a mutable message aggregate. */
  updateMessage(messageSerial: string, options?: MessageMutation): Promise<MessageAck>;
}

/**
 * Per-write options accepted by public encoders.
 */
export interface WriteOptions {
  /** Verified client id to pass through privileged write paths. */
  clientId?: string;
  /** Extras merged with AI transport and codec tiers. */
  extras?: unknown;
  /** Idempotent message id. */
  messageId?: string;
}

/**
 * Encoder construction options.
 *
 * @defaultValue `extras` defaults to no additional metadata.
 */
export interface EncoderOptions extends WriteOptions {
  /** Hook invoked before each write; exceptions are isolated. */
  onMessage?(message: EncoderOutboundMessage): void;
}

/**
 * Public encoder contract.
 */
export interface Encoder<TInput, TOutput> {
  /** Publishes an input event. */
  publishInput(input: TInput, options?: WriteOptions): Promise<MessageAck>;
  /** Publishes an output event. */
  publishOutput(output: TOutput, options?: WriteOptions): Promise<MessageAck>;
  /** Cancels active streams owned by this encoder. */
  cancel(reason?: string): Promise<void>;
  /** Closes the encoder and flushes active streams. */
  close(): Promise<void>;
}

/**
 * Public decoder contract.
 */
export interface Decoder<TInput, TOutput> {
  /** Decodes one inbound message into typed input and output events. */
  decode(message: InboundMessage): DecodedBatch<TInput, TOutput>;
}

/**
 * Decoded event plus deterministic fold metadata.
 */
export interface DecodedEvent<TEvent> {
  /** Decoded payload. */
  event: TEvent;
  /** Message serial used as the codec message id. */
  messageId?: string;
  /** Fold metadata. */
  meta: ReducerMeta;
}

/**
 * Decoder result split by event direction.
 */
export interface DecodedBatch<TInput, TOutput> {
  /** Decoded input events. */
  inputs: DecodedEvent<TInput>[];
  /** Decoded output events. */
  outputs: DecodedEvent<TOutput>[];
}

/**
 * Mutable outbound write envelope passed to encoder hooks.
 */
export interface EncoderOutboundMessage {
  /** Write kind. */
  kind: "publish" | "append" | "update";
  /** Mutable publish payload for create/discrete writes. */
  publish?: PublishMessage;
  /** Target mutable message serial for append/update writes. */
  messageSerial?: string;
  /** Append data. */
  data?: string;
  /** Mutable mutation options for append/update writes. */
  mutation?: MessageMutation | Omit<MessageMutation, "data">;
}

/**
 * Message accumulator compatible with the documented Ably codec shape.
 */
export interface MessageAccumulator<TOutput, TMessage> {
  /** Current visible messages. */
  readonly messages: TMessage[];
  /** Messages that have received a terminal output. */
  readonly completedMessages: TMessage[];
  /** Whether at least one stream is active. */
  readonly hasActiveStream: boolean;
  /** Processes decoded output events through the underlying codec reducer. */
  processOutputs(outputs: readonly DecodedEvent<TOutput>[]): void;
  /** Updates a message idempotently. */
  updateMessage(message: TMessage): void;
  /** Initializes a message when it is not already present. */
  initMessage(message: TMessage): void;
  /** Marks a message complete when it is currently active. */
  completeMessage(message: TMessage): void;
}

/**
 * Options for {@link createAccumulator}.
 */
export interface CreateAccumulatorOptions<TMessage> {
  /** Returns a stable message id. */
  getMessageId?(message: TMessage): string | undefined;
}

/**
 * Creates the documented accumulator adapter over `fold` and `getMessages`.
 */
export function createAccumulator<TInput, TOutput, TProjection, TMessage>(
  codec: Codec<TInput, TOutput, TProjection, TMessage>,
  options: CreateAccumulatorOptions<TMessage> = {},
): MessageAccumulator<TOutput, TMessage> {
  let projection = codec.init();
  const active = new Set<string>();
  const completed = new Set<string>();
  const manualMessages = new Map<string, TMessage>();
  const getId = (message: TMessage): string | undefined =>
    options.getMessageId?.(message) ?? defaultMessageId(message);
  const accumulator: MessageAccumulator<TOutput, TMessage> = {
    get messages(): TMessage[] {
      const messages = codec.getMessages(projection);
      if (manualMessages.size === 0) {
        return messages;
      }
      const byId = new Map<string, TMessage>();
      for (const message of messages) {
        const id = getId(message);
        if (id !== undefined) {
          byId.set(id, message);
        }
      }
      for (const [id, message] of manualMessages) {
        if (!byId.has(id)) {
          messages.push(message);
        }
      }
      return messages;
    },
    get completedMessages(): TMessage[] {
      return accumulator.messages.filter((message) => {
        const id = getId(message);
        return id !== undefined && completed.has(id);
      });
    },
    get hasActiveStream(): boolean {
      return active.size > 0;
    },
    processOutputs(outputs) {
      for (const output of outputs) {
        projection = codec.fold(projection, output.event, output.meta);
        const id = output.messageId;
        if (id !== undefined) {
          if (codec.isTerminal(output.event)) {
            active.delete(id);
            completed.add(id);
          } else if (!completed.has(id)) {
            active.add(id);
          }
        }
      }
    },
    updateMessage(message) {
      const id = getId(message);
      if (id !== undefined) {
        manualMessages.set(id, message);
      }
    },
    initMessage(message) {
      const id = getId(message);
      if (id !== undefined && !manualMessages.has(id)) {
        manualMessages.set(id, message);
        active.add(id);
      }
    },
    completeMessage(message) {
      const id = getId(message);
      if (id !== undefined && active.has(id)) {
        active.delete(id);
        completed.add(id);
      }
    },
  };
  return accumulator;
}

function defaultMessageId(message: unknown): string | undefined {
  if (message !== null && typeof message === "object" && "id" in message) {
    const id = (message as { id?: unknown }).id;
    return typeof id === "string" ? id : undefined;
  }
  return undefined;
}

/**
 * Compile-time structural assertion that a type satisfies {@link ChannelWriter}.
 */
export type AssertChannelWriter<T extends ChannelWriter> = T;

/**
 * AI codec transport headers carried with stream writes.
 */
export interface CodecHeaderSet {
  /** Transport-tier headers. */
  transport?: HeaderMap;
  /** Codec-tier headers. */
  codec?: HeaderMap;
}
