import {
  HEADER_DISCRETE,
  HEADER_ERROR_MESSAGE,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_STREAM_ID,
} from "../../constants.js";
import { ErrorCode, ErrorInfo, toErrorInfo } from "../../errors.js";
import { getCodecHeaders, getTransportHeaders } from "../../utils.js";
import type { HeaderMap } from "../../utils.js";
import type { MessageAck, MessageMutation, PublishMessage } from "../../realtime/types.js";
import type {
  ChannelWriter,
  CodecHeaderSet,
  EncoderOptions,
  EncoderOutboundMessage,
  WriteOptions,
} from "./types.js";

/**
 * Options accepted by encoder-core write operations.
 */
export interface EncoderCoreWriteOptions extends WriteOptions, CodecHeaderSet {
  /** Event name for the create message. */
  name?: string;
  /** Operation id for mutation idempotency. */
  opId?: string;
}

/**
 * Core streaming encoder with Sockudo mutable-message recovery semantics.
 */
export interface EncoderCore {
  /** Publishes one discrete create. */
  publishDiscrete(payload: unknown, options?: EncoderCoreWriteOptions): Promise<MessageAck>;
  /** Publishes discrete creates sequentially. */
  publishDiscreteBatch(
    payloads: readonly unknown[],
    options?: EncoderCoreWriteOptions,
  ): Promise<MessageAck[]>;
  /** Starts a tracked stream and returns the codec message id. */
  startStream(
    streamId: string,
    payload: string,
    options?: EncoderCoreWriteOptions,
  ): Promise<string>;
  /** Appends stream data without awaiting the mutation. */
  appendStream(streamId: string, delta: string, options?: EncoderCoreWriteOptions): void;
  /** Completes a stream, flushing and recovering failed appends. */
  closeStream(streamId: string, options?: EncoderCoreWriteOptions): Promise<void>;
  /** Cancels a stream, flushing and recovering failed appends. */
  cancelStream(streamId: string, reason?: string, options?: EncoderCoreWriteOptions): Promise<void>;
  /** Cancels every active stream. */
  cancelAllStreams(reason?: string): Promise<void>;
  /** Closes all active streams as complete. */
  close(): Promise<void>;
}

/**
 * Creates the framework-agnostic encoder core.
 *
 * @defaultValue Stream append writes are fire-and-forget until close/cancel.
 */
export function createEncoderCore(
  writer: ChannelWriter,
  options: EncoderOptions = {},
): EncoderCore {
  const streams = new Map<string, StreamState>();
  const core: EncoderCore = {
    async publishDiscrete(payload, writeOptions = {}) {
      const publish = buildPublish(payload, options, writeOptions, {
        [HEADER_STREAM]: "false",
        [HEADER_DISCRETE]: "true",
      });
      fireHook(options, { kind: "publish", publish });
      return publishWithError(writer, publish, "publish a discrete message");
    },
    async publishDiscreteBatch(payloads, writeOptions = {}) {
      const acks: MessageAck[] = [];
      for (const payload of payloads) {
        acks.push(await this.publishDiscrete(payload, writeOptions));
      }
      return acks;
    },
    async startStream(streamId, payload, writeOptions = {}) {
      assertString("streamId", streamId);
      assertString("payload", payload);
      const publish = buildPublish(payload, options, writeOptions, {
        [HEADER_STREAM]: "true",
        [HEADER_STATUS]: "streaming",
        [HEADER_STREAM_ID]: streamId,
      });
      fireHook(options, { kind: "publish", publish });
      const ack = await publishWithError(writer, publish, "start a stream");
      if (ack.messageSerial === "") {
        throw new ErrorInfo({
          code: ErrorCode.BadRequest,
          message: "unable to start stream; Sockudo acknowledgement did not include messageSerial",
          detail: ack,
        });
      }
      const transport = mergeHeaderMap(getTransportHeaders(publish.extras), writeOptions.transport);
      const codec = mergeHeaderMap(getCodecHeaders(publish.extras), writeOptions.codec);
      streams.set(streamId, {
        serial: ack.messageSerial,
        accumulated: payload,
        persistentTransport: transport,
        persistentCodec: codec,
        pending: [],
        cancelled: false,
        tail: Promise.resolve(undefined),
      });
      return ack.messageSerial;
    },
    appendStream(streamId, delta, writeOptions = {}) {
      assertString("streamId", streamId);
      assertString("delta", delta);
      const state = requireStream(streams, streamId);
      if (state.cancelled) {
        return;
      }
      state.accumulated += delta;
      const mutation = buildMutation(options, writeOptions, state, {
        [HEADER_STATUS]: "streaming",
      });
      fireHook(options, {
        kind: "append",
        messageSerial: state.serial,
        data: delta,
        mutation,
      });
      const write = state.tail
        .catch(() => undefined)
        .then(() => appendWithError(writer, state.serial, delta, mutation));
      state.tail = write.catch(() => undefined);
      state.pending.push(write);
    },
    closeStream(streamId, writeOptions = {}) {
      return terminalStream({
        streams,
        writer,
        options,
        streamId,
        status: "complete",
        writeOptions,
      });
    },
    cancelStream(streamId, reason, writeOptions = {}) {
      const terminal: TerminalOptions = {
        streams,
        writer,
        options,
        streamId,
        status: "cancelled",
        writeOptions,
      };
      if (reason !== undefined) {
        terminal.reason = reason;
      }
      return terminalStream(terminal);
    },
    async cancelAllStreams(reason) {
      const ids = Array.from(streams.keys());
      await Promise.all(ids.map((id) => this.cancelStream(id, reason)));
    },
    async close() {
      const ids = Array.from(streams.keys());
      await Promise.all(ids.map((id) => this.closeStream(id)));
    },
  };
  return core;
}

interface StreamState {
  serial: string;
  accumulated: string;
  persistentTransport: HeaderMap;
  persistentCodec: HeaderMap;
  pending: Promise<MessageAck>[];
  tail: Promise<MessageAck | undefined>;
  cancelled: boolean;
  closing?: Promise<void>;
}

interface TerminalOptions {
  streams: Map<string, StreamState>;
  writer: ChannelWriter;
  options: EncoderOptions;
  streamId: string;
  status: "complete" | "cancelled";
  reason?: string;
  writeOptions: EncoderCoreWriteOptions;
}

function terminalStream(input: TerminalOptions): Promise<void> {
  const state = requireStream(input.streams, input.streamId);
  if (state.closing) {
    return state.closing;
  }
  state.cancelled = input.status === "cancelled";
  state.closing = doTerminalStream(input, state).finally(() => {
    input.streams.delete(input.streamId);
  });
  return state.closing;
}

async function doTerminalStream(input: TerminalOptions, state: StreamState): Promise<void> {
  const terminalHeaders: Record<string, string> = {
    [HEADER_STATUS]: input.status,
  };
  if (input.reason !== undefined) {
    terminalHeaders[HEADER_ERROR_MESSAGE] = input.reason;
  }
  const mutation = buildMutation(input.options, input.writeOptions, state, terminalHeaders);
  fireHook(input.options, {
    kind: "update",
    messageSerial: state.serial,
    mutation,
  });
  const terminal = state.tail
    .catch(() => undefined)
    .then(() =>
      updateWithError(input.writer, state.serial, {
        ...mutation,
        data: state.accumulated,
      }),
    );
  state.tail = terminal.catch(() => undefined);
  state.pending.push(terminal);
  const results = await Promise.allSettled(state.pending);
  const terminalResult = results.at(-1);
  if (terminalResult?.status === "fulfilled") {
    return;
  }
  if (!results.some((result) => result.status === "rejected")) {
    return;
  }
  const recovery = buildMutation(input.options, input.writeOptions, state, terminalHeaders);
  const update: MessageMutation = {
    ...recovery,
    data: state.accumulated,
  };
  fireHook(input.options, {
    kind: "update",
    messageSerial: state.serial,
    mutation: update,
  });
  try {
    await input.writer.updateMessage(state.serial, update);
  } catch (error) {
    throw new ErrorInfo({
      code: ErrorCode.EncoderRecoveryFailed,
      message: "unable to recover stream after append failure; updateMessage failed",
      cause: error,
      detail: {
        messageSerial: state.serial,
      },
    });
  }
}

function buildPublish(
  payload: unknown,
  defaults: EncoderOptions,
  options: EncoderCoreWriteOptions,
  transport: HeaderMap,
): PublishMessage {
  const publish: PublishMessage = {
    data: payload,
    extras: buildExtras(defaults.extras ?? options.extras, {
      transport: mergeHeaderMap(
        getTransportHeaders(defaults.extras),
        getTransportHeaders(options.extras),
        options.transport,
        transport,
      ),
      codec: mergeHeaderMap(
        getCodecHeaders(defaults.extras),
        getCodecHeaders(options.extras),
        options.codec,
      ),
    }),
  };
  setOptional(publish, "name", options.name);
  setOptional(publish, "messageId", options.messageId ?? defaults.messageId);
  setOptional(publish, "clientId", options.clientId ?? defaults.clientId);
  setOptional(publish, "opId", options.opId);
  return publish;
}

function buildMutation(
  defaults: EncoderOptions,
  options: EncoderCoreWriteOptions,
  state: StreamState,
  transport: HeaderMap,
): Omit<MessageMutation, "data"> {
  const mutation: Omit<MessageMutation, "data"> = {
    extras: buildExtras(defaults.extras ?? options.extras, {
      transport: mergeHeaderMap(
        state.persistentTransport,
        getTransportHeaders(defaults.extras),
        getTransportHeaders(options.extras),
        options.transport,
        transport,
      ),
      codec: mergeHeaderMap(
        state.persistentCodec,
        getCodecHeaders(defaults.extras),
        getCodecHeaders(options.extras),
        options.codec,
      ),
    }),
  };
  setOptional(mutation, "clientId", options.clientId ?? defaults.clientId);
  setOptional(mutation, "opId", options.opId);
  return mutation;
}

function buildExtras(base: unknown, headers: CodecHeaderSet): unknown {
  const root = cloneRecord(base);
  const ai = cloneRecord(root.ai);
  ai.transport = headers.transport ?? Object.create(null);
  ai.codec = headers.codec ?? Object.create(null);
  root.ai = ai;
  return root;
}

function cloneRecord(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object") {
    return {};
  }
  return { ...(value as Record<string, unknown>) };
}

function mergeHeaderMap(...sources: readonly (HeaderMap | undefined)[]): HeaderMap {
  const merged = Object.create(null) as Record<string, string>;
  for (const source of sources) {
    if (source === undefined) {
      continue;
    }
    for (const [key, value] of Object.entries(source)) {
      merged[key] = value;
    }
  }
  return merged;
}

function fireHook(options: EncoderOptions, message: EncoderOutboundMessage): void {
  try {
    options.onMessage?.(message);
  } catch {
    // Hook failures must not affect delivery or recovery.
  }
}

async function publishWithError(
  writer: ChannelWriter,
  publish: PublishMessage,
  operation: string,
): Promise<MessageAck> {
  try {
    return await writer.publish(publish);
  } catch (error) {
    throw toErrorInfo(error, {
      code: ErrorCode.TransportSendFailed,
      message: `unable to ${operation}; channel publish failed`,
    });
  }
}

function appendWithError(
  writer: ChannelWriter,
  messageSerial: string,
  data: string,
  mutation: Omit<MessageMutation, "data">,
): Promise<MessageAck> {
  try {
    return writer.appendMessage(messageSerial, data, mutation).catch((error: unknown) => {
      throw toErrorInfo(error, {
        code: ErrorCode.TransportSendFailed,
        message: "unable to append stream; channel append failed",
      });
    });
  } catch (error) {
    throw toErrorInfo(error, {
      code: ErrorCode.TransportSendFailed,
      message: "unable to append stream; channel append failed",
    });
  }
}

function updateWithError(
  writer: ChannelWriter,
  messageSerial: string,
  mutation: MessageMutation,
): Promise<MessageAck> {
  try {
    return writer.updateMessage(messageSerial, mutation).catch((error: unknown) => {
      throw toErrorInfo(error, {
        code: ErrorCode.TransportSendFailed,
        message: "unable to update stream; channel update failed",
      });
    });
  } catch (error) {
    throw toErrorInfo(error, {
      code: ErrorCode.TransportSendFailed,
      message: "unable to update stream; channel update failed",
    });
  }
}

function requireStream(streams: Map<string, StreamState>, streamId: string): StreamState {
  const state = streams.get(streamId);
  if (!state) {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message: `unable to write stream; stream '${streamId}' is not active`,
    });
  }
  return state;
}

function assertString(name: string, value: string): void {
  if (value === "") {
    throw new ErrorInfo({
      code: ErrorCode.InvalidArgument,
      message: `unable to encode stream; ${name} must be a non-empty string`,
    });
  }
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
