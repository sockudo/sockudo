import { EVENT_AI_INPUT, EVENT_AI_OUTPUT, HEADER_FORK_OF, HEADER_PARENT } from "../../constants.js";
import { createDecoderCore, type DecoderStreamTracker } from "../../core/codec/decoder.js";
import type { DecodedEvent, Decoder } from "../../core/codec/index.js";
import type { InboundMessage } from "../../realtime/index.js";
import type { HeaderMap } from "../../utils.js";
import type { AI, VercelInput, VercelOutput } from "./events.js";
import { chunkType, normalizeVercelHeaders, readJsonHeader } from "./headers.js";

/** Creates the inverse Vercel wire decoder. */
export function createVercelDecoder(): Decoder<VercelInput, VercelOutput> {
  const outputCore = createDecoderCore<VercelOutput>({
    buildStartEvents: (tracker) => [decoded(chunkFromHeaders(tracker, "start"), tracker.message)],
    buildDeltaEvents: (tracker, delta) =>
      delta === "" ? [] : [decoded(chunkFromHeaders(tracker, "delta", delta), tracker.message)],
    buildEndEvents: (tracker, headers) => [
      decoded(chunkFromHeaders(tracker, "end", undefined, headers), tracker.message),
    ],
    decodeDiscrete: (message) => decodeOutputDiscrete(message),
  });
  return {
    decode(message) {
      if (message.name === EVENT_AI_INPUT) {
        return {
          inputs: decodeInput(message),
          outputs: [],
        };
      }
      if (message.name === EVENT_AI_OUTPUT) {
        return {
          inputs: [],
          outputs: outputCore.decode(message),
        };
      }
      return { inputs: [], outputs: [] };
    },
  };
}

function decodeInput(message: InboundMessage): DecodedEvent<VercelInput>[] {
  const headers = normalizeVercelHeaders(message.getCodecHeaders());
  const type = chunkType(headers);
  const messageId = headers.messageId ?? message.messageSerial;
  const rawMessage = userMessagePayload(message.data);
  if (!type && rawMessage) {
    return [
      decoded(
        {
          message: rawMessage,
        },
        message,
      ),
    ];
  }
  if (type === "user-part") {
    const payload = record(message.data);
    return [
      decoded(
        {
          message: {
            id: stringValue(payload.id) ?? messageId,
            role: roleValue(payload.role) ?? "user",
            parts: [partValue(payload.part)],
            ...(payload.metadata !== undefined ? { metadata: payload.metadata } : {}),
          },
        },
        message,
      ),
    ];
  }
  if (type === "tool-result") {
    return [
      decoded(
        {
          type: "tool-result",
          toolCallId: headers.toolCallId ?? "",
          output: record(message.data).output,
        },
        message,
      ),
    ];
  }
  if (type === "tool-result-error") {
    return [
      decoded(
        {
          type: "tool-result-error",
          toolCallId: headers.toolCallId ?? "",
          message: stringValue(record(message.data).message) ?? "",
        },
        message,
      ),
    ];
  }
  if (type === "tool-approval-response") {
    return [
      decoded(
        {
          type: "tool-approval-response",
          toolCallId: headers.toolCallId ?? "",
          approved: headers.approved === "true",
          ...(headers.reason !== undefined ? { reason: headers.reason } : {}),
          ...(headers.approvalId !== undefined ? { approvalId: headers.approvalId } : {}),
        },
        message,
      ),
    ];
  }
  if (type === "regenerate") {
    const transport = message.getTransportHeaders();
    return [
      decoded(
        {
          target: transport[HEADER_FORK_OF] ?? "",
          parent: transport[HEADER_PARENT] ?? "",
        },
        message,
      ),
    ];
  }
  return [];
}

function decodeOutputDiscrete(message: InboundMessage): DecodedEvent<VercelOutput>[] {
  const headers = normalizeVercelHeaders(message.getCodecHeaders());
  const type = chunkType(headers);
  if (!type) {
    return [];
  }
  return [decoded(chunkFromType(type, message.data, headers), message)];
}

function chunkFromHeaders(
  tracker: DecoderStreamTracker,
  phase: "start" | "delta" | "end",
  delta?: string,
  closingHeaders?: HeaderMap,
): VercelOutput {
  const headers = normalizeVercelHeaders(
    phase === "end" && closingHeaders ? closingHeaders : tracker.message.getCodecHeaders(),
  );
  const type = chunkType(headers);
  if (type?.startsWith("text-")) {
    const id = headers.id ?? tracker.messageId;
    const messageId = headers.messageId ?? tracker.messageId;
    if (phase === "start") {
      return chunk({ type: "text-start", id, messageId });
    }
    if (phase === "delta") {
      return chunk({ type: "text-delta", id, delta: delta ?? "", messageId });
    }
    return chunk({ type: "text-end", id, messageId });
  }
  if (type?.startsWith("reasoning-")) {
    const id = headers.id ?? tracker.messageId;
    const messageId = headers.messageId ?? tracker.messageId;
    if (phase === "start") {
      return chunk({ type: "reasoning-start", id, messageId });
    }
    if (phase === "delta") {
      return chunk({
        type: "reasoning-delta",
        id,
        delta: delta ?? "",
        messageId,
      });
    }
    return chunk({ type: "reasoning-end", id, messageId });
  }
  const toolCallId = headers.toolCallId ?? tracker.messageId;
  if (phase === "start") {
    return chunk({
      type: "tool-input-start",
      toolCallId,
      toolName: headers.toolName ?? "tool",
      messageId: headers.messageId,
    });
  }
  if (phase === "delta") {
    return chunk({
      type: "tool-input-delta",
      toolCallId,
      delta: delta ?? "",
      messageId: headers.messageId,
    });
  }
  return chunk({
    type: "tool-input-available",
    toolCallId,
    toolName: headers.toolName,
    input: parseJsonish(tracker.accumulated.trimStart()),
    messageId: headers.messageId,
  });
}

function chunkFromType(type: string, data: unknown, headers: HeaderMap): VercelOutput {
  switch (type) {
    case "start":
      return chunk({
        type: "start",
        messageId: headers.messageId,
        messageMetadata: readJsonHeader(headers, "messageMetadata"),
      });
    case "start-step":
      return { type: "start-step" };
    case "finish-step":
      return { type: "finish-step" };
    case "finish":
      return chunk({
        type: "finish",
        finishReason: headers.finishReason,
        metadata: readJsonHeader(headers, "providerMetadata"),
      });
    case "error":
      return { type: "error", errorText: stringValue(data) ?? "" };
    case "abort":
      return { type: "abort" };
    case "message-metadata":
      return chunk({
        type: "message-metadata",
        messageMetadata: readJsonHeader(headers, "messageMetadata"),
      });
    case "tool-input-error":
      return chunk({
        type: "tool-input-error",
        toolCallId: headers.toolCallId ?? "",
        toolName: headers.toolName,
        errorText: stringValue(record(data).errorText) ?? stringValue(data) ?? "",
        messageId: headers.messageId,
      });
    case "tool-input-available":
      return chunk({
        type,
        toolCallId: headers.toolCallId ?? "",
        toolName: headers.toolName,
        input: data === "" ? undefined : data,
        providerExecuted:
          headers.providerExecuted === undefined ? undefined : headers.providerExecuted === "true",
        preliminary: headers.preliminary === undefined ? undefined : headers.preliminary === "true",
        messageId: headers.messageId,
      });
    case "tool-output-available":
      return chunk({
        type,
        toolCallId: headers.toolCallId ?? "",
        output: data,
        messageId: headers.messageId,
      });
    case "tool-output-error":
      return chunk({
        type,
        toolCallId: headers.toolCallId ?? "",
        errorText: stringValue(record(data).errorText) ?? stringValue(data) ?? "",
        messageId: headers.messageId,
      });
    case "tool-approval-request":
      return chunk({
        type,
        toolCallId: headers.toolCallId ?? "",
        approvalId: headers.approvalId,
        messageId: headers.messageId,
      });
    case "tool-output-denied":
      return chunk({
        type,
        toolCallId: headers.toolCallId ?? "",
        reason: headers.reason,
        messageId: headers.messageId,
      });
    case "file":
      return chunk({
        type,
        url: stringValue(data) ?? "",
        mediaType: headers.mediaType,
        filename: headers.filename,
      });
    case "source-url":
      return chunk({
        type,
        url: stringValue(data) ?? "",
        sourceId: headers.sourceId,
        title: headers.title,
      });
    case "source-document":
      return chunk({
        type,
        sourceId: headers.sourceId,
        title: headers.title,
        mediaType: headers.mediaType,
        filename: headers.filename,
      });
    default:
      if (type.startsWith("data-")) {
        return chunk({
          type,
          data,
          transient: headers.transient === "true",
        });
      }
      return { type: "error", errorText: `unknown chunk type ${type}` };
  }
}

function decoded<TEvent>(event: TEvent, message: InboundMessage): DecodedEvent<TEvent> {
  const messageId =
    normalizeVercelHeaders(message.getCodecHeaders()).messageId ??
    message.getTransportHeaders()["codec-message-id"] ??
    message.messageSerial;
  return {
    event,
    messageId,
    meta: {
      serial: message.deliverySerial ?? message.historySerial,
      messageId,
    },
  };
}

function parseJsonish(value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function userMessagePayload(value: unknown): AI.UIMessage | undefined {
  const candidate =
    value !== null && typeof value === "object" && "message" in value ? value.message : value;
  if (
    candidate !== null &&
    typeof candidate === "object" &&
    typeof (candidate as { id?: unknown }).id === "string" &&
    typeof (candidate as { role?: unknown }).role === "string" &&
    Array.isArray((candidate as { parts?: unknown }).parts)
  ) {
    return candidate as AI.UIMessage;
  }
  return undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function roleValue(value: unknown): AI.UIMessage["role"] | undefined {
  return value === "system" || value === "user" || value === "assistant" || value === "tool"
    ? value
    : undefined;
}

function partValue(value: unknown): AI.UIMessage["parts"][number] {
  const part = record(value);
  return typeof part.type === "string"
    ? (part as AI.UIMessage["parts"][number])
    : { type: "text", text: "" };
}

function chunk(value: Record<string, unknown>): VercelOutput {
  const result: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    if (item !== undefined) {
      result[key] = item;
    }
  }
  return result as VercelOutput;
}
