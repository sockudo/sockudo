import {
  EVENT_AI_INPUT,
  EVENT_AI_OUTPUT,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_FORK_OF,
  HEADER_MSG_REGENERATE,
  HEADER_PARENT,
  HEADER_ROLE,
} from "../../constants.js";
import { createEncoderCore } from "../../core/codec/encoder.js";
import type {
  ChannelWriter,
  Encoder,
  EncoderOptions,
  WriteOptions,
} from "../../core/codec/index.js";
import {
  getTransportHeaders,
  mergeHeaders,
  type HeaderMap,
} from "../../utils.js";
import type {
  AI,
  ToolApprovalResponse,
  ToolResult,
  ToolResultError,
  VercelInput,
  VercelOutput,
} from "./events.js";
import { headersForChunk } from "./headers.js";

let generatedMessageCounter = 0;

/** Creates a Vercel UIMessage encoder over Sockudo mutable-message writes. */
export function createVercelEncoder(
  channel: ChannelWriter,
  options: EncoderOptions = {},
): Encoder<VercelInput, VercelOutput> {
  const core = createEncoderCore(channel, options);
  const activeStreams = new Map<string, string>();
  let activeMessageId = getTransportHeaders(options.extras)[
    HEADER_CODEC_MESSAGE_ID
  ];
  return {
    async publishInput(input, writeOptions = {}) {
      if (isUserMessage(input)) {
        const message = userMessagePayload(input);
        if (!message) {
          throw new TypeError("expected Vercel UI message input");
        }
        const parts = message.parts.length
          ? message.parts
          : [{ type: "text" as const, text: "" }];
        let lastAck = await core.publishDiscrete(
          {
            id: message.id,
            role: message.role,
            part: parts[0],
            metadata: message.metadata,
          },
          inputOptions("user-part", message.id, writeOptions),
        );
        for (let index = 1; index < parts.length; index += 1) {
          lastAck = await core.publishDiscrete(
            {
              id: message.id,
              role: message.role,
              part: parts[index],
              metadata: message.metadata,
            },
            inputOptions("user-part", message.id, writeOptions),
          );
        }
        return lastAck;
      }
      if (isRegenerate(input)) {
        return core.publishDiscrete(
          {},
          {
            name: EVENT_AI_INPUT,
            transport: mergeHeaders(getTransportHeaders(writeOptions.extras), {
              [HEADER_MSG_REGENERATE]: "true",
              [HEADER_FORK_OF]: input.target,
              [HEADER_PARENT]: input.parent,
              [HEADER_ROLE]: "user",
            }),
            codec: { type: "regenerate" },
            ...writeOptions,
          },
        );
      }
      return core.publishDiscrete(inputPayload(input), {
        name: EVENT_AI_INPUT,
        transport: mergeHeaders(getTransportHeaders(writeOptions.extras), {
          [HEADER_ROLE]: "user",
        }),
        codec: inputCodecHeaders(input),
        ...writeOptions,
      });
    },
    async publishOutput(output, writeOptions = {}) {
      const chunkMessageId = messageIdOfChunk(output);
      if (chunkMessageId !== undefined) {
        activeMessageId = chunkMessageId;
      } else if (shouldStartAssistantMessage(output)) {
        activeMessageId =
          getTransportHeaders(writeOptions.extras)[HEADER_CODEC_MESSAGE_ID] ??
          activeMessageId ??
          nextGeneratedMessageId();
      }
      const optionsForOutput = outputOptions(
        output,
        writeOptions,
        activeMessageId,
      );
      if (output.type === "abort") {
        await core.cancelAllStreams("abort");
        return core.publishDiscrete(outputPayload(output), optionsForOutput);
      }
      const stream = streamDescriptor(output);
      if (!stream) {
        const ack = await core.publishDiscrete(
          outputPayload(output),
          optionsForOutput,
        );
        if (output.type === "finish") {
          activeMessageId = undefined;
        }
        return ack;
      }
      const key = stream.key;
      if (stream.op === "start") {
        activeStreams.set(key, key);
        await core.startStream(key, " ", optionsForOutput);
        return {
          messageSerial: writeOptions.messageId ?? key,
          historySerial: "0",
        };
      }
      if (stream.op === "delta") {
        if (!activeStreams.has(key)) {
          return core.publishDiscrete(outputPayload(output), optionsForOutput);
        }
        core.appendStream(key, stream.delta, optionsForOutput);
        return {
          messageSerial: writeOptions.messageId ?? key,
          historySerial: "0",
        };
      }
      if (!activeStreams.has(key)) {
        return core.publishDiscrete(outputPayload(output), optionsForOutput);
      }
      activeStreams.delete(key);
      await core.closeStream(key, optionsForOutput);
      return {
        messageSerial: writeOptions.messageId ?? key,
        historySerial: "0",
      };
    },
    cancel(reason) {
      return core.cancelAllStreams(reason);
    },
    close() {
      return core.close();
    },
  };
}

function inputOptions(
  type: string,
  messageId: string,
  writeOptions: WriteOptions,
): Parameters<ReturnType<typeof createEncoderCore>["publishDiscrete"]>[1] {
  return {
    name: EVENT_AI_INPUT,
    transport: mergeHeaders(getTransportHeaders(writeOptions.extras), {
      [HEADER_ROLE]: "user",
    }),
    codec: { type, messageId },
    ...writeOptions,
  };
}

function outputOptions(
  chunk: AI.UIMessageChunk,
  writeOptions: WriteOptions,
  fallbackMessageId?: string,
): Parameters<ReturnType<typeof createEncoderCore>["publishDiscrete"]>[1] {
  const chunkMessageId = messageIdOfChunk(chunk);
  const codec = { ...headersForChunk(chunk) };
  const messageId = chunkMessageId ?? fallbackMessageId;
  if (messageId !== undefined) {
    codec["message-id"] = messageId;
  }
  return {
    name: EVENT_AI_OUTPUT,
    codec,
    transport: mergeHeaders(
      getTransportHeaders(writeOptions.extras),
      {
        [HEADER_ROLE]: "assistant",
      },
      messageId === undefined ? {} : { [HEADER_CODEC_MESSAGE_ID]: messageId },
    ),
    ...writeOptions,
    ...(chunkMessageId !== undefined ? { messageId: chunkMessageId } : {}),
  };
}

function streamDescriptor(
  chunk: AI.UIMessageChunk,
):
  | { op: "start"; key: string }
  | { op: "delta"; key: string; delta: string }
  | { op: "end"; key: string }
  | undefined {
  switch (chunk.type) {
    case "text-start":
      return { op: "start", key: `text:${chunk.messageId ?? ""}:${chunk.id}` };
    case "text-delta":
      return {
        op: "delta",
        key: `text:${chunk.messageId ?? ""}:${chunk.id}`,
        delta: chunk.delta,
      };
    case "text-end":
      return { op: "end", key: `text:${chunk.messageId ?? ""}:${chunk.id}` };
    case "reasoning-start":
      return {
        op: "start",
        key: `reasoning:${chunk.messageId ?? ""}:${chunk.id}`,
      };
    case "reasoning-delta":
      return {
        op: "delta",
        key: `reasoning:${chunk.messageId ?? ""}:${chunk.id}`,
        delta: chunk.delta,
      };
    case "reasoning-end":
      return {
        op: "end",
        key: `reasoning:${chunk.messageId ?? ""}:${chunk.id}`,
      };
    case "tool-input-start":
      return { op: "start", key: `tool-input:${chunk.toolCallId}` };
    case "tool-input-delta":
      return {
        op: "delta",
        key: `tool-input:${chunk.toolCallId}`,
        delta: chunk.delta,
      };
    case "tool-input-available":
      return { op: "end", key: `tool-input:${chunk.toolCallId}` };
    default:
      return undefined;
  }
}

function outputPayload(chunk: AI.UIMessageChunk): unknown {
  if (isDataChunk(chunk)) {
    return chunk.data;
  }
  switch (chunk.type) {
    case "text-delta":
    case "reasoning-delta":
    case "tool-input-delta":
      return chunk.delta;
    case "error":
      return chunk.errorText;
    case "file":
      return chunk.url;
    case "source-url":
      return chunk.url;
    case "tool-output-available":
      return chunk.output;
    case "tool-input-available":
      return chunk.input ?? "";
    default:
      return chunk;
  }
}

function isDataChunk(
  chunk: AI.UIMessageChunk,
): chunk is Extract<AI.UIMessageChunk, { type: `data-${string}` }> {
  return chunk.type.startsWith("data-");
}

function inputPayload(
  input: ToolResult | ToolResultError | ToolApprovalResponse,
): unknown {
  switch (input.type) {
    case "tool-result":
      return { output: input.output };
    case "tool-result-error":
      return { message: input.message };
    case "tool-approval-response":
      return {
        approved: input.approved,
        reason: input.reason,
        approvalId: input.approvalId,
      };
  }
}

function inputCodecHeaders(
  input: ToolResult | ToolResultError | ToolApprovalResponse,
): HeaderMap {
  switch (input.type) {
    case "tool-result":
      return { type: "tool-result", toolCallId: input.toolCallId };
    case "tool-result-error":
      return { type: "tool-result-error", toolCallId: input.toolCallId };
    case "tool-approval-response":
      return {
        type: "tool-approval-response",
        toolCallId: input.toolCallId,
        approved: input.approved ? "true" : "false",
        ...(input.reason !== undefined ? { reason: input.reason } : {}),
        ...(input.approvalId !== undefined
          ? { approvalId: input.approvalId }
          : {}),
      };
  }
}

function messageIdOfChunk(chunk: AI.UIMessageChunk): string | undefined {
  return "messageId" in chunk ? chunk.messageId : undefined;
}

function shouldStartAssistantMessage(chunk: AI.UIMessageChunk): boolean {
  return (
    chunk.type === "start" ||
    chunk.type === "text-start" ||
    chunk.type === "reasoning-start" ||
    chunk.type === "tool-input-start"
  );
}

function nextGeneratedMessageId(): string {
  generatedMessageCounter += 1;
  return `msg_${Date.now().toString(36)}_${generatedMessageCounter.toString(36)}`;
}

function isUserMessage(input: VercelInput): input is { message: AI.UIMessage } {
  return userMessagePayload(input) !== undefined;
}

function userMessagePayload(input: unknown): AI.UIMessage | undefined {
  const candidate =
    input !== null && typeof input === "object" && "message" in input
      ? input.message
      : input;
  if (
    candidate !== null &&
    typeof candidate === "object" &&
    "parts" in candidate &&
    Array.isArray(candidate.parts)
  ) {
    return candidate as AI.UIMessage;
  }
  return undefined;
}

function isRegenerate(
  input: VercelInput,
): input is { target: string; parent: string } {
  return "target" in input && "parent" in input;
}
