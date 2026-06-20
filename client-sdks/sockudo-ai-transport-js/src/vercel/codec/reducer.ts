import type { ReducerMeta, Regenerate, UserMessage } from "../../core/codec/index.js";
import type { AI, MessageTrackers, VercelInput, VercelOutput, VercelProjection } from "./events.js";
import { transitionToolPart } from "./tool-transitions.js";

/** Creates an empty Vercel projection. */
export function createVercelProjection(): VercelProjection {
  return {
    messages: [],
    conflictSerials: new Map(),
    trackers: {
      text: new Map(),
      reasoning: new Map(),
      tools: new Map(),
    },
    pendingToolResolutions: new Map(),
  };
}

/** Folds one Vercel input or output event into the projection. */
export function foldVercelEvent(
  projection: VercelProjection,
  event: VercelInput | VercelOutput,
  meta: ReducerMeta,
): VercelProjection {
  if (isUserMessage(event)) {
    const message = userMessagePayload(event);
    if (message) {
      foldUserMessage(projection, message, meta);
    }
  } else if (isRegenerate(event)) {
    // Regenerate is wire-only; branch metadata is handled by the tree.
  } else if (isToolResultInput(event)) {
    applyToolResolution(
      projection,
      {
        type: "tool-output-available",
        toolCallId: event.toolCallId,
        output: event.output,
      },
      meta,
    );
  } else if (isToolResultErrorInput(event)) {
    applyToolResolution(
      projection,
      {
        type: "tool-output-error",
        toolCallId: event.toolCallId,
        errorText: event.message,
      },
      meta,
    );
  } else if (isToolApprovalResponseInput(event)) {
    applyToolApprovalResponse(projection, event, meta);
  } else {
    foldOutput(projection, event, meta);
  }
  retryPendingToolResolutions(projection);
  return projection;
}

function foldUserMessage(
  projection: VercelProjection,
  message: AI.UIMessage,
  meta: ReducerMeta,
): void {
  if (!wins(projection, `user-msg:${message.id}`, meta)) {
    return;
  }
  const index = projection.messages.findIndex((item) => item.id === message.id);
  if (index === -1) {
    projection.messages.push(message);
  } else {
    projection.messages[index] = message;
  }
}

function foldOutput(projection: VercelProjection, chunk: VercelOutput, meta: ReducerMeta): void {
  const messageId = messageIdFor(chunk, meta);
  switch (chunk.type) {
    case "start":
      ensureMessage(projection, messageId, chunk.messageMetadata);
      return;
    case "start-step":
      ensureMessage(projection, messageId);
      return;
    case "finish-step":
      projection.trackers.text.delete(messageId);
      projection.trackers.reasoning.delete(messageId);
      return;
    case "finish":
      if (wins(projection, `finish:${messageId}`, meta)) {
        ensureMessage(projection, messageId).metadata = chunk.metadata;
      }
      return;
    case "message-metadata":
      ensureMessage(projection, messageId).metadata = chunk.messageMetadata;
      return;
    case "text-start":
      startText(projection, messageId, chunk.id, "text");
      return;
    case "text-delta":
      appendText(projection, messageId, chunk.id, "text", chunk.delta);
      return;
    case "text-end":
      return;
    case "reasoning-start":
      startText(projection, messageId, chunk.id, "reasoning");
      return;
    case "reasoning-delta":
      appendText(projection, messageId, chunk.id, "reasoning", chunk.delta);
      return;
    case "reasoning-end":
      return;
    case "tool-input-start":
      startTool(projection, messageId, chunk);
      return;
    case "tool-input-delta":
      appendToolInput(projection, chunk.toolCallId, chunk.delta);
      return;
    case "tool-input-available":
      finishToolInput(projection, messageId, chunk);
      return;
    case "tool-input-error":
      applyToolPart(projection, messageId, chunk.toolCallId, {
        state: "output-error",
        errorText: chunk.errorText,
        ...(chunk.toolName !== undefined ? { toolName: chunk.toolName } : {}),
      });
      return;
    case "tool-approval-request":
      applyToolPart(projection, messageId, chunk.toolCallId, {
        state: "approval-requested",
        approval: {
          approved: false,
          ...(chunk.approvalId !== undefined
            ? { id: chunk.approvalId, approvalId: chunk.approvalId }
            : {}),
        },
      });
      return;
    case "tool-output-available":
    case "tool-output-error":
    case "tool-output-denied":
      applyToolResolution(projection, chunk, meta);
      return;
    case "file":
      ensureMessage(projection, messageId).parts.push({
        type: "file",
        url: chunk.url,
        ...(chunk.mediaType !== undefined ? { mediaType: chunk.mediaType } : {}),
        ...(chunk.filename !== undefined ? { filename: chunk.filename } : {}),
      });
      return;
    case "source-url":
      ensureMessage(projection, messageId).parts.push({
        type: "source-url",
        url: chunk.url,
        ...(chunk.sourceId !== undefined ? { sourceId: chunk.sourceId } : {}),
        ...(chunk.title !== undefined ? { title: chunk.title } : {}),
      });
      return;
    case "source-document":
      ensureMessage(projection, messageId).parts.push({
        type: "source-document",
        ...(chunk.sourceId !== undefined ? { sourceId: chunk.sourceId } : {}),
        ...(chunk.title !== undefined ? { title: chunk.title } : {}),
        ...(chunk.mediaType !== undefined ? { mediaType: chunk.mediaType } : {}),
        ...(chunk.filename !== undefined ? { filename: chunk.filename } : {}),
      });
      return;
    case "error":
      if (wins(projection, `finish:${messageId}`, meta)) {
        ensureMessage(projection, messageId).parts.push({
          type: "data-error",
          data: chunk.errorText,
        });
      }
      return;
    case "abort":
      if (wins(projection, `finish:${messageId}`, meta)) {
        ensureMessage(projection, messageId).parts.push({
          type: "data-abort",
          data: true,
        });
      }
      return;
    default:
      if (chunk.type.startsWith("data-") && !chunk.transient) {
        ensureMessage(projection, messageId).parts.push({
          type: chunk.type,
          data: chunk.data,
        });
      }
  }
}

function startText(
  projection: VercelProjection,
  messageId: string,
  streamId: string,
  family: "text" | "reasoning",
): void {
  const message = ensureMessage(projection, messageId);
  const part =
    family === "text"
      ? ({ type: "text", id: streamId, text: "" } satisfies AI.UIMessagePart)
      : ({
          type: "reasoning",
          id: streamId,
          text: "",
        } satisfies AI.UIMessagePart);
  const index = message.parts.push(part) - 1;
  trackerMap(projection.trackers, family, messageId).set(streamId, index);
}

function appendText(
  projection: VercelProjection,
  messageId: string,
  streamId: string,
  family: "text" | "reasoning",
  delta: string,
): void {
  const message = ensureMessage(projection, messageId);
  let index = trackerMap(projection.trackers, family, messageId).get(streamId);
  if (index === undefined) {
    startText(projection, messageId, streamId, family);
    index = trackerMap(projection.trackers, family, messageId).get(streamId);
  }
  const part = index === undefined ? undefined : message.parts[index];
  if (part?.type === family) {
    part.text += delta;
  }
}

function startTool(
  projection: VercelProjection,
  messageId: string,
  chunk: Extract<VercelOutput, { type: "tool-input-start" }>,
): void {
  const message = ensureMessage(projection, messageId);
  const part: AI.UIMessagePart = {
    type: "dynamic-tool",
    toolName: chunk.toolName,
    toolCallId: chunk.toolCallId,
    state: "input-streaming",
  };
  const partIndex = message.parts.push(part) - 1;
  projection.trackers.tools.set(chunk.toolCallId, {
    messageId,
    partIndex,
    inputText: "",
  });
}

function appendToolInput(projection: VercelProjection, toolCallId: string, delta: string): void {
  const tracker = projection.trackers.tools.get(toolCallId);
  if (!tracker) {
    return;
  }
  tracker.inputText += delta;
}

function finishToolInput(
  projection: VercelProjection,
  messageId: string,
  chunk: Extract<VercelOutput, { type: "tool-input-available" }>,
): void {
  const tracker = projection.trackers.tools.get(chunk.toolCallId);
  const input = chunk.input ?? parseJsonish(tracker?.inputText ?? "");
  applyToolPart(projection, tracker?.messageId ?? messageId, chunk.toolCallId, {
    state: "input-available",
    input,
    ...(chunk.toolName !== undefined ? { toolName: chunk.toolName } : {}),
  });
}

function applyToolResolution(
  projection: VercelProjection,
  chunk: Extract<
    VercelOutput,
    {
      type: "tool-output-available" | "tool-output-error" | "tool-output-denied";
    }
  >,
  meta: ReducerMeta,
): void {
  if (!wins(projection, `tool-output:${chunk.toolCallId}`, meta)) {
    return;
  }
  const tracker = projection.trackers.tools.get(chunk.toolCallId);
  const messageId = "messageId" in chunk ? chunk.messageId : undefined;
  const targetMessageId = tracker?.messageId ?? messageId;
  if (!targetMessageId) {
    bufferPending(projection, chunk.toolCallId, chunk, meta);
    return;
  }
  applyToolResolutionPart(projection, targetMessageId, chunk);
}

function applyToolResolutionPart(
  projection: VercelProjection,
  targetMessageId: string,
  chunk: Extract<
    VercelOutput,
    {
      type: "tool-output-available" | "tool-output-error" | "tool-output-denied";
    }
  >,
): void {
  if (chunk.type === "tool-output-available") {
    applyToolPart(
      projection,
      targetMessageId,
      chunk.toolCallId,
      {
        state: "output-available",
        output: chunk.output,
      },
      true,
    );
  } else if (chunk.type === "tool-output-error") {
    applyToolPart(
      projection,
      targetMessageId,
      chunk.toolCallId,
      {
        state: "output-error",
        errorText: chunk.errorText,
      },
      true,
    );
  } else {
    applyToolPart(
      projection,
      targetMessageId,
      chunk.toolCallId,
      {
        state: "output-denied",
        ...(chunk.reason !== undefined ? { errorText: chunk.reason } : {}),
      },
      true,
    );
  }
}

function applyToolApprovalResponse(
  projection: VercelProjection,
  event: Extract<VercelInput, { type: "tool-approval-response" }>,
  meta: ReducerMeta,
): void {
  if (!wins(projection, `tool-approval:${event.toolCallId}`, meta)) {
    return;
  }
  const tracker = projection.trackers.tools.get(event.toolCallId);
  const targetMessageId = tracker?.messageId ?? meta.messageId;
  if (!targetMessageId) {
    return;
  }
  applyToolPart(projection, targetMessageId, event.toolCallId, {
    state: "approval-responded",
    approval: {
      approved: event.approved,
      ...(event.approvalId !== undefined
        ? { id: event.approvalId, approvalId: event.approvalId }
        : {}),
      ...(event.reason !== undefined ? { reason: event.reason } : {}),
    },
  });
}

function applyToolPart(
  projection: VercelProjection,
  messageId: string,
  toolCallId: string,
  patch: {
    state: AI.DynamicToolState;
    toolName?: string;
    input?: unknown;
    output?: unknown;
    errorText?: string;
    approval?: AI.ToolApproval;
  },
  force = false,
): void {
  const message = ensureMessage(projection, messageId);
  let tracker = projection.trackers.tools.get(toolCallId);
  if (!tracker) {
    const part: AI.UIMessagePart = {
      type: "dynamic-tool",
      toolName: patch.toolName ?? "tool",
      toolCallId,
      state: "input-streaming",
    };
    const partIndex = message.parts.push(part) - 1;
    tracker = { messageId, partIndex, inputText: "" };
    projection.trackers.tools.set(toolCallId, tracker);
  }
  const existing = message.parts[tracker.partIndex];
  message.parts[tracker.partIndex] = force
    ? forceToolPart(existing, toolCallId, patch)
    : transitionToolPart(existing, patch.state, patch);
}

function retryPendingToolResolutions(projection: VercelProjection): void {
  for (const [toolCallId, chunks] of Array.from(projection.pendingToolResolutions)) {
    const tracker = projection.trackers.tools.get(toolCallId);
    const message = tracker
      ? projection.messages.find((item) => item.id === tracker.messageId)
      : undefined;
    const part = tracker ? message?.parts[tracker.partIndex] : undefined;
    if (!tracker || part?.type !== "dynamic-tool" || part.state === "input-streaming") {
      continue;
    }
    projection.pendingToolResolutions.delete(toolCallId);
    const winner = chunks.reduce((current, candidate) =>
      compareSerial(String(candidate.meta.serial), String(current.meta.serial)) > 0
        ? candidate
        : current,
    );
    const targetMessageId = tracker.messageId;
    const winningSerial = projection.conflictSerials.get(`tool-output:${winner.event.toolCallId}`);
    if (
      winningSerial === undefined ||
      compareSerial(winningSerial, String(winner.meta.serial)) <= 0
    ) {
      applyToolResolutionPart(projection, targetMessageId, winner.event);
    }
  }
}

function ensureMessage(
  projection: VercelProjection,
  messageId: string,
  metadata?: unknown,
): AI.UIMessage {
  let message = projection.messages.find((item) => item.id === messageId);
  if (!message) {
    message = {
      id: messageId,
      role: "assistant",
      parts: [],
      ...(metadata !== undefined ? { metadata } : {}),
    };
    projection.messages.push(message);
  } else if (metadata !== undefined) {
    message.metadata = metadata;
  }
  return message;
}

function trackerMap(
  trackers: MessageTrackers,
  family: "text" | "reasoning",
  messageId: string,
): Map<string, number> {
  const root = family === "text" ? trackers.text : trackers.reasoning;
  let map = root.get(messageId);
  if (!map) {
    map = new Map();
    root.set(messageId, map);
  }
  return map;
}

function wins(projection: VercelProjection, key: string, meta: ReducerMeta): boolean {
  const serial = String(meta.serial);
  const previous = projection.conflictSerials.get(key);
  if (previous !== undefined && compareSerial(previous, serial) >= 0) {
    return false;
  }
  projection.conflictSerials.set(key, serial);
  return true;
}

function compareSerial(left: string, right: string): number {
  const integer = /^\d+$/u;
  if (integer.test(left) && integer.test(right)) {
    const leftValue = BigInt(left);
    const rightValue = BigInt(right);
    return leftValue === rightValue ? 0 : leftValue > rightValue ? 1 : -1;
  }
  return left.localeCompare(right);
}

function messageIdFor(chunk: VercelOutput, meta: ReducerMeta): string {
  if ("messageId" in chunk && chunk.messageId) {
    return chunk.messageId;
  }
  return meta.messageId ?? "message";
}

function bufferPending(
  projection: VercelProjection,
  toolCallId: string,
  chunk: Extract<
    VercelOutput,
    {
      type: "tool-output-available" | "tool-output-error" | "tool-output-denied";
    }
  >,
  meta: ReducerMeta,
): void {
  let pending = projection.pendingToolResolutions.get(toolCallId);
  if (!pending) {
    pending = [];
    projection.pendingToolResolutions.set(toolCallId, pending);
  }
  pending.push({ event: chunk, meta });
}

function forceToolPart(
  existing: AI.UIMessagePart | undefined,
  toolCallId: string,
  patch: {
    state: AI.DynamicToolState;
    toolName?: string;
    input?: unknown;
    output?: unknown;
    errorText?: string;
    approval?: AI.ToolApproval;
  },
): AI.UIMessagePart {
  const current =
    existing?.type === "dynamic-tool"
      ? existing
      : {
          type: "dynamic-tool" as const,
          toolName: patch.toolName ?? "tool",
          toolCallId,
          state: "input-streaming" as const,
        };
  return {
    ...current,
    ...(patch.toolName !== undefined ? { toolName: patch.toolName } : {}),
    state: patch.state,
    ...(patch.input !== undefined ? { input: patch.input } : {}),
    ...(patch.output !== undefined ? { output: patch.output } : {}),
    ...(patch.errorText !== undefined ? { errorText: patch.errorText } : {}),
    ...(patch.approval !== undefined ? { approval: patch.approval } : {}),
  };
}

function parseJsonish(value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function isUserMessage(input: VercelInput | VercelOutput): input is UserMessage<AI.UIMessage> {
  return userMessagePayload(input) !== undefined;
}

function userMessagePayload(input: VercelInput | VercelOutput): AI.UIMessage | undefined {
  const candidate = "message" in input ? input.message : input;
  return isUiMessage(candidate) ? candidate : undefined;
}

function isUiMessage(value: unknown): value is AI.UIMessage {
  return (
    value !== null && typeof value === "object" && "parts" in value && Array.isArray(value.parts)
  );
}

function isRegenerate(input: VercelInput | VercelOutput): input is Regenerate {
  return "target" in input && "parent" in input;
}

function isToolResultInput(
  input: VercelInput | VercelOutput,
): input is Extract<VercelInput, { type: "tool-result" }> {
  return "type" in input && input.type === "tool-result";
}

function isToolResultErrorInput(
  input: VercelInput | VercelOutput,
): input is Extract<VercelInput, { type: "tool-result-error" }> {
  return "type" in input && input.type === "tool-result-error";
}

function isToolApprovalResponseInput(
  input: VercelInput | VercelOutput,
): input is Extract<VercelInput, { type: "tool-approval-response" }> {
  return "type" in input && input.type === "tool-approval-response";
}
