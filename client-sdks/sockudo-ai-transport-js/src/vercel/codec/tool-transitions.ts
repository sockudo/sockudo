import type { AI } from "./events.js";

/** Base dynamic tool part fields. */
export interface ToolBase {
  /** Tool name. */
  toolName: string;
  /** Tool call id. */
  toolCallId: string;
}

/** Creates a base dynamic tool part. */
export function toolBase(base: ToolBase): AI.UIMessagePart {
  return {
    type: "dynamic-tool",
    toolName: base.toolName,
    toolCallId: base.toolCallId,
    state: "input-streaming",
  };
}

/** Applies a legal Vercel dynamic-tool transition. */
export function transitionToolPart(
  part: AI.UIMessagePart | undefined,
  next: AI.DynamicToolState,
  patch: {
    toolName?: string;
    input?: unknown;
    output?: unknown;
    errorText?: string;
    approval?: AI.ToolApproval;
  } = {},
): AI.UIMessagePart {
  const current =
    part?.type === "dynamic-tool"
      ? part
      : {
          type: "dynamic-tool" as const,
          toolName: patch.toolName ?? "tool",
          toolCallId: "",
          state: "input-streaming" as const,
        };
  if (!isLegalTransition(current.state, next)) {
    return current;
  }
  return {
    ...current,
    ...(patch.toolName !== undefined ? { toolName: patch.toolName } : {}),
    state: next,
    ...(patch.input !== undefined ? { input: patch.input } : {}),
    ...(patch.output !== undefined ? { output: patch.output } : {}),
    ...(patch.errorText !== undefined ? { errorText: patch.errorText } : {}),
    ...(patch.approval !== undefined ? { approval: patch.approval } : {}),
  };
}

function isLegalTransition(from: AI.DynamicToolState, to: AI.DynamicToolState): boolean {
  if (from === to) {
    return true;
  }
  const allowed: Record<AI.DynamicToolState, readonly AI.DynamicToolState[]> = {
    "input-streaming": ["input-available", "output-error"],
    "input-available": [
      "approval-requested",
      "approval-responded",
      "output-available",
      "output-error",
      "output-denied",
    ],
    "approval-requested": ["approval-responded", "output-denied"],
    "approval-responded": ["output-available", "output-error", "output-denied"],
    "output-available": [],
    "output-error": [],
    "output-denied": [],
  };
  return allowed[from].includes(to);
}
