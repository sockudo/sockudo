import type { ReducerMeta, Regenerate, UserMessage } from "../../core/codec/index.js";

/**
 * Structural Vercel AI SDK v6/v7 type namespace used by the optional Vercel
 * integration without requiring the `ai` peer at build time.
 */
// eslint-disable-next-line @typescript-eslint/no-namespace -- Preserve the documented AI.UIMessage public shape without requiring the optional ai peer.
export namespace AI {
  /** Dynamic tool part state carried by UI messages. */
  export type DynamicToolState =
    | "input-streaming"
    | "input-available"
    | "approval-requested"
    | "approval-responded"
    | "output-available"
    | "output-error"
    | "output-denied";

  /** UI message part shape used by Vercel AI SDK v6/v7. */
  export type UIMessagePart =
    | {
        type: "text";
        text: string;
        id?: string;
        state?: "streaming" | "done";
        providerMetadata?: unknown;
      }
    | {
        type: "reasoning";
        text: string;
        id?: string;
        state?: "streaming" | "done";
        providerMetadata?: unknown;
      }
    | { type: "custom"; kind: `${string}.${string}`; providerMetadata?: unknown }
    | {
        type: "dynamic-tool";
        toolName: string;
        toolCallId: string;
        state: DynamicToolState;
        title?: string;
        toolMetadata?: Record<string, unknown>;
        providerExecuted?: boolean;
        input?: unknown;
        output?: unknown;
        errorText?: string;
        approval?: ToolApproval;
        preliminary?: boolean;
        callProviderMetadata?: unknown;
        resultProviderMetadata?: unknown;
      }
    | {
        type: "file";
        url: string;
        mediaType?: string;
        filename?: string;
        providerReference?: unknown;
        providerMetadata?: unknown;
      }
    | { type: "reasoning-file"; url: string; mediaType: string; providerMetadata?: unknown }
    | {
        type: "source-url";
        sourceId?: string;
        url: string;
        title?: string;
        providerMetadata?: unknown;
      }
    | {
        type: "source-document";
        sourceId?: string;
        title?: string;
        mediaType?: string;
        filename?: string;
        providerMetadata?: unknown;
      }
    | { type: `data-${string}`; id?: string; data: unknown; transient?: boolean }
    | { type: "step-start" };

  /** Vercel UI message. */
  export interface UIMessage {
    /** Stable message id. */
    id: string;
    /** Message role. */
    role: "system" | "user" | "assistant" | "tool";
    /** Message parts. */
    parts: UIMessagePart[];
    /** Optional metadata. */
    metadata?: unknown;
  }

  /** Approval payload stored on dynamic tool parts. */
  export interface ToolApproval {
    /** Vercel UI approval id. */
    id?: string;
    /** Approval id. */
    approvalId?: string;
    /** Whether the tool call was approved. */
    approved: boolean;
    /** Optional denial reason. */
    reason?: string;
    /** Whether approval can be handled automatically by the client. */
    isAutomatic?: boolean;
    /** Provider signature for approval verification. */
    signature?: string;
  }

  /** Vercel UI message stream chunk. */
  export type UIMessageChunk =
    | { type: "start"; messageId?: string; messageMetadata?: unknown }
    | { type: "start-step" }
    | { type: "finish-step" }
    | { type: "finish"; finishReason?: string; messageMetadata?: unknown; metadata?: unknown }
    | { type: "error"; errorText: string }
    | { type: "abort"; reason?: string }
    | { type: "message-metadata"; messageMetadata: unknown }
    | { type: "text-start"; id: string; messageId?: string; providerMetadata?: unknown }
    | {
        type: "text-delta";
        id: string;
        delta: string;
        messageId?: string;
        providerMetadata?: unknown;
      }
    | { type: "text-end"; id: string; messageId?: string; providerMetadata?: unknown }
    | { type: "reasoning-start"; id: string; messageId?: string; providerMetadata?: unknown }
    | {
        type: "reasoning-delta";
        id: string;
        delta: string;
        messageId?: string;
        providerMetadata?: unknown;
      }
    | { type: "reasoning-end"; id: string; messageId?: string; providerMetadata?: unknown }
    | { type: "custom"; kind: `${string}.${string}`; providerMetadata?: unknown }
    | {
        type: "tool-input-start";
        id?: string;
        toolCallId: string;
        toolName: string;
        dynamic?: boolean;
        messageId?: string;
        providerExecuted?: boolean;
        providerMetadata?: unknown;
        toolMetadata?: Record<string, unknown>;
        title?: string;
      }
    | {
        type: "tool-input-delta";
        toolCallId: string;
        inputTextDelta?: string;
        /** Legacy Sockudo alias accepted for pre-v7 compatibility. */
        delta?: string;
        messageId?: string;
      }
    | {
        type: "tool-input-available";
        toolCallId: string;
        toolName?: string;
        input?: unknown;
        providerExecuted?: boolean;
        preliminary?: boolean;
        messageId?: string;
        providerMetadata?: unknown;
        toolMetadata?: Record<string, unknown>;
        dynamic?: boolean;
        title?: string;
      }
    | {
        type: "tool-input-error";
        toolCallId: string;
        toolName?: string;
        input?: unknown;
        errorText: string;
        messageId?: string;
        providerExecuted?: boolean;
        providerMetadata?: unknown;
        toolMetadata?: Record<string, unknown>;
        dynamic?: boolean;
        title?: string;
      }
    | {
        type: "tool-output-available";
        toolCallId: string;
        output: unknown;
        messageId?: string;
        providerExecuted?: boolean;
        providerMetadata?: unknown;
        toolMetadata?: Record<string, unknown>;
        dynamic?: boolean;
        preliminary?: boolean;
      }
    | {
        type: "tool-output-error";
        toolCallId: string;
        errorText: string;
        messageId?: string;
        providerExecuted?: boolean;
        providerMetadata?: unknown;
        toolMetadata?: Record<string, unknown>;
        dynamic?: boolean;
      }
    | {
        type: "tool-approval-request";
        toolCallId: string;
        approvalId?: string;
        isAutomatic?: boolean;
        signature?: string;
        messageId?: string;
      }
    | {
        type: "tool-approval-response";
        approvalId: string;
        toolCallId?: string;
        approved: boolean;
        reason?: string;
        providerExecuted?: boolean;
        providerMetadata?: unknown;
        messageId?: string;
      }
    | {
        type: "tool-output-denied";
        toolCallId: string;
        reason?: string;
        messageId?: string;
      }
    | {
        type: "file";
        url: string;
        mediaType?: string;
        filename?: string;
        providerMetadata?: unknown;
      }
    | { type: "reasoning-file"; url: string; mediaType: string; providerMetadata?: unknown }
    | {
        type: "source-url";
        sourceId?: string;
        url: string;
        title?: string;
        providerMetadata?: unknown;
      }
    | {
        type: "source-document";
        sourceId?: string;
        title?: string;
        mediaType?: string;
        filename?: string;
        providerMetadata?: unknown;
      }
    | { type: `data-${string}`; id?: string; data: unknown; transient?: boolean };
}

/** Tool result input from client-side tool execution. */
export interface ToolResult {
  /** Discriminator. */
  type: "tool-result";
  /** Tool call id. */
  toolCallId: string;
  /** Tool output. */
  output: unknown;
}

/** Tool result error input from client-side tool execution. */
export interface ToolResultError {
  /** Discriminator. */
  type: "tool-result-error";
  /** Tool call id. */
  toolCallId: string;
  /** Error message. */
  message: string;
}

/** Tool approval response input. */
export interface ToolApprovalResponse {
  /** Discriminator. */
  type: "tool-approval-response";
  /** Tool call id. */
  toolCallId: string;
  /** Approval id. */
  approvalId?: string;
  /** Whether the tool call is approved. */
  approved: boolean;
  /** Optional denial reason. */
  reason?: string;
}

/** Client-to-agent Vercel transport input. */
export type VercelInput =
  | UserMessage<AI.UIMessage>
  | Regenerate
  | ToolResult
  | ToolResultError
  | ToolApprovalResponse;

/** Agent-to-client Vercel transport output. */
export type VercelOutput = AI.UIMessageChunk;

/** Reducer projection for Vercel UI messages. */
export interface VercelProjection {
  /** Materialized Vercel UI messages. */
  messages: AI.UIMessage[];
  /** Conflict keys with their winning serial. */
  conflictSerials: Map<string, string>;
  /** Streaming part trackers. */
  trackers: MessageTrackers;
  /** Out-of-order tool resolutions. */
  pendingToolResolutions: Map<string, PendingToolResolution[]>;
}

/** Buffered tool resolution with its original reducer metadata. */
export interface PendingToolResolution {
  /** Buffered output event. */
  event: Extract<
    VercelOutput,
    {
      type: "tool-output-available" | "tool-output-error" | "tool-output-denied";
    }
  >;
  /** Original fold metadata. */
  meta: ReducerMeta;
}

/** Active stream trackers used by the reducer. */
export interface MessageTrackers {
  /** Message id to text part index by stream id. */
  text: Map<string, Map<string, number>>;
  /** Message id to reasoning part index by stream id. */
  reasoning: Map<string, Map<string, number>>;
  /** Tool call id to message/part/input tracker. */
  tools: Map<
    string,
    {
      messageId: string;
      partIndex: number;
      inputText: string;
    }
  >;
  /** Approval id to tool call id. */
  approvals: Map<string, string>;
}
