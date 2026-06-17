import type {
  ReducerMeta,
  Regenerate,
  UserMessage,
} from "../../core/codec/index.js";

/**
 * Structural Vercel AI SDK v6 type namespace used by the optional Vercel
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

  /** UI message part shape used by Vercel AI SDK v6. */
  export type UIMessagePart =
    | { type: "text"; text: string; id?: string }
    | { type: "reasoning"; text: string; id?: string }
    | {
        type: "dynamic-tool";
        toolName: string;
        toolCallId: string;
        state: DynamicToolState;
        input?: unknown;
        output?: unknown;
        errorText?: string;
        approval?: ToolApproval;
      }
    | { type: "file"; url: string; mediaType?: string; filename?: string }
    | { type: "source-url"; sourceId?: string; url: string; title?: string }
    | {
        type: "source-document";
        sourceId?: string;
        title?: string;
        mediaType?: string;
        filename?: string;
      }
    | { type: `data-${string}`; data: unknown; transient?: boolean };

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
  }

  /** Vercel UI message stream chunk. */
  export type UIMessageChunk =
    | { type: "start"; messageId?: string; messageMetadata?: unknown }
    | { type: "start-step" }
    | { type: "finish-step" }
    | { type: "finish"; finishReason?: string; metadata?: unknown }
    | { type: "error"; errorText: string }
    | { type: "abort" }
    | { type: "message-metadata"; messageMetadata?: unknown }
    | { type: "text-start"; id: string; messageId?: string }
    | { type: "text-delta"; id: string; delta: string; messageId?: string }
    | { type: "text-end"; id: string; messageId?: string }
    | { type: "reasoning-start"; id: string; messageId?: string }
    | {
        type: "reasoning-delta";
        id: string;
        delta: string;
        messageId?: string;
      }
    | { type: "reasoning-end"; id: string; messageId?: string }
    | {
        type: "tool-input-start";
        id?: string;
        toolCallId: string;
        toolName: string;
        dynamic?: boolean;
        messageId?: string;
      }
    | {
        type: "tool-input-delta";
        toolCallId: string;
        delta: string;
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
      }
    | {
        type: "tool-input-error";
        toolCallId: string;
        toolName?: string;
        errorText: string;
        messageId?: string;
      }
    | {
        type: "tool-output-available";
        toolCallId: string;
        output: unknown;
        messageId?: string;
      }
    | {
        type: "tool-output-error";
        toolCallId: string;
        errorText: string;
        messageId?: string;
      }
    | {
        type: "tool-approval-request";
        toolCallId: string;
        approvalId?: string;
        messageId?: string;
      }
    | {
        type: "tool-output-denied";
        toolCallId: string;
        reason?: string;
        messageId?: string;
      }
    | { type: "file"; url: string; mediaType?: string; filename?: string }
    | { type: "source-url"; sourceId?: string; url: string; title?: string }
    | {
        type: "source-document";
        sourceId?: string;
        title?: string;
        mediaType?: string;
        filename?: string;
      }
    | { type: `data-${string}`; data: unknown; transient?: boolean };
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
      type:
        | "tool-output-available"
        | "tool-output-error"
        | "tool-output-denied";
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
}
