import type { ChannelWriter, Codec, Codec2 } from "../../core/codec/index.js";
import { createVercelDecoder } from "./decoder.js";
import { createVercelEncoder } from "./encoder.js";
import type {
  AI,
  VercelInput,
  VercelOutput,
  VercelProjection,
} from "./events.js";
import { createVercelProjection, foldVercelEvent } from "./reducer.js";

/**
 * Vercel AI SDK v6 UI message codec.
 *
 * Chunk mapping:
 * `text-start|text-delta|text-end`, `reasoning-start|reasoning-delta|reasoning-end`,
 * and `tool-input-start|tool-input-delta|tool-input-available` use Sockudo
 * mutable stream create/append/terminal writes. All other UIMessageChunk
 * families are discrete `ai-output` events with codec headers carrying the
 * Vercel chunk type and stable ids. User UI messages may be published either as
 * raw `{ message }` inputs by `ClientTransport` or as `ai-input` user-part
 * discretes by the codec encoder; observers decode both forms. Tool results,
 * tool errors, approval responses, and regenerate requests are `ai-input`
 * discretes with their tool/branch ids in codec or transport headers.
 */
export const UIMessageCodec: Codec<
  VercelInput,
  VercelOutput,
  VercelProjection,
  AI.UIMessage
> = {
  init: createVercelProjection,
  fold: foldVercelEvent,
  getMessages: (projection) => projection.messages,
  createUserMessage(message) {
    return { message };
  },
  createRegenerate(target, parent) {
    return { target, parent };
  },
  resolveToolTarget(output, projection) {
    if (!("toolCallId" in output)) {
      return undefined;
    }
    for (const message of projection.messages) {
      if (
        message.parts.some(
          (part) =>
            part.type === "dynamic-tool" &&
            part.toolCallId === output.toolCallId &&
            (part.state === "approval-requested" ||
              part.state === "approval-responded"),
        )
      ) {
        return message.id;
      }
    }
    return undefined;
  },
  isTerminal(output) {
    return (
      output.type === "finish" ||
      output.type === "error" ||
      output.type === "abort"
    );
  },
  createEncoder(channel: ChannelWriter, options = {}) {
    return createVercelEncoder(channel, options);
  },
  createDecoder: createVercelDecoder,
};

/** Type-level docs-compatible codec conformance. */
type DocsCompatibleCodec = Codec2<VercelOutput, AI.UIMessage>;
const docsCompatibleCodec = UIMessageCodec as unknown as DocsCompatibleCodec;
void docsCompatibleCodec;

export { createVercelDecoder } from "./decoder.js";
export { createVercelEncoder } from "./encoder.js";
export { createVercelProjection, foldVercelEvent } from "./reducer.js";
export { toolBase, transitionToolPart } from "./tool-transitions.js";
export type {
  AI,
  MessageTrackers,
  ToolApprovalResponse,
  ToolResult,
  ToolResultError,
  VercelInput,
  VercelOutput,
  VercelProjection,
} from "./events.js";
