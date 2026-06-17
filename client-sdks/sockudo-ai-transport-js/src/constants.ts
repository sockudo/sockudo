/** Client-to-agent AI input event name. */
export const EVENT_AI_INPUT = "ai-input";
/** Agent-to-client AI output event name. */
export const EVENT_AI_OUTPUT = "ai-output";
/** AI turn lifecycle start event name. */
export const EVENT_AI_TURN_START = "ai-turn-start";
/** AI turn lifecycle end event name. */
export const EVENT_AI_TURN_END = "ai-turn-end";
/** AI cancellation event name. */
export const EVENT_AI_CANCEL = "ai-cancel";

/** Transport header key for turn identity. */
export const HEADER_TURN_ID = "turn-id";
/** Transport header key for verified turn client identity. */
export const HEADER_TURN_CLIENT_ID = "turn-client-id";
/** Transport header key for turn end reason. */
export const HEADER_TURN_REASON = "turn-reason";
/** Transport header key for suspended-turn continuation. */
export const HEADER_TURN_CONTINUE = "turn-continue";
/** Transport header key for invocation identity. */
export const HEADER_INVOCATION_ID = "invocation-id";
/** Transport header key for input event identity. */
export const HEADER_EVENT_ID = "event-id";
/** Transport header key for codec message identity. */
export const HEADER_CODEC_MESSAGE_ID = "codec-message-id";
/** Transport header key indicating streaming content. */
export const HEADER_STREAM = "stream";
/** Transport header key for stream identity. */
export const HEADER_STREAM_ID = "stream-id";
/** Transport header key for stream status. */
export const HEADER_STATUS = "status";
/** Transport header key indicating discrete content. */
export const HEADER_DISCRETE = "discrete";
/** Transport header key for message role. */
export const HEADER_ROLE = "role";
/** Transport header key for parent codec message identity. */
export const HEADER_PARENT = "parent";
/** Transport header key for fork source codec message identity. */
export const HEADER_FORK_OF = "fork-of";
/** Transport header key indicating regeneration. */
export const HEADER_MSG_REGENERATE = "msg-regenerate";
/** Transport header key for stream error code. */
export const HEADER_ERROR_CODE = "error-code";
/** Transport header key for stream error message. */
export const HEADER_ERROR_MESSAGE = "error-message";
/** Transport header key for verified input client identity. */
export const HEADER_INPUT_CLIENT_ID = "input-client-id";
