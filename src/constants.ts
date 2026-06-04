/**
 * Client-to-agent AI input event name.
 */
export const EVENT_AI_INPUT = "ai-input";

/**
 * Agent-to-client AI output event name.
 */
export const EVENT_AI_OUTPUT = "ai-output";

/**
 * AI turn lifecycle start event name.
 */
export const EVENT_AI_TURN_START = "ai-turn-start";

/**
 * AI turn lifecycle end event name.
 */
export const EVENT_AI_TURN_END = "ai-turn-end";

/**
 * AI cancellation signal event name.
 */
export const EVENT_AI_CANCEL = "ai-cancel";

/**
 * Transport header key carrying the turn identity.
 */
export const HEADER_TURN_ID = "turn-id";

/**
 * Transport header key carrying the verified turn client identity.
 */
export const HEADER_TURN_CLIENT_ID = "turn-client-id";

/**
 * Transport header key carrying a turn end reason.
 */
export const HEADER_TURN_REASON = "turn-reason";

/**
 * Transport header key indicating a suspended turn continuation.
 */
export const HEADER_TURN_CONTINUE = "turn-continue";

/**
 * Transport header key carrying the invocation identity.
 */
export const HEADER_INVOCATION_ID = "invocation-id";

/**
 * Transport header key carrying the input event identity.
 */
export const HEADER_EVENT_ID = "event-id";

/**
 * Transport header key carrying the codec message identity.
 */
export const HEADER_CODEC_MESSAGE_ID = "codec-message-id";

/**
 * Transport header key indicating whether content is streamed.
 */
export const HEADER_STREAM = "stream";

/**
 * Transport header key carrying the stream identity.
 */
export const HEADER_STREAM_ID = "stream-id";

/**
 * Transport header key carrying stream status.
 */
export const HEADER_STATUS = "status";

/**
 * Transport header key indicating discrete content.
 */
export const HEADER_DISCRETE = "discrete";

/**
 * Transport header key carrying the message role.
 */
export const HEADER_ROLE = "role";

/**
 * Transport header key carrying a parent codec message identity.
 */
export const HEADER_PARENT = "parent";

/**
 * Transport header key carrying a fork source codec message identity.
 */
export const HEADER_FORK_OF = "fork-of";

/**
 * Transport header key indicating message regeneration.
 */
export const HEADER_MSG_REGENERATE = "msg-regenerate";

/**
 * Transport header key carrying an AI stream error code.
 */
export const HEADER_ERROR_CODE = "error-code";

/**
 * Transport header key carrying an AI stream error message.
 */
export const HEADER_ERROR_MESSAGE = "error-message";

/**
 * Transport header key carrying the verified input client identity.
 */
export const HEADER_INPUT_CLIENT_ID = "input-client-id";
