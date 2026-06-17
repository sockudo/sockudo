import type {
  StreamResult,
  TurnEndReason,
} from "../../core/transport/index.js";

/**
 * Finish reason values returned by Vercel AI SDK stream helpers.
 */
export type VercelFinishReason = string | undefined;

/**
 * Maps a Sockudo pipe result and Vercel finish reason to an AI Transport turn
 * end reason.
 *
 * Non-complete pipe results win immediately and the finish-reason promise is
 * observed in the background to avoid unhandled rejections. Complete streams
 * suspend on Vercel `tool-calls`, complete on all other finish reasons, map
 * abort-shaped rejections to `cancelled`, and map all other rejections to
 * `error`.
 */
export async function vercelTurnEndReason(
  pipeResult: StreamResult,
  finishReason: Promise<VercelFinishReason>,
): Promise<TurnEndReason> {
  if (pipeResult.reason !== "complete") {
    finishReason.catch(() => undefined);
    return pipeResult.reason;
  }

  try {
    return (await finishReason) === "tool-calls" ? "suspended" : "complete";
  } catch (error) {
    return isAbortError(error) ? "cancelled" : "error";
  }
}

function isAbortError(error: unknown): boolean {
  if (error instanceof DOMException && error.name === "AbortError") {
    return true;
  }
  if (error !== null && typeof error === "object") {
    const record = error as Record<string, unknown>;
    return (
      record.name === "AbortError" ||
      record.code === "ABORT_ERR" ||
      record.type === "aborted"
    );
  }
  return false;
}
