import { ErrorCode, ErrorInfo } from "../../errors.js";
import type { Encoder, WriteOptions } from "../codec/index.js";

/** Result of streaming a response through an encoder. */
export interface StreamResult {
  /** Why the stream ended. */
  reason: "complete" | "cancelled" | "error";
  /** Original provider error when `reason` is `"error"`. */
  error?: Error;
}

/** Per-output write option resolver. */
export type ResolveWriteOptions<TOutput> = (output: TOutput) => WriteOptions | undefined;

/** Hooks used by {@link pipeStream}. */
export interface PipeStreamHooks<TOutput> {
  /** Optional per-output write options. */
  resolveWriteOptions?: ResolveWriteOptions<TOutput>;
  /** Publishes final events after abort before encoder cancellation. */
  onAbort?(write: (output: TOutput) => Promise<void>): void | Promise<void>;
  /** Receives wrapped stream errors. */
  onError?(error: ErrorInfo): void;
}

/**
 * Pipes a provider stream into a codec encoder.
 */
export async function pipeStream<TInput, TOutput>(
  stream: ReadableStream<TOutput>,
  encoder: Encoder<TInput, TOutput>,
  abortSignal: AbortSignal,
  hooks: PipeStreamHooks<TOutput> = {},
): Promise<StreamResult> {
  const reader = stream.getReader();
  const write = (output: TOutput): Promise<void> =>
    encoder.publishOutput(output, hooks.resolveWriteOptions?.(output)).then(() => undefined);
  try {
    for (;;) {
      if (abortSignal.aborted) {
        await reader.cancel();
        await hooks.onAbort?.(write);
        await encoder.cancel("cancelled");
        return { reason: "cancelled" };
      }
      const next = await reader.read();
      if (next.done) {
        await encoder.close();
        return { reason: "complete" };
      }
      await write(next.value);
    }
  } catch (error) {
    if (abortSignal.aborted) {
      await encoder.cancel("cancelled");
      return { reason: "cancelled" };
    }
    const wrapped = new ErrorInfo({
      code: ErrorCode.StreamError,
      message: "unable to stream response; stream failed",
      cause: error,
    });
    hooks.onError?.(wrapped);
    return {
      reason: "error",
      error: error instanceof Error ? error : new Error(String(error)),
    };
  } finally {
    reader.releaseLock();
  }
}
