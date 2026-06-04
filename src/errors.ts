/**
 * Stable SDK error codes used by `@sockudo/ai-transport`.
 *
 * Server-originated Sockudo platform codes may still pass through an
 * {@link ErrorInfo} when no SDK-specific code is more precise.
 */
export enum ErrorCode {
  /** Malformed requests or invalid wire data. */
  BadRequest = 40000,
  /** Invalid local API arguments. */
  InvalidArgument = 40003,
  /** Capability token expired and needs refresh. */
  TokenExpired = 40142,
  /** Authentication or capability check failed. */
  InsufficientCapability = 40160,
  /** Encoder recovery failed after a streaming append failure. */
  EncoderRecoveryFailed = 104000,
  /** Channel subscription failed before the transport could become ready. */
  TransportSubscriptionError = 104001,
  /** A cancel listener threw while handling a cancellation event. */
  CancelListenerError = 104002,
  /** Turn lifecycle operation was invalid for the current turn state. */
  TurnLifecycleError = 104003,
  /** Transport was used after close. */
  TransportClosed = 104004,
  /** Transport send failed or timed out before acknowledgement. */
  TransportSendFailed = 104005,
  /** Channel continuity was lost and history backfill is required. */
  ChannelContinuityLost = 104006,
  /** Channel is not attached or not ready for the requested operation. */
  ChannelNotReady = 104007,
  /** Stream failed while producing or consuming events. */
  StreamError = 104008,
  /** Turn start did not arrive before the configured deadline. */
  TurnStartDeadlineExceeded = 104009,
  /** Requested input event could not be found in history or live state. */
  InputEventNotFound = 104010,
}

/**
 * Constructor options for {@link ErrorInfo}.
 */
export interface ErrorInfoOptions {
  /** Numeric Sockudo AI Transport or platform error code. */
  code: ErrorCode | number;
  /** Human-readable error message. */
  message: string;
  /** HTTP-like status code; derived from `code` when omitted. */
  statusCode?: number;
  /** Original error or thrown value that caused this error. */
  cause?: unknown;
  /** Structured diagnostic detail with sensitive values already redacted. */
  detail?: unknown;
}

/**
 * Error shape rejected or thrown by all public SDK operations.
 *
 * @defaultValue `statusCode` is derived from the numeric `code`.
 */
export class ErrorInfo extends Error {
  /** Numeric Sockudo AI Transport or platform error code. */
  public readonly code: ErrorCode | number;

  /** HTTP-like status code derived from the code when not supplied. */
  public readonly statusCode: number;

  /** Original error or thrown value that caused this error. */
  public override readonly cause?: unknown;

  /** Structured diagnostic detail with sensitive values already redacted. */
  public readonly detail?: unknown;

  /**
   * Creates an SDK error.
   *
   * Synchronous public APIs throw this type for misuse; asynchronous public
   * APIs reject with this type for all failures.
   */
  public constructor(options: ErrorInfoOptions) {
    super(options.message, { cause: options.cause });
    this.name = "ErrorInfo";
    this.code = options.code;
    this.statusCode = options.statusCode ?? statusCodeForErrorCode(options.code);
    if (options.cause !== undefined) {
      this.cause = options.cause;
    }
    if (options.detail !== undefined) {
      this.detail = options.detail;
    }
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Returns whether an unknown value is an {@link ErrorInfo} with the supplied code.
 */
export function errorInfoIs(
  value: unknown,
  code: ErrorCode | number,
): value is ErrorInfo {
  return value instanceof ErrorInfo && value.code === code;
}

/**
 * Derives the HTTP-like status code for an SDK or server error code.
 *
 * @defaultValue AI Transport internal codes map to `500`, except deadline
 * codes `104009` and `104010`, which map to `504`.
 */
export function statusCodeForErrorCode(code: ErrorCode | number): number {
  if (
    code === ErrorCode.TurnStartDeadlineExceeded ||
    code === ErrorCode.InputEventNotFound
  ) {
    return 504;
  }
  if (code >= 10_000 && code <= 59_999) {
    return Math.floor(code / 100);
  }
  return 500;
}

/**
 * Converts an unknown thrown value into an {@link ErrorInfo}.
 *
 * Existing `ErrorInfo` values pass through unchanged. Objects with Sockudo
 * `{ code, message }` or `{ code, error }` fields preserve the server code.
 */
export function toErrorInfo(
  value: unknown,
  fallback: ErrorInfoOptions,
): ErrorInfo {
  if (value instanceof ErrorInfo) {
    return value;
  }

  const source = objectRecord(value);
  const data = objectRecord(source?.data) ?? source;
  const code = data?.code;
  const message = data?.message;
  const error = data?.error;

  if (
    typeof code === "number" &&
    (typeof message === "string" || typeof error === "string")
  ) {
    return new ErrorInfo({
      code,
      message: typeof message === "string" ? message : error,
      cause: value,
      detail: data,
    });
  }

  return new ErrorInfo({ ...fallback, cause: value });
}

function objectRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : undefined;
}
