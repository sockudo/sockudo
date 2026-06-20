/**
 * Stable SDK error codes used by `@sockudo/ai-transport`.
 */
export enum ErrorCode {
  /** Malformed request or invalid wire data. */
  BadRequest = 40000,
  /** Invalid local API argument. */
  InvalidArgument = 40003,
  /** Capability token expired. */
  TokenExpired = 40142,
  /** Authentication or capability check failed. */
  InsufficientCapability = 40160,
  /** Encoder recovery failed after a stream append failure. */
  EncoderRecoveryFailed = 104000,
  /** Channel subscription failed. */
  TransportSubscriptionError = 104001,
  /** Cancellation listener failed. */
  CancelListenerError = 104002,
  /** Invalid turn lifecycle operation. */
  TurnLifecycleError = 104003,
  /** Transport was used after close. */
  TransportClosed = 104004,
  /** Send or acknowledgement failed. */
  TransportSendFailed = 104005,
  /** Channel continuity was lost and history backfill is required. */
  ChannelContinuityLost = 104006,
  /** Channel is not ready. */
  ChannelNotReady = 104007,
  /** Stream failed. */
  StreamError = 104008,
  /** Turn start did not arrive before the configured deadline. */
  TurnStartDeadlineExceeded = 104009,
  /** Input event was not found. */
  InputEventNotFound = 104010,
}

/**
 * Constructor options for {@link ErrorInfo}.
 */
export interface ErrorInfoOptions {
  /** Numeric SDK or Sockudo platform code. */
  code: ErrorCode | number;
  /** Human-readable error message. */
  message: string;
  /** HTTP-like status code; derived from `code` by default. */
  statusCode?: number;
  /** Original error or thrown value. */
  cause?: unknown;
  /** Structured diagnostic detail. */
  detail?: unknown;
}

/**
 * Formats SDK error messages in the Ably-compatible form.
 */
export function formatErrorMessage(operation: string, reason: string): string {
  return `unable to ${operation}; ${reason}`;
}

/**
 * Error shape thrown or rejected by public SDK APIs.
 *
 * @defaultValue `statusCode` is derived from the numeric code.
 */
export class ErrorInfo extends Error {
  /** Numeric SDK or Sockudo platform code. */
  public readonly code: ErrorCode | number;

  /** HTTP-like status code. */
  public readonly statusCode: number;

  /** Original error or thrown value. */
  public override readonly cause?: unknown;

  /** Structured diagnostic detail. */
  public readonly detail?: unknown;

  /** Creates a new SDK error. */
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
 * Returns whether a value is an {@link ErrorInfo} with `code`.
 */
export function errorInfoIs(value: unknown, code: ErrorCode | number): value is ErrorInfo {
  return value instanceof ErrorInfo && value.code === code;
}

/**
 * Derives an HTTP-like status code from an SDK or platform error code.
 */
export function statusCodeForErrorCode(code: ErrorCode | number): number {
  if (code === 104_009 || code === 104_010) {
    return 504;
  }
  if (code >= 10_000 && code <= 59_999) {
    return Math.floor(code / 100);
  }
  return 500;
}

/**
 * Converts unknown thrown values into {@link ErrorInfo}.
 */
export function toErrorInfo(value: unknown, fallback: ErrorInfoOptions): ErrorInfo {
  if (value instanceof ErrorInfo) {
    return value;
  }
  const root = asRecord(value);
  const data = asRecord(root?.data) ?? root;
  const code = data?.code;
  const message = data?.message;
  const error = data?.error;
  const status = readStatusCode(data);
  const mappedCode =
    status === 401 || status === 403 ? ErrorCode.InsufficientCapability : undefined;
  if (typeof code === "number" && (typeof message === "string" || typeof error === "string")) {
    const reason = typeof message === "string" ? message : (error as string);
    return new ErrorInfo({
      code: mappedCode ?? code,
      message: reason,
      cause: value,
      detail: data,
      ...(status !== undefined ? { statusCode: status } : {}),
    });
  }
  if (typeof code === "string" && (typeof message === "string" || typeof error === "string")) {
    const numeric = Number(code);
    const reason = typeof message === "string" ? message : (error as string);
    return new ErrorInfo({
      code: mappedCode ?? (Number.isFinite(numeric) ? numeric : fallback.code),
      message: reason,
      cause: value,
      detail: data,
      ...(status !== undefined ? { statusCode: status } : {}),
    });
  }
  if (mappedCode !== undefined && status !== undefined) {
    return new ErrorInfo({
      code: mappedCode,
      message: fallback.message,
      cause: value,
      detail: data,
      statusCode: status,
    });
  }
  return new ErrorInfo({ ...fallback, cause: value });
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : undefined;
}

function readStatusCode(value: Record<string, unknown> | undefined): number | undefined {
  const raw = value?.statusCode ?? value?.status;
  return typeof raw === "number" && Number.isFinite(raw) ? raw : undefined;
}
