/**
 * Logging levels in ascending verbosity.
 */
export enum LogLevel {
  /** Disable all logging. */
  Silent = 0,
  /** Log errors only. */
  Error = 1,
  /** Log warnings and errors. */
  Warn = 2,
  /** Log info, warnings, and errors. */
  Info = 3,
  /** Log debug and higher severity messages. */
  Debug = 4,
  /** Log every message. */
  Trace = 5,
}

/**
 * Context object attached to SDK log messages.
 */
export type LogContext = Readonly<Record<string, unknown>>;

/**
 * Sink used by {@link makeLogger}.
 */
export type LogHandler = (line: string) => void;

/**
 * Logger interface used throughout the SDK.
 */
export interface Logger {
  /** Emits a trace message when enabled. */
  trace(message: string, context?: LogContext): void;
  /** Emits a debug message when enabled. */
  debug(message: string, context?: LogContext): void;
  /** Emits an info message when enabled. */
  info(message: string, context?: LogContext): void;
  /** Emits a warning message when enabled. */
  warn(message: string, context?: LogContext): void;
  /** Emits an error message when enabled. */
  error(message: string, context?: LogContext): void;
  /** Returns a logger with context merged into every subsequent call. */
  withContext(context: LogContext): Logger;
}

/**
 * Options for {@link makeLogger}.
 */
export interface MakeLoggerOptions {
  /** Log sink.
   *
   * @defaultValue A no-op handler.
   */
  logHandler?: LogHandler;
  /** Maximum verbosity to emit.
   *
   * @defaultValue {@link LogLevel.Silent}
   */
  logLevel?: LogLevel;
  /** Context merged into every log call.
   *
   * @defaultValue `{}`
   */
  context?: LogContext;
  /** Clock provider for deterministic tests.
   *
   * @defaultValue `new Date().toISOString()`
   */
  now?: () => string;
}

/**
 * Console-backed log sink.
 */
export const consoleLogger: LogHandler = (line: string): void => {
  console.log(line);
};

const sensitiveKeyPattern = /(?:token|authorization|secret)/iu;

/**
 * Creates a redacting SDK logger.
 */
export function makeLogger(options: MakeLoggerOptions = {}): Logger {
  const logLevel = options.logLevel ?? LogLevel.Silent;
  const logHandler = options.logHandler ?? (() => undefined);
  const baseContext = options.context ?? {};
  const now = options.now ?? (() => new Date().toISOString());

  const emit = (
    level: Exclude<keyof typeof LogLevel, "Silent">,
    levelValue: LogLevel,
    message: string,
    context?: LogContext,
  ): void => {
    if (logLevel < levelValue) {
      return;
    }
    const mergedContext = Object.assign({}, baseContext, context);
    const redacted = redactValue(mergedContext);
    const contextSuffix =
      Object.keys(mergedContext).length > 0 ? `, context: ${JSON.stringify(redacted)}` : "";
    logHandler(
      `[${now()}] ${level.toUpperCase()} sockudo-ai-transport: ${message}${contextSuffix}`,
    );
  };

  const logger: Logger = {
    trace(message, context) {
      emit("Trace", LogLevel.Trace, message, context);
    },
    debug(message, context) {
      emit("Debug", LogLevel.Debug, message, context);
    },
    info(message, context) {
      emit("Info", LogLevel.Info, message, context);
    },
    warn(message, context) {
      emit("Warn", LogLevel.Warn, message, context);
    },
    error(message, context) {
      emit("Error", LogLevel.Error, message, context);
    },
    withContext(context) {
      return makeLogger({
        logHandler,
        logLevel,
        now,
        context: { ...baseContext, ...context },
      });
    },
  };

  return logger;
}

/**
 * Returns a JSON-safe copy with sensitive keys masked.
 */
export function redactValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => redactValue(item));
  }
  if (value !== null && typeof value === "object") {
    const source = value as Record<string, unknown>;
    const redacted = Object.create(null) as Record<string, unknown>;
    for (const [key, item] of Object.entries(source)) {
      redacted[key] = sensitiveKeyPattern.test(key) ? "[redacted]" : redactValue(item);
    }
    return redacted;
  }
  return value;
}
