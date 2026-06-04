import {
  HEADER_CODEC_MESSAGE_ID,
  HEADER_EVENT_ID,
  HEADER_FORK_OF,
  HEADER_INPUT_CLIENT_ID,
  HEADER_INVOCATION_ID,
  HEADER_MSG_REGENERATE,
  HEADER_PARENT,
  HEADER_ROLE,
  HEADER_TURN_CLIENT_ID,
  HEADER_TURN_CONTINUE,
  HEADER_TURN_ID,
} from "./constants.js";

/**
 * Immutable string header map returned by SDK header readers.
 */
export type HeaderMap = Readonly<Record<string, string>>;

/**
 * AI message extras shape used by Sockudo AI Transport.
 */
export interface AiExtras {
  /** AI metadata grouped by transport and codec tiers. */
  ai?: {
    /** SDK-interpreted transport headers. */
    transport?: Record<string, unknown>;
    /** Opaque codec headers. */
    codec?: Record<string, unknown>;
  };
}

/**
 * Options for building canonical AI transport headers.
 */
export interface BuildTransportHeadersOptions {
  /** Message role to write into the `role` transport header. */
  role?: string;
  /** Turn identity to write into `turn-id`. */
  turnId?: string;
  /** Codec message identity to write into `codec-message-id`. */
  codecMessageId?: string;
  /** Verified turn client identity to write into `turn-client-id`. */
  turnClientId?: string;
  /** Parent codec message identity to write into `parent`. */
  parent?: string;
  /** Fork source codec message identity to write into `fork-of`. */
  forkOf?: string;
  /** Whether this message regenerates a previous message. */
  regenerates?: boolean;
  /** Invocation identity to write into `invocation-id`. */
  invocationId?: string;
  /** Verified input client identity to write into `input-client-id`. */
  inputClientId?: string;
  /** Input event identity to write into `event-id`. */
  inputEventId?: string;
  /** Whether the turn continues a suspended turn. */
  turnContinue?: boolean;
}

/**
 * Reads `extras.ai.transport` into a null-prototype string map.
 *
 * Missing or malformed tiers return an empty map. Prototype-pollution keys are
 * copied as data keys and never assigned to `Object.prototype`.
 */
export function getTransportHeaders(extras: unknown): HeaderMap {
  return readHeaderTier(extras, "transport");
}

/**
 * Reads `extras.ai.codec` into a null-prototype string map.
 *
 * Missing or malformed tiers return an empty map. Values that are not strings
 * are ignored because the wire contract is string-to-string.
 */
export function getCodecHeaders(extras: unknown): HeaderMap {
  return readHeaderTier(extras, "codec");
}

/**
 * Creates a mutable null-prototype header writer.
 *
 * Undefined values are skipped. Boolean values are serialized as `"true"` or
 * `"false"` to match the Sockudo AI header registry.
 */
export function headerWriter(): {
  readonly headers: Record<string, string>;
  set(key: string, value: string | number | boolean | undefined): void;
  setJson(key: string, value: unknown | undefined): void;
} {
  const headers: Record<string, string> = Object.create(null) as Record<
    string,
    string
  >;
  return {
    headers,
    set(key, value) {
      if (value !== undefined) {
        headers[key] = typeof value === "boolean" ? String(value) : `${value}`;
      }
    },
    setJson(key, value) {
      if (value !== undefined) {
        headers[key] = JSON.stringify(value);
      }
    },
  };
}

/**
 * Creates typed readers over a header map.
 */
export function headerReader(headers: HeaderMap): {
  string(key: string): string | undefined;
  boolean(key: string): boolean | undefined;
  json<T>(key: string): T | undefined;
} {
  return {
    string(key) {
      return headers[key];
    },
    boolean(key) {
      const value = headers[key];
      if (value === "true") {
        return true;
      }
      if (value === "false") {
        return false;
      }
      return undefined;
    },
    json<T>(key) {
      const value = headers[key];
      if (value === undefined) {
        return undefined;
      }
      try {
        return JSON.parse(value) as T;
      } catch {
        return undefined;
      }
    },
  };
}

/**
 * Merges header maps into a new null-prototype map.
 */
export function mergeHeaders(...sources: readonly HeaderMap[]): HeaderMap {
  const merged: Record<string, string> = Object.create(null) as Record<
    string,
    string
  >;
  for (const source of sources) {
    for (const [key, value] of Object.entries(source)) {
      merged[key] = value;
    }
  }
  return merged;
}

/**
 * Removes keys whose values are `undefined` while preserving all other values.
 */
export function stripUndefined<T extends Record<string, unknown>>(
  value: T,
): Partial<T> {
  const stripped: Partial<T> = {};
  for (const key of Object.keys(value) as Array<keyof T>) {
    if (value[key] !== undefined) {
      stripped[key] = value[key];
    }
  }
  return stripped;
}

/**
 * Builds canonical Sockudo AI transport headers.
 */
export function buildTransportHeaders(
  options: BuildTransportHeadersOptions,
): HeaderMap {
  const writer = headerWriter();
  writer.set(HEADER_ROLE, options.role);
  writer.set(HEADER_TURN_ID, options.turnId);
  writer.set(HEADER_CODEC_MESSAGE_ID, options.codecMessageId);
  writer.set(HEADER_TURN_CLIENT_ID, options.turnClientId);
  writer.set(HEADER_PARENT, options.parent);
  writer.set(HEADER_FORK_OF, options.forkOf);
  writer.set(HEADER_MSG_REGENERATE, options.regenerates);
  writer.set(HEADER_INVOCATION_ID, options.invocationId);
  writer.set(HEADER_INPUT_CLIENT_ID, options.inputClientId);
  writer.set(HEADER_EVENT_ID, options.inputEventId);
  writer.set(HEADER_TURN_CONTINUE, options.turnContinue);
  return writer.headers;
}

function readHeaderTier(extras: unknown, tier: "transport" | "codec"): HeaderMap {
  const result: Record<string, string> = Object.create(null) as Record<
    string,
    string
  >;
  const extrasRecord = objectRecord(extras);
  const ai = objectRecord(extrasRecord?.ai);
  const headers = objectRecord(ai?.[tier]);
  if (!headers) {
    return result;
  }
  for (const [key, value] of Object.entries(headers)) {
    if (typeof value === "string") {
      result[key] = value;
    }
  }
  return result;
}

function objectRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : undefined;
}
