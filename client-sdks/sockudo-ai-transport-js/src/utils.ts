import {
  HEADER_CODEC_MESSAGE_ID,
  HEADER_EVENT_ID,
  HEADER_FORK_OF,
  HEADER_LEGACY_TURN_CLIENT_ID,
  HEADER_LEGACY_TURN_ID,
  HEADER_LEGACY_TURN_REASON,
  HEADER_INPUT_CLIENT_ID,
  HEADER_INVOCATION_ID,
  HEADER_MSG_REGENERATE,
  HEADER_PARENT,
  HEADER_RUN_CLIENT_ID,
  HEADER_RUN_ID,
  HEADER_RUN_REASON,
  HEADER_ROLE,
  HEADER_TURN_CONTINUE,
} from "./constants.js";

/** Immutable string header map. */
export type HeaderMap = Readonly<Record<string, string>>;

/** Extras shape carrying Sockudo AI metadata tiers. */
export interface AiExtras {
  /** AI metadata tiers. */
  ai?: {
    /** SDK-interpreted transport headers. */
    transport?: Record<string, unknown>;
    /** Opaque codec headers. */
    codec?: Record<string, unknown>;
  };
}

/** Inputs for canonical transport header construction. */
export interface BuildTransportHeadersOptions {
  /** Message role. */
  role?: string;
  /** Run identity. */
  runId?: string;
  /** Legacy alias for run identity. */
  turnId?: string;
  /** Codec message identity. */
  codecMessageId?: string;
  /** Verified run client identity. */
  runClientId?: string;
  /** Legacy alias for verified run client identity. */
  turnClientId?: string;
  /** Parent codec message identity. */
  parent?: string;
  /** Fork source codec message identity. */
  forkOf?: string;
  /** Codec message id of the assistant message this message regenerates. */
  regenerates?: string | boolean;
  /** Invocation identity. */
  invocationId?: string;
  /** Verified input client identity. */
  inputClientId?: string;
  /** Input event identity. */
  inputEventId?: string;
  /** Whether this legacy turn continues a suspended turn. Native runs use `ai-run-resume`. */
  turnContinue?: boolean;
}

/** Reads AI transport headers into a null-prototype string map. */
export function getTransportHeaders(extras: unknown): HeaderMap {
  return readHeaderTier(extras, "transport");
}

/** Reads AI codec headers into a null-prototype string map. */
export function getCodecHeaders(extras: unknown): HeaderMap {
  return readHeaderTier(extras, "codec");
}

/** Creates a mutable null-prototype writer for string headers. */
export function headerWriter(): {
  readonly headers: Record<string, string>;
  str(key: string, value: string | undefined): void;
  bool(key: string, value: boolean | undefined): void;
  json(key: string, value: unknown): void;
  set(key: string, value: string | number | boolean | undefined): void;
  setJson(key: string, value?: unknown): void;
} {
  const headers = Object.create(null) as Record<string, string>;
  return {
    headers,
    str(key, value) {
      if (value !== undefined) {
        headers[key] = value;
      }
    },
    bool(key, value) {
      if (value !== undefined) {
        headers[key] = value ? "true" : "false";
      }
    },
    json(key, value) {
      if (value !== undefined) {
        headers[key] = JSON.stringify(value);
      }
    },
    set(key, value) {
      if (value !== undefined) {
        headers[key] = typeof value === "boolean" ? String(value) : String(value);
      }
    },
    setJson(key, value) {
      this.json(key, value);
    },
  };
}

/** Creates typed readers over string headers. */
export function headerReader(headers: HeaderMap): {
  str(key: string): string | undefined;
  bool(key: string): boolean | undefined;
  string(key: string): string | undefined;
  boolean(key: string): boolean | undefined;
  json(key: string): unknown;
} {
  return {
    str(key) {
      return headers[key];
    },
    bool(key) {
      const value = headers[key];
      if (value === "true") {
        return true;
      }
      if (value === "false") {
        return false;
      }
      return undefined;
    },
    string(key) {
      return this.str(key);
    },
    boolean(key) {
      return this.bool(key);
    },
    json(key: string) {
      const value = headers[key];
      if (value === undefined) {
        return undefined;
      }
      try {
        return JSON.parse(value) as unknown;
      } catch {
        return undefined;
      }
    },
  };
}

/** Merges header maps into a new null-prototype map. */
export function mergeHeaders(...sources: readonly HeaderMap[]): HeaderMap {
  const merged = Object.create(null) as Record<string, string>;
  for (const source of sources) {
    for (const [key, value] of Object.entries(source)) {
      merged[key] = value;
    }
  }
  return merged;
}

/** Returns a shallow copy with `undefined` properties omitted. */
export function stripUndefined<T extends Record<string, unknown>>(value: T): Partial<T> {
  const stripped: Partial<T> = {};
  for (const key of Object.keys(value) as (keyof T)[]) {
    if (value[key] !== undefined) {
      stripped[key] = value[key];
    }
  }
  return stripped;
}

/** Builds canonical Sockudo AI transport headers. */
export function buildTransportHeaders(options: BuildTransportHeadersOptions): HeaderMap {
  const writer = headerWriter();
  writer.set(HEADER_ROLE, options.role);
  writer.set(HEADER_RUN_ID, options.runId ?? options.turnId);
  writer.set(HEADER_CODEC_MESSAGE_ID, options.codecMessageId);
  writer.set(HEADER_RUN_CLIENT_ID, options.runClientId ?? options.turnClientId);
  writer.set(HEADER_PARENT, options.parent);
  writer.set(HEADER_FORK_OF, options.forkOf);
  writer.set(HEADER_MSG_REGENERATE, regenerateHeaderValue(options));
  writer.set(HEADER_INVOCATION_ID, options.invocationId);
  writer.set(HEADER_INPUT_CLIENT_ID, options.inputClientId);
  writer.set(HEADER_EVENT_ID, options.inputEventId);
  writer.set(HEADER_TURN_CONTINUE, options.turnContinue);
  return writer.headers;
}

function regenerateHeaderValue(options: BuildTransportHeadersOptions): string | undefined {
  if (typeof options.regenerates === "string") {
    return options.regenerates;
  }
  if (options.regenerates === true) {
    return options.forkOf ?? options.codecMessageId ?? "true";
  }
  return undefined;
}

function readHeaderTier(extras: unknown, tier: "transport" | "codec"): HeaderMap {
  const result = readSockudoHeaderFallbacks(extras, tier);
  const ai = asRecord(asRecord(extras)?.ai);
  const source = asRecord(ai?.[tier]);
  if (source) {
    for (const [key, value] of Object.entries(source)) {
      if (typeof value === "string") {
        result[key] = value;
      }
    }
  }
  if (tier === "transport") {
    addRunHeaderAliases(result);
  }
  return result;
}

function addRunHeaderAliases(headers: Record<string, string>): void {
  if (headers[HEADER_RUN_ID] === undefined && headers[HEADER_LEGACY_TURN_ID] !== undefined) {
    headers[HEADER_RUN_ID] = headers[HEADER_LEGACY_TURN_ID];
  }
  if (
    headers[HEADER_RUN_CLIENT_ID] === undefined &&
    headers[HEADER_LEGACY_TURN_CLIENT_ID] !== undefined
  ) {
    headers[HEADER_RUN_CLIENT_ID] = headers[HEADER_LEGACY_TURN_CLIENT_ID];
  }
  if (
    headers[HEADER_RUN_REASON] === undefined &&
    headers[HEADER_LEGACY_TURN_REASON] !== undefined
  ) {
    headers[HEADER_RUN_REASON] = headers[HEADER_LEGACY_TURN_REASON];
  }
}

const transportHeaderFallbacks: Readonly<Record<string, string>> = {
  "x-sockudo-run-id": HEADER_RUN_ID,
  "x-sockudo-turn-id": HEADER_RUN_ID,
  "x-sockudo-run-client-id": HEADER_RUN_CLIENT_ID,
  "x-sockudo-turn-client-id": HEADER_RUN_CLIENT_ID,
  "x-sockudo-client-id": HEADER_RUN_CLIENT_ID,
  "x-sockudo-input-client-id": HEADER_INPUT_CLIENT_ID,
  "x-sockudo-run-reason": HEADER_RUN_REASON,
  "x-sockudo-turn-reason": HEADER_RUN_REASON,
  "x-sockudo-turn-continue": HEADER_TURN_CONTINUE,
  "x-sockudo-invocation-id": HEADER_INVOCATION_ID,
  "x-sockudo-event-id": HEADER_EVENT_ID,
  "x-sockudo-input-event-id": HEADER_EVENT_ID,
  "x-sockudo-codec-message-id": HEADER_CODEC_MESSAGE_ID,
  "x-sockudo-message-id": HEADER_CODEC_MESSAGE_ID,
  "x-sockudo-stream": "stream",
  "x-sockudo-stream-id": "stream-id",
  "x-sockudo-status": "status",
  "x-sockudo-discrete": "discrete",
  "x-sockudo-role": HEADER_ROLE,
  "x-sockudo-parent": HEADER_PARENT,
  "x-sockudo-fork-of": HEADER_FORK_OF,
  "x-sockudo-msg-regenerate": HEADER_MSG_REGENERATE,
  "x-sockudo-regenerate": HEADER_MSG_REGENERATE,
  "x-sockudo-error-code": "error-code",
  "x-sockudo-error-message": "error-message",
};

const codecHeaderFallbacks: Readonly<Record<string, string>> = {
  "x-sockudo-codec-type": "type",
  "x-sockudo-codec-id": "id",
  "x-sockudo-codec-message-id": "message-id",
  "x-sockudo-codec-tool-call-id": "tool-call-id",
  "x-sockudo-tool-call-id": "tool-call-id",
  "x-sockudo-codec-tool-name": "tool-name",
  "x-sockudo-tool-name": "tool-name",
  "x-sockudo-codec-dynamic": "dynamic",
  "x-sockudo-codec-title": "title",
  "x-sockudo-codec-provider-executed": "provider-executed",
  "x-sockudo-codec-preliminary": "preliminary",
  "x-sockudo-codec-is-automatic": "is-automatic",
  "x-sockudo-codec-approval-id": "approval-id",
  "x-sockudo-approval-id": "approval-id",
  "x-sockudo-codec-approved": "approved",
  "x-sockudo-approved": "approved",
  "x-sockudo-codec-reason": "reason",
  "x-sockudo-codec-signature": "signature",
  "x-sockudo-codec-kind": "kind",
  "x-sockudo-codec-finish-reason": "finish-reason",
  "x-sockudo-finish-reason": "finish-reason",
  "x-sockudo-codec-source-id": "source-id",
  "x-sockudo-codec-media-type": "media-type",
  "x-sockudo-codec-filename": "filename",
  "x-sockudo-codec-transient": "transient",
  "x-sockudo-codec-tool-metadata": "tool-metadata",
  "x-sockudo-codec-provider-metadata": "provider-metadata",
  "x-sockudo-codec-message-metadata": "message-metadata",
};

function readSockudoHeaderFallbacks(
  extras: unknown,
  tier: "transport" | "codec",
): Record<string, string> {
  const result = Object.create(null) as Record<string, string>;
  const headers = asRecord(asRecord(extras)?.headers);
  if (!headers) {
    return result;
  }
  const mappings = tier === "transport" ? transportHeaderFallbacks : codecHeaderFallbacks;
  for (const [rawKey, rawValue] of Object.entries(headers)) {
    const mapped = mappings[rawKey.toLowerCase()];
    const value = stringifyHeader(rawValue);
    if (mapped !== undefined && value !== undefined) {
      result[mapped] = value;
    }
  }
  return result;
}

function stringifyHeader(value: unknown): string | undefined {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value === "boolean") {
    return String(value);
  }
  return undefined;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object"
    ? (value as Record<string, unknown>)
    : undefined;
}
