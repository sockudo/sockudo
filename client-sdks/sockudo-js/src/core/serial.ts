import type { WireSerial } from "./connection/protocol/message-types";

const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

function parseIntegerString(value: string): bigint | null {
  const trimmed = value.trim();
  if (!/^-?\d+$/.test(trimmed)) {
    return null;
  }
  return BigInt(trimmed);
}

export function normalizeWireSerial(value: unknown): WireSerial | undefined {
  if (typeof value === "number") {
    if (!Number.isFinite(value) || !Number.isInteger(value)) {
      return undefined;
    }
    return Number.isSafeInteger(value) ? value : value.toString();
  }
  if (typeof value === "bigint") {
    return value <= MAX_SAFE_BIGINT && value >= MIN_SAFE_BIGINT ? Number(value) : value.toString();
  }
  if (typeof value === "string") {
    const parsed = parseIntegerString(value);
    if (parsed === null) {
      return undefined;
    }
    return parsed <= MAX_SAFE_BIGINT && parsed >= MIN_SAFE_BIGINT ? Number(parsed) : value.trim();
  }
  if (
    value != null &&
    typeof value === "object" &&
    "toString" in value &&
    typeof (value as { toString: () => string }).toString === "function"
  ) {
    return normalizeWireSerial((value as { toString: () => string }).toString());
  }
  return undefined;
}

export function isWireSerial(value: unknown): value is WireSerial {
  return typeof value === "number" || typeof value === "string";
}
