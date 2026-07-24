const ACTION_MESSAGE = /"action"\s*:\s*15\b/;
const CHANNEL_SERIAL = /"channelSerial"\s*:\s*"([^"\\]*)"/;
const DATA_KEY = '"data"';
const SEQUENCE_KEY = '"sequence"';
const SENT_AT_KEY = '"sentAt"';

export const DEFAULT_LATENCY_SAMPLE_LIMIT = 100_000;

/** Exact, bounded per-subscriber delivery accounting. */
export class StreamingDeliveryAudit {
  constructor(expected = 0) {
    if (!Number.isSafeInteger(expected) || expected < 0) {
      throw new Error(`invalid expected delivery count ${expected}`);
    }
    this.expected = expected;
    this.seen = expected === 0 ? null : new Uint8Array(expected + 1);
    this.received = 0;
    this.unique = 0;
    this.duplicates = 0;
    this.reordered = 0;
    this.unexpected = 0;
    this.lastDeliverySerial = null;
    this.lastChannelSerial = undefined;
  }

  record({ sequence, channelSerial, deliverySerial }) {
    this.received += 1;
    this.lastChannelSerial = channelSerial;

    if (deliverySerial !== null) {
      if (this.lastDeliverySerial !== null && deliverySerial <= this.lastDeliverySerial) {
        this.reordered += 1;
      }
      this.lastDeliverySerial = deliverySerial;
    }

    if (this.seen === null) return;
    if (!Number.isSafeInteger(sequence) || sequence < 1 || sequence > this.expected) {
      this.unexpected += 1;
      return;
    }
    if (this.seen[sequence] === 1) {
      this.duplicates += 1;
      return;
    }
    this.seen[sequence] = 1;
    this.unique += 1;
  }

  result() {
    const loss = Math.max(0, this.expected - this.unique);
    return {
      expected: this.expected,
      received: this.received,
      loss,
      duplicates: this.duplicates,
      reordered: this.reordered,
      unexpected: this.unexpected,
      gaps: loss,
    };
  }
}

/** Deterministic reservoir sampling with a hard retained-value limit. */
export class BoundedLatencySamples {
  constructor(limit = DEFAULT_LATENCY_SAMPLE_LIMIT) {
    if (!Number.isSafeInteger(limit) || limit < 1) {
      throw new Error(`invalid latency sample limit ${limit}`);
    }
    this.limit = limit;
    this.count = 0;
    this.values = [];
    this.randomState = 0x9e3779b9;
  }

  add(value) {
    if (!Number.isFinite(value)) return;
    this.count += 1;
    if (this.values.length < this.limit) {
      this.values.push(value);
      return;
    }

    let state = this.randomState;
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    this.randomState = state >>> 0;
    const replacement = this.randomState % this.count;
    if (replacement < this.limit) this.values[replacement] = value;
  }

  summary() {
    if (this.values.length === 0) {
      return { count: this.count, sampleCount: 0, sampleLimit: this.limit };
    }
    const sorted = [...this.values].sort((left, right) => left - right);
    return {
      count: this.count,
      sampleCount: sorted.length,
      sampleLimit: this.limit,
      median: percentile(sorted, 0.5),
      p95: percentile(sorted, 0.95),
      p99: percentile(sorted, 0.99),
      max: Number(sorted.at(-1).toFixed(3)),
    };
  }
}

/**
 * Extract audit fields without materializing controlled load-payload filler.
 * Non-message frames return `null` for normal state-machine parsing.
 */
export function parseDeliveryMetadata(text, receivedAt = Date.now()) {
  if (!ACTION_MESSAGE.test(text)) return null;

  const channelSerial = CHANNEL_SERIAL.exec(text)?.[1];
  const deliverySerial = parseDeliverySerial(channelSerial);
  const dataKey = text.indexOf(DATA_KEY);
  if (dataKey < 0) return [];
  const dataStart = jsonValueStart(text, dataKey + DATA_KEY.length);
  if (dataStart < 0) return [];

  let payload = text;
  let payloadStart = dataStart;
  if (text[dataStart] === '"') {
    const dataEnd = text.indexOf('"', dataStart + 1);
    if (dataEnd < 0) return [];
    try {
      payload = Buffer.from(text.slice(dataStart + 1, dataEnd), "base64").toString("utf8");
      payloadStart = 0;
    } catch {
      return [];
    }
  }

  // Every load publish is one REST message and therefore one realtime
  // protocol message. If the runtime unexpectedly batches frames, accounting
  // fails closed as loss instead of retaining or scanning the large filler.
  const sequence = safeIntegerAfter(payload, SEQUENCE_KEY, payloadStart);
  const sentAt = safeIntegerAfter(payload, SENT_AT_KEY, payloadStart);
  if (sequence === null || sentAt === null) return [];
  return [{ sequence, sentAt, receivedAt, channelSerial, deliverySerial }];
}

function jsonValueStart(text, start) {
  const colon = text.indexOf(":", start);
  if (colon < 0) return -1;
  let cursor = colon + 1;
  while (cursor < text.length && /\s/.test(text[cursor])) cursor += 1;
  return cursor < text.length ? cursor : -1;
}

function safeIntegerAfter(text, key, start) {
  const keyStart = text.indexOf(key, start);
  if (keyStart < 0) return null;
  let cursor = jsonValueStart(text, keyStart + key.length);
  if (cursor < 0) return null;
  const numberStart = cursor;
  if (text[cursor] === "-") cursor += 1;
  const digitStart = cursor;
  while (cursor < text.length && text.charCodeAt(cursor) >= 48 && text.charCodeAt(cursor) <= 57) {
    cursor += 1;
  }
  if (cursor === digitStart) return null;
  const value = Number(text.slice(numberStart, cursor));
  return Number.isSafeInteger(value) ? value : null;
}

function parseDeliverySerial(channelSerial) {
  const value = Number(String(channelSerial ?? "").split(":").at(-1));
  return Number.isSafeInteger(value) ? value : null;
}

function percentile(sorted, fraction) {
  return Number(sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))].toFixed(3));
}
