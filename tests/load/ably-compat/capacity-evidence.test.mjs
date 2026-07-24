import assert from "node:assert/strict";
import test from "node:test";

import {
  BoundedLatencySamples,
  parseDeliveryMetadata,
  StreamingDeliveryAudit,
} from "./capacity-evidence.mjs";

test("streaming audit accounts exactly without retaining deliveries", () => {
  const audit = new StreamingDeliveryAudit(4);
  audit.record(delivery(1, 10));
  audit.record(delivery(2, 11));
  audit.record(delivery(2, 12));
  audit.record(delivery(5, 13));
  audit.record(delivery(4, 12));

  assert.deepEqual(audit.result(), {
    expected: 4,
    received: 5,
    loss: 1,
    duplicates: 1,
    reordered: 1,
    unexpected: 1,
    gaps: 1,
  });
  assert.equal(audit.lastChannelSerial, "stream:12");
});

test("latency evidence remains bounded and reports the full observation count", () => {
  const samples = new BoundedLatencySamples(64);
  for (let value = 0; value < 10_000; value += 1) samples.add(value);

  const summary = samples.summary();
  assert.equal(summary.count, 10_000);
  assert.equal(summary.sampleCount, 64);
  assert.equal(summary.sampleLimit, 64);
  assert.equal(samples.values.length, 64);
  assert.ok(summary.p99 <= summary.max);
});

test("plain delivery metadata extraction does not retain a large filler", () => {
  const text = JSON.stringify({
    action: 15,
    channelSerial: "stream:42",
    messages: [{ data: { sequence: 7, sentAt: 1000, filler: "x".repeat(65_536) } }],
  });

  assert.deepEqual(parseDeliveryMetadata(text, 1010), [{
    sequence: 7,
    sentAt: 1000,
    receivedAt: 1010,
    channelSerial: "stream:42",
    deliverySerial: 42,
  }]);
});

test("cipher-labelled base64 delivery metadata is decoded for auditing", () => {
  const data = Buffer.from(JSON.stringify({ sequence: 9, sentAt: 2000, filler: "x".repeat(4096) })).toString("base64");
  const text = JSON.stringify({
    action: 15,
    channelSerial: "stream:43",
    messages: [{ encoding: "cipher+aes-256-cbc", data }],
  });

  assert.deepEqual(parseDeliveryMetadata(text, 2025), [{
    sequence: 9,
    sentAt: 2000,
    receivedAt: 2025,
    channelSerial: "stream:43",
    deliverySerial: 43,
  }]);
});

test("control frames are left to the state-machine parser", () => {
  assert.equal(parseDeliveryMetadata('{"action":4,"connectionId":"one"}'), null);
});

function delivery(sequence, serial) {
  return { sequence, channelSerial: `stream:${serial}`, deliverySerial: serial };
}
