import { describe, expect, it } from "vitest";
import { createDelta as fossilCreate } from "fossil-delta";
import DeltaCompressionManager from "../src/core/delta/manager";

function encode(base: string, next: string): string {
  const encoder = new TextEncoder();
  const delta = fossilCreate(encoder.encode(base), encoder.encode(next));
  let binary = "";
  for (const byte of delta) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function manager(sent: Array<[string, unknown]> = []): DeltaCompressionManager {
  return new DeltaCompressionManager({ enabled: true }, (event, data) => {
    sent.push([event, data]);
    return true;
  });
}

describe("delta bases per (event, conflation key)", () => {
  it("decodes interleaved events whose server sequences overlap", () => {
    const delta = manager();
    const a0 = '{"event":"a","channel":"c","data":"{\\"v\\":\\"a0\\"}"}';
    const b0 = '{"event":"b","channel":"c","data":"{\\"v\\":\\"b0\\"}"}';
    // The server keeps a separate cache per event: both start at sequence 0.
    delta.handleFullMessage("c", a0, 0, undefined, "a");
    delta.handleFullMessage("c", b0, 0, undefined, "b");

    const a1 = '{"event":"a","channel":"c","data":"{\\"v\\":\\"a1\\"}"}';
    const fromA = delta.handleDeltaMessage("c", {
      event: "a",
      delta: encode(a0, a1),
      seq: 1,
      algorithm: "fossil",
      base_index: 0,
    });
    expect(fromA?.data).toEqual('{"v":"a1"}');

    const b1 = '{"event":"b","channel":"c","data":"{\\"v\\":\\"b1\\"}"}';
    const fromB = delta.handleDeltaMessage("c", {
      event: "b",
      delta: encode(b0, b1),
      seq: 1,
      algorithm: "fossil",
      base_index: 0,
    });
    expect(fromB?.data).toEqual('{"v":"b1"}');
    expect(delta.getStats().errors).toBe(0);
  });

  it("keeps conflation keys of the same event apart", () => {
    const delta = manager();
    const btc = '{"event":"tick","data":"{\\"s\\":\\"BTC\\",\\"p\\":1}"}';
    const eth = '{"event":"tick","data":"{\\"s\\":\\"ETH\\",\\"p\\":1}"}';
    delta.handleFullMessage("c", btc, 0, "BTC", "tick");
    delta.handleFullMessage("c", eth, 0, "ETH", "tick");
    const eth1 = '{"event":"tick","data":"{\\"s\\":\\"ETH\\",\\"p\\":2}"}';
    const result = delta.handleDeltaMessage("c", {
      event: "tick",
      delta: encode(eth, eth1),
      seq: 1,
      algorithm: "fossil",
      conflation_key: "ETH",
      base_index: 0,
    });
    expect(result?.data).toEqual('{"s":"ETH","p":2}');
  });

  it("resyncs instead of guessing when the requested base is not cached", () => {
    const sent: Array<[string, unknown]> = [];
    const delta = manager(sent);
    const a0 = '{"event":"a","data":"x"}';
    delta.handleFullMessage("c", a0, 5, undefined, "a");
    const result = delta.handleDeltaMessage("c", {
      event: "a",
      delta: encode(a0, '{"event":"a","data":"y"}'),
      seq: 7,
      algorithm: "fossil",
      base_index: 6,
    });
    expect(result).toBeNull();
    expect(sent.some(([event]) => event.endsWith("delta_sync_error"))).toBe(true);
  });
});

describe("delta negotiation", () => {
  it("honours a server refusal", () => {
    const delta = manager();
    delta.handleEnabled({ enabled: true, algorithm: "fossil" });
    expect(delta.isEnabled()).toBe(true);
    delta.handleEnabled({
      enabled: false,
      reason: "delta compression requires the JSON wire format",
    });
    expect(delta.isEnabled()).toBe(false);
  });
});
