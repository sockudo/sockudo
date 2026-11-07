import { afterEach, describe, expect, it, vi } from "vitest";
import SequentialStrategy from "../src/core/strategies/sequential_strategy";
import type Strategy from "../src/core/strategies/strategy";

function failingStrategy(attemptTimes: number[]): Strategy {
  return {
    isSupported: () => true,
    connect: (_minPriority, callback) => {
      attemptTimes.push(Date.now());
      // Fail on the next tick, like a refused TCP connection.
      setTimeout(() => callback(true), 0);
      return { abort: () => {}, forceMinPriority: () => {} };
    },
  } as Strategy;
}

describe("sequential strategy retry", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("waits out the timeout after a fast failure without retryDelay", async () => {
    vi.useFakeTimers();
    const attempts: number[] = [];
    const strategy = new SequentialStrategy([failingStrategy(attempts)], {
      loop: true,
      timeout: 15000,
      timeoutLimit: 60000,
    });

    const runner = strategy.connect(0, () => {});
    await vi.advanceTimersByTimeAsync(14000);
    expect(attempts).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(attempts).toHaveLength(2);
    runner.abort();
  });

  it("retries a fast failure after a short jittered backoff", async () => {
    vi.useFakeTimers();
    const attempts: number[] = [];
    const strategy = new SequentialStrategy([failingStrategy(attempts)], {
      loop: true,
      timeout: 15000,
      timeoutLimit: 60000,
      retryDelay: 1000,
      retryDelayLimit: 5000,
    });

    const runner = strategy.connect(0, () => {});
    // Attempt delays are within [delay/2, delay] for 1 s, 2 s, 4 s, then 5 s.
    await vi.advanceTimersByTimeAsync(1000 + 2000 + 4000 + 5000 + 5000);
    expect(attempts.length).toBeGreaterThanOrEqual(6);
    const gaps = attempts.slice(1).map((time, i) => time - attempts[i]);
    expect(gaps[0]).toBeGreaterThanOrEqual(500);
    expect(gaps[0]).toBeLessThanOrEqual(1000);
    for (const gap of gaps) {
      expect(gap).toBeLessThanOrEqual(5000);
    }
    runner.abort();
  });

  it("cancels a pending retry on abort", async () => {
    vi.useFakeTimers();
    const attempts: number[] = [];
    const strategy = new SequentialStrategy([failingStrategy(attempts)], {
      loop: true,
      timeout: 15000,
      retryDelay: 1000,
    });

    const runner = strategy.connect(0, () => {});
    await vi.advanceTimersByTimeAsync(1);
    runner.abort();
    await vi.advanceTimersByTimeAsync(20000);
    expect(attempts).toHaveLength(1);
  });
});
