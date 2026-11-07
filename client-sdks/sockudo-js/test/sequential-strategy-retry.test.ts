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

  it("keeps the base timeout for an attempt that follows fast failures", async () => {
    vi.useFakeTimers();
    const starts: number[] = [];
    const aborts: number[] = [];
    const strategy = new SequentialStrategy(
      [
        {
          isSupported: () => true,
          connect: (_minPriority, callback) => {
            starts.push(Date.now());
            if (starts.length <= 3) {
              setTimeout(() => callback(true), 0);
            }
            return { abort: () => aborts.push(Date.now()), forceMinPriority: () => {} };
          },
        } as Strategy,
      ],
      { loop: true, timeout: 15000, timeoutLimit: 60000, retryDelay: 1000, retryDelayLimit: 5000 },
    );

    const runner = strategy.connect(0, () => {});
    await vi.advanceTimersByTimeAsync(30000);
    expect(starts.length).toBeGreaterThanOrEqual(4);
    expect(aborts[0] - starts[3]).toBe(15000);
    runner.abort();
  });

  it("doubles the timeout after an attempt times out", async () => {
    vi.useFakeTimers();
    const starts: number[] = [];
    const strategy = new SequentialStrategy(
      [
        {
          isSupported: () => true,
          connect: (_minPriority, _callback) => {
            starts.push(Date.now());
            return { abort: () => {}, forceMinPriority: () => {} };
          },
        } as Strategy,
      ],
      { loop: true, timeout: 15000, timeoutLimit: 60000, retryDelay: 1000, retryDelayLimit: 5000 },
    );

    const runner = strategy.connect(0, () => {});
    await vi.advanceTimersByTimeAsync(15000 + 30000 + 60000);
    const gaps = starts.slice(1).map((time, i) => time - starts[i]);
    expect(gaps).toEqual([15000, 30000, 60000]);
    runner.abort();
  });

  it("ignores a failure reported after abort", async () => {
    vi.useFakeTimers();
    const attempts: number[] = [];
    const strategy = new SequentialStrategy([failingStrategy(attempts)], {
      loop: true,
      timeout: 15000,
      retryDelay: 1000,
    });

    const runner = strategy.connect(0, () => {});
    runner.abort();
    await vi.advanceTimersByTimeAsync(60000);
    expect(attempts).toHaveLength(1);
  });
});
