import { afterEach, describe, expect, it, vi } from "vitest";
import { getConfig } from "../src/core/config";
import ConnectionManager from "../src/core/connection/connection_manager";
import EventsDispatcher from "../src/core/events/dispatcher";
import type Strategy from "../src/core/strategies/strategy";
import type Timeline from "../src/core/timeline/timeline";
import Runtime from "runtime";

describe("connection manager reconnection", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("uses parity defaults and preserves null as unlimited", () => {
    const defaults = getConfig({ cluster: "local" }, {});
    const unlimited = getConfig({ cluster: "local", maxReconnectAttempts: null }, {});

    expect(defaults.maxReconnectAttempts).toBe(6);
    expect(defaults.maxReconnectGapInSeconds).toBe(120);
    expect(defaults.reconnectJitter).toBe(0);
    expect(unlimited.maxReconnectAttempts).toBeNull();
  });

  it("emits reconnecting and stops at the configured attempt limit", async () => {
    vi.useFakeTimers();
    const { callbacks, manager } = createManager({ maxReconnectAttempts: 1 });
    const states: string[] = [];
    manager.bind("state_change", ({ current }) => states.push(current));

    manager.connect();
    callbacks[0](null, { action: "backoff" });
    await vi.advanceTimersByTimeAsync(0);

    expect(states).toEqual(["connecting", "reconnecting"]);

    callbacks[1](null, { action: "backoff" });

    expect(manager.state).toBe("disconnected");
    expect(states.at(-1)).toBe("disconnected");
  });

  it("uses quadratic capped delays and resets attempts after success", async () => {
    vi.useFakeTimers();
    const { callbacks, manager, timeline } = createManager({
      maxReconnectAttempts: null,
      maxReconnectGapInSeconds: 5,
    });
    const internals = manager as unknown as { reconnectAttempts: number };
    internals.reconnectAttempts = 3;

    manager.errorCallbacks.backoff({ action: "backoff" });

    expect(timeline.info).toHaveBeenLastCalledWith({ action: "retry", delay: 5000 });

    await vi.advanceTimersByTimeAsync(5000);
    const connection = fakeConnection();
    callbacks[0](null, {
      action: "connected",
      activityTimeout: 120_000,
      connection,
    });

    expect(manager.state).toBe("connected");
    expect(internals.reconnectAttempts).toBe(0);

    timeline.info.mockClear();
    manager.errorCallbacks.retry({ action: "retry" });
    manager.connectionCallbacks.closed();

    expect(timeline.info).toHaveBeenCalledTimes(1);
    expect(timeline.info).toHaveBeenLastCalledWith({ action: "retry", delay: 0 });

    manager.disconnect();
    internals.reconnectAttempts = 4;
    manager.connect();

    expect(internals.reconnectAttempts).toBe(0);
  });

  it("prepares a fresh token before initial and reconnect attempts", async () => {
    vi.useFakeTimers();
    const reasons: string[] = [];
    const { callbacks, manager } = createManager({
      beforeConnect: async (reason) => {
        reasons.push(reason);
      },
    });

    manager.connect();
    await vi.runAllTicks();
    callbacks[0](null, { action: "backoff" });
    await vi.advanceTimersByTimeAsync(0);

    expect(reasons).toEqual(["initial", "reconnect"]);
  });
  it("keeps delays exact when jitter is not configured", () => {
    const { manager, timeline } = createManager({ maxReconnectAttempts: null });
    const internals = manager as unknown as { reconnectAttempts: number };
    internals.reconnectAttempts = 3;

    manager.errorCallbacks.backoff({ action: "backoff" });

    expect(timeline.info).toHaveBeenLastCalledWith({ action: "retry", delay: 9000 });
  });

  it("randomizes the delay downwards within the configured fraction", () => {
    const randomInt = vi.spyOn(Runtime, "randomInt");
    const { manager, timeline } = createManager({
      maxReconnectAttempts: null,
      reconnectJitter: 0.5,
    });
    const internals = manager as unknown as { reconnectAttempts: number };

    const delays: number[] = [];
    for (const value of [0, 2500, 4500]) {
      randomInt.mockReturnValueOnce(value);
      internals.reconnectAttempts = 3;
      manager.errorCallbacks.backoff({ action: "backoff" });
      delays.push(lastRetryDelay(timeline));
    }

    // Half of 9000 is randomized away, so delays land in [4500, 9000].
    expect(randomInt).toHaveBeenCalledWith(4501);
    expect(delays).toEqual([9000, 6500, 4500]);
    randomInt.mockRestore();
  });

  it("never jitters past the cap or below zero at full jitter", () => {
    const randomInt = vi.spyOn(Runtime, "randomInt");
    const { manager, timeline } = createManager({
      maxReconnectAttempts: null,
      maxReconnectGapInSeconds: 5,
      reconnectJitter: 1,
    });
    const internals = manager as unknown as { reconnectAttempts: number };

    for (const value of [0, 5000]) {
      randomInt.mockReturnValueOnce(value);
      internals.reconnectAttempts = 10;
      manager.errorCallbacks.backoff({ action: "backoff" });
      const delay = lastRetryDelay(timeline);
      expect(delay).toBeGreaterThanOrEqual(0);
      expect(delay).toBeLessThanOrEqual(5000);
    }

    randomInt.mockRestore();
  });

  it("clamps out-of-range jitter values", () => {
    expect(getConfig({ cluster: "local", reconnectJitter: 5 }, {}).reconnectJitter).toBe(1);
    expect(getConfig({ cluster: "local", reconnectJitter: -1 }, {}).reconnectJitter).toBe(0);
    expect(getConfig({ cluster: "local", reconnectJitter: NaN }, {}).reconnectJitter).toBe(0);
  });

  it("leaves immediate retry and TLS-upgrade paths un-jittered", () => {
    const randomInt = vi.spyOn(Runtime, "randomInt");
    const { manager, timeline } = createManager({
      maxReconnectAttempts: null,
      reconnectJitter: 1,
    });

    manager.errorCallbacks.retry({ action: "retry" });
    expect(timeline.info).toHaveBeenLastCalledWith({ action: "retry", delay: 0 });

    manager.errorCallbacks.tls_only({ action: "tls_only" });
    expect(timeline.info).toHaveBeenLastCalledWith({ action: "retry", delay: 0 });

    expect(randomInt).not.toHaveBeenCalled();
    randomInt.mockRestore();
  });
});

function lastRetryDelay(timeline: { info: ReturnType<typeof vi.fn> }): number {
  const lastCall = timeline.info.mock.lastCall;
  expect(lastCall).toBeDefined();
  return (lastCall as [{ delay: number }])[0].delay;
}

function createManager(
  overrides: Partial<{
    maxReconnectAttempts: number | null;
    maxReconnectGapInSeconds: number;
    reconnectJitter: number;
    beforeConnect: (reason: "initial" | "reconnect") => Promise<void>;
  }> = {},
) {
  const callbacks: Array<(error: unknown, handshake: any) => void> = [];
  const strategy: Strategy = {
    isSupported: () => true,
    connect: (_priority, callback) => {
      callbacks.push(callback);
      return { abort: vi.fn(), forceMinPriority: vi.fn() };
    },
  };
  const timeline = {
    info: vi.fn(),
    error: vi.fn(),
  } as unknown as Timeline & { info: ReturnType<typeof vi.fn>; error: ReturnType<typeof vi.fn> };
  const manager = new ConnectionManager("app-key", {
    timeline,
    getStrategy: () => strategy,
    unavailableTimeout: 10_000,
    pongTimeout: 30_000,
    activityTimeout: 120_000,
    useTLS: false,
    maxReconnectAttempts:
      overrides.maxReconnectAttempts === undefined ? 6 : overrides.maxReconnectAttempts,
    maxReconnectGapInSeconds: overrides.maxReconnectGapInSeconds ?? 120,
    reconnectJitter: overrides.reconnectJitter ?? 0,
    beforeConnect: overrides.beforeConnect,
  });

  return { callbacks, manager, timeline };
}

function fakeConnection() {
  const connection = new EventsDispatcher() as EventsDispatcher & {
    id: string;
    activityTimeout: number;
    handlesActivityChecks: () => boolean;
    close: () => void;
  };
  connection.id = "1.1";
  connection.activityTimeout = 120_000;
  connection.handlesActivityChecks = () => true;
  connection.close = vi.fn();
  return connection;
}
