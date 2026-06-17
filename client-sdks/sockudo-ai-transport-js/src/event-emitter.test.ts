import { describe, expect, it } from "vitest";

import { EventEmitter, type EventsMap } from "./event-emitter.js";
import { LogLevel, makeLogger } from "./logger.js";

interface TestEvents extends EventsMap {
  ready: { count: number };
  empty: undefined;
}

describe("EventEmitter", () => {
  it("dispatches synchronously and unsubscribes", () => {
    const emitter = new EventEmitter<TestEvents>();
    const calls: number[] = [];
    const unsubscribe = emitter.on("ready", (payload) => {
      calls.push(payload.count);
    });

    emitter.emit("ready", { count: 1 });
    unsubscribe();
    emitter.emit("ready", { count: 2 });
    emitter.emit("empty", undefined);

    expect(calls).toEqual([1]);
  });

  it("logs listener exceptions without propagating them", () => {
    const lines: string[] = [];
    const logger = makeLogger({
      logLevel: LogLevel.Error,
      logHandler: (line) => lines.push(line),
      now: () => "2026-06-04T00:00:00.000Z",
    });
    const emitter = new EventEmitter<TestEvents>({ logger });
    const calls: number[] = [];
    emitter.on("ready", () => {
      throw new Error("listener failed");
    });
    emitter.on("ready", (payload) => {
      calls.push(payload.count);
    });

    expect(() => {
      emitter.emit("ready", { count: 7 });
    }).not.toThrow();

    expect(calls).toEqual([7]);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain(
      "ERROR sockudo-ai-transport: event listener failed",
    );
    expect(lines[0]).toContain('"event":"ready"');
  });
});
