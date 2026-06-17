import { describe, expect, it } from "vitest";

import { createLifecycleTracker } from "./lifecycle-tracker.js";

describe("lifecycle tracker", () => {
  it("synthesizes missing phases before the triggering event", () => {
    const tracker = createLifecycleTracker<{ turn: string }, string>([
      {
        name: "start",
        buildEvent: (scopeId, context) => `${scopeId}:${context.turn}:start`,
      },
      {
        name: "start-step",
        buildEvent: (scopeId, context) =>
          `${scopeId}:${context.turn}:start-step`,
      },
    ]);

    const preamble = tracker.ensurePhases("scope-1", { turn: "turn-1" });
    const delivered = [...preamble, "real-event"];

    expect(delivered).toEqual([
      "scope-1:turn-1:start",
      "scope-1:turn-1:start-step",
      "real-event",
    ]);
    expect(tracker.ensurePhases("scope-1", { turn: "turn-1" })).toEqual([]);
  });

  it("supports mark, reset, and clear per scope", () => {
    const tracker = createLifecycleTracker<undefined, string>([
      {
        name: "start",
        buildEvent: (scopeId) => `${scopeId}:start`,
      },
      {
        name: "step",
        buildEvent: (scopeId) => `${scopeId}:step`,
      },
    ]);

    tracker.markEmitted("scope-1", "start");
    expect(tracker.ensurePhases("scope-1", undefined)).toEqual([
      "scope-1:step",
    ]);

    tracker.resetPhase("scope-1", "start");
    expect(tracker.ensurePhases("scope-1", undefined)).toEqual([
      "scope-1:start",
      "scope-1:step",
    ]);

    tracker.clearScope("scope-1");
    expect(tracker.ensurePhases("scope-1", undefined)).toEqual([
      "scope-1:start",
      "scope-1:step",
    ]);
  });
});
