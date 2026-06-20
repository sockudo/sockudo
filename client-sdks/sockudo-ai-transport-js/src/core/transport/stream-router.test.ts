import { describe, expect, it } from "vitest";

import { ErrorInfo } from "../../errors.js";
import { createStreamRouter } from "./stream-router.js";

describe("stream router", () => {
  it("routes by active invocation and closes on terminal output", async () => {
    const router = createStreamRouter<Event>({
      isTerminal: (event) => event.done === true,
    });
    const stream = router.createStream("turn-1", "inv-1");
    const reader = stream.getReader();

    expect(router.route("turn-1", "stale", { text: "old" })).toBe(false);
    expect(router.route("turn-1", "inv-1", { text: "a" })).toBe(true);
    expect(await reader.read()).toEqual({
      done: false,
      value: { text: "a" },
    });
    expect(router.route("turn-1", "inv-1", { text: "end", done: true })).toBe(true);
    expect(await reader.read()).toEqual({
      done: false,
      value: { text: "end", done: true },
    });
    expect(await reader.read()).toEqual({ done: true, value: undefined });
    expect(router.has("turn-1")).toBe(false);
  });

  it("errors instead of growing beyond the bounded queue", async () => {
    const router = createStreamRouter<Event>({
      isTerminal: () => false,
      maxQueuedChunks: 1,
    });
    const stream = router.createStream("turn-1", "inv-1");
    const reader = stream.getReader();

    expect(router.route("turn-1", "inv-1", { text: "a" })).toBe(true);
    expect(router.route("turn-1", "inv-1", { text: "b" })).toBe(false);
    await expect(reader.read()).rejects.toBeInstanceOf(ErrorInfo);
  });
});

interface Event {
  text: string;
  done?: boolean;
}
