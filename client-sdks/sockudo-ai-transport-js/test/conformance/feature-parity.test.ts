import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { FEATURE_RECEIPTS } from "./features.js";

describe("feature parity receipt", () => {
  it("lists every release-gate feature exactly once", () => {
    const names = FEATURE_RECEIPTS.map((receipt) => receipt.feature);
    expect(new Set(names).size).toBe(names.length);
    expect(names).toMatchInlineSnapshot(`
      [
        "Golden transcript replay",
        "Golden transcript produce",
        "Token streaming and rollup content equality",
        "Cancellation filters and cancelled turns",
        "Reconnection and history fallback",
        "Multi-device observer rendering and active turns",
        "History/replay paging and branch materialization",
        "Branching edit/regenerate/sibling navigation",
        "Interruption cancel-then-send/send-alongside",
        "Concurrent turns and waitForTurn routing",
        "Tool calling server/client continuation",
        "Human-in-the-loop suspended continuation",
        "Optimistic update POST failure and race reconciliation",
        "Agent presence status flow",
        "Chain-of-thought reasoning multiplexing",
        "Double texting patterns",
        "Chaos-lite websocket drop stages",
        "Clock skew serial determinism",
        "Slow-consumer stream backpressure",
        "Concepts: sessions, turns, transport, codec, tree",
        "Core API surface",
        "React API surface",
        "Vercel API surface",
        "Vercel React API surface",
      ]
    `);
  });

  it("keeps FEATURE_PARITY.md synchronized with the suite manifest", async () => {
    const receipt = await readFile(resolve("FEATURE_PARITY.md"), "utf8");
    for (const item of FEATURE_RECEIPTS) {
      expect(receipt).toMatch(
        new RegExp(
          `\\|\\s*${escapeRegex(item.feature)}\\s*\\|\\s*${escapeRegex(item.specFile)}\\s*\\|\\s*${item.status}\\s*\\|`,
          "u",
        ),
      );
    }
  });
});

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}
