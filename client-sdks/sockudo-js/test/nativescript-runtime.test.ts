import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const currentDir = dirname(fileURLToPath(import.meta.url));

describe("nativescript runtime surface", () => {
  it("uses global websocket/xhr APIs and documents the websocket polyfill requirement", () => {
    const runtimeSource = readFileSync(
      join(currentDir, "..", "src", "runtimes", "nativescript", "runtime.ts"),
      "utf8",
    );
    const entrySource = readFileSync(
      join(currentDir, "..", "nativescript", "index.js"),
      "utf8",
    );

    expect(runtimeSource).toContain("globalThis.WebSocket");
    expect(runtimeSource).toContain("globalThis as any).XMLHttpRequest");
    expect(runtimeSource).toContain("@valor/nativescript-websockets");
    expect(entrySource).toContain('import "@valor/nativescript-websockets"');
  });
});
