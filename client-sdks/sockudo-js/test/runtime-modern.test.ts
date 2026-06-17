import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const currentDir = dirname(fileURLToPath(import.meta.url));

describe("node runtime modern surface", () => {
  it("uses native WebSocket and fetch-first APIs", () => {
    const runtimeSource = readFileSync(
      join(currentDir, "..", "src", "runtimes", "node", "runtime.ts"),
      "utf8",
    );

    expect(runtimeSource).toContain("globalThis.WebSocket");
    expect(runtimeSource).toContain("isXHRSupported(): boolean");
    expect(runtimeSource).toContain("return false;");
    expect(runtimeSource).toContain("createXHR()");
    expect(runtimeSource).toContain("fetchAuth");
    expect(runtimeSource).toContain("fetchTimeline");
    expect(runtimeSource.includes("faye-websocket")).toBe(false);
  });
});

describe("web runtime modern surface", () => {
  it("uses fetch auth and websocket-first strategy without legacy fallbacks", () => {
    const webRuntimeSource = readFileSync(
      join(currentDir, "..", "src", "runtimes", "web", "runtime.ts"),
      "utf8",
    );
    const webStrategySource = readFileSync(
      join(currentDir, "..", "src", "runtimes", "web", "default_strategy.ts"),
      "utf8",
    );

    expect(webRuntimeSource).toContain("fetchAuth");
    expect(webRuntimeSource).toContain("fetchTimeline");
    expect(webRuntimeSource.includes("jsonpAuth")).toBe(false);
    expect(webRuntimeSource.includes("xhrAuth")).toBe(false);

    expect(webStrategySource.includes("sockjs")).toBe(false);
    expect(webStrategySource.includes("xhr_streaming")).toBe(false);
    expect(webStrategySource.includes("xhr_polling")).toBe(false);
    expect(webStrategySource.includes("xdr_streaming")).toBe(false);
    expect(webStrategySource.includes("xdr_polling")).toBe(false);
  });
});
