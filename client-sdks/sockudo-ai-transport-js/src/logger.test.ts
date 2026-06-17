import { describe, expect, it, vi } from "vitest";

import {
  LogLevel,
  consoleLogger,
  makeLogger,
  redactValue,
  type LogContext,
} from "./logger.js";

describe("logger", () => {
  it("is silent by default and filters by level", () => {
    const lines: string[] = [];
    makeLogger({ logHandler: (line) => lines.push(line) }).error("hidden");
    makeLogger({ logLevel: LogLevel.Error }).error("handled by noop");
    const logger = makeLogger({
      logLevel: LogLevel.Warn,
      logHandler: (line) => lines.push(line),
      now: () => "2026-06-04T00:00:00.000Z",
    });

    logger.info("ignored");
    logger.warn("visible");

    expect(lines).toEqual([
      "[2026-06-04T00:00:00.000Z] WARN sockudo-ai-transport: visible",
    ]);
  });

  it("uses the current ISO timestamp by default", () => {
    const lines: string[] = [];
    makeLogger({
      logLevel: LogLevel.Error,
      logHandler: (line) => lines.push(line),
    }).error("visible");

    expect(lines[0]).toMatch(
      /^\[\d{4}-\d{2}-\d{2}T.*Z\] ERROR sockudo-ai-transport: visible$/u,
    );
  });

  it("formats all enabled levels with merged redacted context", () => {
    const lines: string[] = [];
    const logger = makeLogger({
      logLevel: LogLevel.Trace,
      logHandler: (line) => lines.push(line),
      now: () => "2026-06-04T00:00:00.000Z",
      context: { session: "s1", token: "secret" },
    }).withContext({ authorization: "Bearer abc" });

    logger.trace("trace", { nested: { apiSecret: "x" } });
    logger.debug("debug");
    logger.info("info");
    logger.error("error", { arr: [{ refreshToken: "t" }] });

    expect(lines[0]).toContain("TRACE sockudo-ai-transport: trace");
    expect(lines[0]).toContain('"token":"[redacted]"');
    expect(lines[0]).toContain('"authorization":"[redacted]"');
    expect(lines[0]).toContain('"apiSecret":"[redacted]"');
    expect(lines[1]).toBe(
      '[2026-06-04T00:00:00.000Z] DEBUG sockudo-ai-transport: debug, context: {"session":"s1","token":"[redacted]","authorization":"[redacted]"}',
    );
    expect(lines[2]).toContain("INFO sockudo-ai-transport: info");
    expect(lines[3]).toContain('"refreshToken":"[redacted]"');
  });

  it("redacts arrays, null-prototype objects, and primitives", () => {
    const input: LogContext = {
      safe: "ok",
      secretKey: "hide",
      nested: [{ authorizationHeader: "hide" }, null, 1],
    };

    expect(redactValue(input)).toEqual({
      safe: "ok",
      secretKey: "[redacted]",
      nested: [{ authorizationHeader: "[redacted]" }, null, 1],
    });
    expect(redactValue("plain")).toBe("plain");
  });

  it("exposes a console logger sink", () => {
    const spy = vi.spyOn(console, "log").mockImplementation(() => undefined);
    consoleLogger("line");
    expect(spy).toHaveBeenCalledWith("line");
    spy.mockRestore();
  });
});
