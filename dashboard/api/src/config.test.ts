import { describe, expect, test } from "bun:test";
import { resolveSessionSecret } from "./auth/configuration.ts";

describe("dashboard session secret configuration", () => {
  test("requires an explicitly configured secret", () => {
    expect(() => resolveSessionSecret({})).toThrow(
      "DASHBOARD_SESSION_SECRET is required",
    );
    expect(() =>
      resolveSessionSecret({
        JWT_SECRET: "legacy-generic-jwt-secret-that-is-not-dashboard-config",
      }),
    ).toThrow("DASHBOARD_SESSION_SECRET is required");
  });

  test("rejects short and documented placeholder secrets", () => {
    expect(() =>
      resolveSessionSecret({ DASHBOARD_SESSION_SECRET: "too-short" }),
    ).toThrow("at least 32 bytes");
    expect(() =>
      resolveSessionSecret({
        DASHBOARD_SESSION_SECRET: "change-me-use-openssl-rand-base64-32",
      }),
    ).toThrow("placeholder");
  });

  test("accepts the explicit test secret only in test mode", () => {
    expect(
      resolveSessionSecret({
        NODE_ENV: "test",
        DASHBOARD_SESSION_SECRET: "test-only-dashboard-session-secret-32-bytes",
      }),
    ).toBe("test-only-dashboard-session-secret-32-bytes");
    expect(() =>
      resolveSessionSecret({
        NODE_ENV: "production",
        DASHBOARD_SESSION_SECRET: "test-only-dashboard-session-secret-32-bytes",
      }),
    ).toThrow("only valid in NODE_ENV=test");
  });
});
