import { describe, expect, test } from "bun:test";
import { LoginRateLimiter } from "./login-rate-limit.ts";

describe("LoginRateLimiter", () => {
  test("limits source IPs and account identities independently", () => {
    let now = 1_000;
    const limiter = new LoginRateLimiter({
      windowMs: 60_000,
      maxPerIp: 2,
      maxPerAccount: 2,
      maxTrackedIps: 4,
      maxTrackedAccounts: 4,
      now: () => now,
    });

    expect(limiter.consume("192.0.2.1", "first@example.com").limited).toBe(
      false,
    );
    expect(limiter.consume("192.0.2.1", "second@example.com").limited).toBe(
      false,
    );
    expect(limiter.consume("192.0.2.1", "third@example.com").scope).toBe(
      "ip",
    );

    now += 60_001;
    expect(limiter.consume("192.0.2.2", "target@example.com").limited).toBe(
      false,
    );
    expect(limiter.consume("192.0.2.3", "target@example.com").limited).toBe(
      false,
    );
    expect(limiter.consume("192.0.2.4", "target@example.com").scope).toBe(
      "account",
    );
  });

  test("never grows either tracking map beyond its configured bound", () => {
    const limiter = new LoginRateLimiter({
      windowMs: 60_000,
      maxPerIp: 2,
      maxPerAccount: 2,
      maxTrackedIps: 1,
      maxTrackedAccounts: 1,
      now: () => 1_000,
    });

    expect(limiter.consume("192.0.2.1", "first@example.com").limited).toBe(
      false,
    );
    expect(limiter.consume("192.0.2.2", "second@example.com").limited).toBe(
      true,
    );
    expect(limiter.tracked).toEqual({ ips: 1, accounts: 1 });
  });
});
