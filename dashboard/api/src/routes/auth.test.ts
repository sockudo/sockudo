import { expect, test } from "bun:test";
import { Hono } from "hono";
import { LoginRateLimiter } from "../auth/login-rate-limit.ts";
import type { UsersRepository } from "../db/users-repository.ts";
import { createAuthRoutes } from "./auth.ts";

test("the login route returns 429 with Retry-After for a limited account", async () => {
  const users = {
    findByEmail: async () => null,
  } as unknown as UsersRepository;
  const limiter = new LoginRateLimiter({
    windowMs: 60_000,
    maxPerIp: 10,
    maxPerAccount: 1,
    maxTrackedIps: 10,
    maxTrackedAccounts: 10,
    now: () => 1_000,
  });
  const app = new Hono();
  app.route(
    "/auth",
    createAuthRoutes(users, {
      limiter,
      resolveSourceIp: () => "192.0.2.1",
    }),
  );
  const request = () =>
    app.request("/auth/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        email: "target@example.com",
        password: "incorrect",
      }),
    });

  expect((await request()).status).toBe(401);
  const limited = await request();
  expect(limited.status).toBe(429);
  expect(limited.headers.get("retry-after")).toBe("60");
  expect(await limited.json()).toEqual({ error: "Too many login attempts" });
});
