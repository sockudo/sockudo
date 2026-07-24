import { describe, expect, test } from "bun:test";
import { Hono } from "hono";
import { createSession, sessionCookie } from "./session.ts";
import { createRequireAdmin, createRequireAuth } from "./middleware.ts";
import type { UsersRepository } from "../db/users-repository.ts";
import type { DashboardUser } from "../types/user.ts";

function user(overrides: Partial<DashboardUser> = {}): DashboardUser {
  return {
    id: "user-1",
    email: "admin@example.com",
    password_hash: "$argon2id$credential-version-one",
    name: "Admin",
    role: "admin",
    active: true,
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
    ...overrides,
  };
}

function repository(read: () => DashboardUser | null): UsersRepository {
  return {
    findById: async () => read(),
  } as unknown as UsersRepository;
}

async function cookieFor(value: DashboardUser): Promise<string> {
  const token = await createSession({
    id: value.id,
    email: value.email,
    name: value.name,
    role: value.role,
    passwordHash: value.password_hash,
  });
  return sessionCookie(token).split(";")[0]!;
}

describe("database-backed dashboard authorization", () => {
  test("uses the current database role rather than a stale JWT role", async () => {
    let current = user();
    const app = new Hono();
    app.get("/admin", createRequireAdmin(repository(() => current)), (c) =>
      c.json({ ok: true }),
    );
    const cookie = await cookieFor(current);

    expect((await app.request("/admin", { headers: { cookie } })).status).toBe(
      200,
    );
    current = user({ role: "operator" });
    expect((await app.request("/admin", { headers: { cookie } })).status).toBe(
      403,
    );
  });

  test("rejects disabled, deleted, and password-reset users", async () => {
    let current: DashboardUser | null = user();
    const app = new Hono();
    app.get("/protected", createRequireAuth(repository(() => current)), (c) =>
      c.json({ ok: true }),
    );
    const cookie = await cookieFor(current);

    current = user({ active: false });
    expect(
      (await app.request("/protected", { headers: { cookie } })).status,
    ).toBe(401);

    current = null;
    expect(
      (await app.request("/protected", { headers: { cookie } })).status,
    ).toBe(401);

    current = user({ password_hash: "$argon2id$credential-version-two" });
    expect(
      (await app.request("/protected", { headers: { cookie } })).status,
    ).toBe(401);
  });
});
