import { Hono, type Context } from "hono";
import { createHash } from "node:crypto";
import { isIP } from "node:net";
import {
  clearSessionCookie,
  createSession,
  sessionCookie,
} from "../auth/session.ts";
import { verifyPassword } from "../auth/password.ts";
import { createRequireAuth } from "../auth/middleware.ts";
import {
  createDefaultLoginRateLimiter,
  type LoginRateLimiter,
} from "../auth/login-rate-limit.ts";
import type { UsersRepository } from "../db/users-repository.ts";
import { toPublicUser } from "../types/user.ts";
import type { AppVariables } from "../types/hono.ts";
import { config } from "../config.ts";

interface AuthBindings {
  remoteAddress?: string;
}

interface AuthRouteOptions {
  limiter?: LoginRateLimiter;
  resolveSourceIp?: (c: Context) => string;
}

export function createAuthRoutes(
  usersRepo: UsersRepository,
  options: AuthRouteOptions = {},
) {
  const authRoutes = new Hono<{
    Bindings: AuthBindings;
    Variables: AppVariables;
  }>();
  const limiter = options.limiter ?? createDefaultLoginRateLimiter();
  const resolveSourceIp = options.resolveSourceIp ?? defaultSourceIp;
  const requireAuth = createRequireAuth(usersRepo);

  authRoutes.post("/login", async (c) => {
    const body = await c.req.json<{ email?: string; password?: string }>();
    const email = body.email?.trim().toLowerCase() ?? "";
    const password = body.password ?? "";
    const accountKey = accountFingerprint(email);

    const rate = limiter.consume(resolveSourceIp(c), accountKey);
    if (rate.limited) {
      c.header("Retry-After", String(rate.retryAfterSeconds));
      return c.json({ error: "Too many login attempts" }, 429);
    }

    const user = await usersRepo.findByEmail(email);
    if (!user || !user.active) {
      return c.json({ error: "Invalid credentials" }, 401);
    }

    const valid = await verifyPassword(password, user.password_hash);
    if (!valid) {
      return c.json({ error: "Invalid credentials" }, 401);
    }

    const token = await createSession({
      id: user.id,
      email: user.email,
      name: user.name,
      role: user.role,
      passwordHash: user.password_hash,
    });
    limiter.resetAccount(accountKey);
    c.header("Set-Cookie", sessionCookie(token));
    return c.json(toPublicUser(user));
  });

  authRoutes.post("/logout", (c) => {
    c.header("Set-Cookie", clearSessionCookie());
    return c.json({ ok: true });
  });

  authRoutes.get("/me", requireAuth, (c) =>
    c.json(toPublicUser(c.get("currentUser"))),
  );

  authRoutes.use("/protected-check", requireAuth);
  authRoutes.get("/protected-check", (c) => c.json({ ok: true }));

  return authRoutes;
}

function accountFingerprint(email: string): string {
  return createHash("sha256").update(email, "utf8").digest("base64url");
}

function defaultSourceIp(c: Context): string {
  const direct = (c.env as { remoteAddress?: unknown } | undefined)
    ?.remoteAddress;
  if (typeof direct === "string" && direct.length <= 64 && isIP(direct)) {
    if (!config.trustProxy) return direct;
  }

  if (config.trustProxy) {
    const forwarded = c.req.header("x-forwarded-for");
    if (forwarded && forwarded.length <= 512) {
      const comma = forwarded.indexOf(",");
      const first = forwarded.slice(0, comma === -1 ? undefined : comma).trim();
      if (first.length <= 64 && isIP(first)) return first;
    }
  }

  return typeof direct === "string" && direct.length <= 64 ? direct : "unknown";
}
