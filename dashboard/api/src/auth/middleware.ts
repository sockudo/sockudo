import type { MiddlewareHandler, Next } from "hono";
import {
  readSessionCookie,
  sessionMatchesPasswordHash,
  verifySession,
} from "./session.ts";
import type { UsersRepository } from "../db/users-repository.ts";
import type { AppContext, AppVariables } from "../types/hono.ts";

async function authenticate(
  c: AppContext,
  users: UsersRepository,
): Promise<boolean> {
  const token = readSessionCookie(c.req.header("cookie"));
  if (!token) return false;

  const session = await verifySession(token);
  if (!session) return false;

  const user = await users.findById(session.userId);
  if (
    !user ||
    !user.active ||
    !sessionMatchesPasswordHash(session, user.password_hash)
  ) {
    return false;
  }

  c.set("session", {
    ...session,
    email: user.email,
    name: user.name,
    role: user.role,
  });
  c.set("currentUser", user);
  return true;
}

function createAuthMiddleware(
  authorize: (c: AppContext, next: Next) => Promise<Response | void>,
): MiddlewareHandler<{ Variables: AppVariables }> {
  return async (c, next) => {
    return authorize(c as AppContext, next);
  };
}

export function createRequireAuth(
  users: UsersRepository,
): MiddlewareHandler<{ Variables: AppVariables }> {
  return createAuthMiddleware(async (c, next) => {
    if (!(await authenticate(c, users))) {
      return c.json({ error: "Invalid or expired session" }, 401);
    }
    await next();
  });
}

export function createRequireAdmin(
  users: UsersRepository,
): MiddlewareHandler<{ Variables: AppVariables }> {
  return createAuthMiddleware(async (c, next) => {
    if (!(await authenticate(c, users))) {
      return c.json({ error: "Invalid or expired session" }, 401);
    }
    if (c.get("session").role !== "admin") {
      return c.json({ error: "Admin access required" }, 403);
    }
    await next();
  });
}
