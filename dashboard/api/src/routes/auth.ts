import { Hono } from "hono";
import {
  clearSessionCookie,
  createSession,
  sessionCookie,
  verifySession,
  readSessionCookie,
} from "../auth/session.ts";
import { verifyPassword } from "../auth/password.ts";
import { requireAuth } from "../auth/middleware.ts";
import type { UsersRepository } from "../db/users-repository.ts";
import { toPublicUser } from "../types/user.ts";
import type { AppVariables } from "../types/hono.ts";

export function createAuthRoutes(usersRepo: UsersRepository) {
  const authRoutes = new Hono<{ Variables: AppVariables }>();

  authRoutes.post("/login", async (c) => {
    const body = await c.req.json<{ email?: string; password?: string }>();
    const email = body.email?.trim().toLowerCase() ?? "";
    const password = body.password ?? "";

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
    });
    c.header("Set-Cookie", sessionCookie(token));
    return c.json(toPublicUser(user));
  });

  authRoutes.post("/logout", (c) => {
    c.header("Set-Cookie", clearSessionCookie());
    return c.json({ ok: true });
  });

  authRoutes.get("/me", async (c) => {
    const token = readSessionCookie(c.req.header("cookie"));
    if (!token) return c.json({ error: "Unauthorized" }, 401);
    const session = await verifySession(token);
    if (!session) return c.json({ error: "Unauthorized" }, 401);

    const user = await usersRepo.findById(session.userId);
    if (!user || !user.active) {
      return c.json({ error: "Unauthorized" }, 401);
    }
    return c.json(toPublicUser(user));
  });

  authRoutes.use("/protected-check", requireAuth);
  authRoutes.get("/protected-check", (c) => c.json({ ok: true }));

  return authRoutes;
}
