import type { Next } from "hono";
import { readSessionCookie, verifySession } from "./session.ts";
import type { AppContext } from "../types/hono.ts";

export async function requireAuth(c: AppContext, next: Next) {
  const token = readSessionCookie(c.req.header("cookie"));
  if (!token) {
    return c.json({ error: "Unauthorized" }, 401);
  }
  const session = await verifySession(token);
  if (!session) {
    return c.json({ error: "Invalid or expired session" }, 401);
  }
  c.set("session", session);
  await next();
}

export async function requireAdmin(c: AppContext, next: Next) {
  const token = readSessionCookie(c.req.header("cookie"));
  if (!token) {
    return c.json({ error: "Unauthorized" }, 401);
  }
  const session = await verifySession(token);
  if (!session) {
    return c.json({ error: "Invalid or expired session" }, 401);
  }
  if (session.role !== "admin") {
    return c.json({ error: "Admin access required" }, 403);
  }
  c.set("session", session);
  await next();
}
