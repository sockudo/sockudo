import { Hono } from "hono";
import { requireAuth } from "../auth/middleware.ts";
import type { AppsRepository } from "../db/types.ts";
import {
  mergePolicy,
  toPublicApp,
  type AppCreateInput,
  type AppUpdateInput,
} from "../types/app.ts";

function generateSecret(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(24));
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

export function createAppsRoutes(repo: AppsRepository) {
  const apps = new Hono();

  apps.use("*", requireAuth);

  apps.get("/", async (c) => {
    const records = await repo.list();
    return c.json(records.map((app) => toPublicApp(app)));
  });

  apps.get("/:id", async (c) => {
    const app = await repo.findById(c.req.param("id"));
    if (!app) return c.json({ error: "App not found" }, 404);
    const reveal = c.req.query("reveal_secret") === "true";
    return c.json(toPublicApp(app, reveal));
  });

  apps.post("/", async (c) => {
    const body = await c.req.json<AppCreateInput>();
    if (!body.id?.trim() || !body.key?.trim()) {
      return c.json({ error: "id and key are required" }, 400);
    }
    const input: AppCreateInput = {
      id: body.id.trim(),
      key: body.key.trim(),
      secret: body.secret?.trim() || generateSecret(),
      enabled: body.enabled ?? true,
      policy: body.policy ? mergePolicy(body.policy) : undefined,
    };
    try {
      const created = await repo.create(input);
      return c.json(toPublicApp(created, true), 201);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Create failed";
      if (message.includes("duplicate") || message.includes("already exists")) {
        return c.json({ error: message }, 409);
      }
      return c.json({ error: message }, 500);
    }
  });

  apps.put("/:id", async (c) => {
    const body = await c.req.json<AppUpdateInput>();
    try {
      const updated = await repo.update(c.req.param("id"), body);
      return c.json(toPublicApp(updated));
    } catch (err) {
      const message = err instanceof Error ? err.message : "Update failed";
      if (message === "App not found") return c.json({ error: message }, 404);
      return c.json({ error: message }, 500);
    }
  });

  apps.delete("/:id", async (c) => {
    try {
      await repo.delete(c.req.param("id"));
      return c.json({ ok: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Delete failed";
      if (message === "App not found") return c.json({ error: message }, 404);
      return c.json({ error: message }, 500);
    }
  });

  apps.post("/:id/rotate-secret", async (c) => {
    try {
      const updated = await repo.update(c.req.param("id"), {
        secret: generateSecret(),
      });
      return c.json(toPublicApp(updated, true));
    } catch (err) {
      const message = err instanceof Error ? err.message : "Rotate failed";
      if (message === "App not found") return c.json({ error: message }, 404);
      return c.json({ error: message }, 500);
    }
  });

  return apps;
}
