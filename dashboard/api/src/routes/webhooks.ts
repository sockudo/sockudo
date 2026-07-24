import { Hono } from "hono";
import { createRequireAuth } from "../auth/middleware.ts";
import type { UsersRepository } from "../db/users-repository.ts";
import type { AppVariables } from "../types/hono.ts";
import type { AppsRepository } from "../db/types.ts";
import { toPublicApp, type Webhook } from "../types/app.ts";

export function createWebhooksRoutes(
  repo: AppsRepository,
  usersRepo: UsersRepository,
) {
  const webhooks = new Hono<{ Variables: AppVariables }>();
  const requireAuth = createRequireAuth(usersRepo);

  webhooks.use("*", requireAuth);

  webhooks.get("/:appId/webhooks", async (c) => {
    const app = await repo.findById(c.req.param("appId"));
    if (!app) return c.json({ error: "App not found" }, 404);
    return c.json(app.policy.webhooks ?? []);
  });

  webhooks.post("/:appId/webhooks", async (c) => {
    const app = await repo.findById(c.req.param("appId"));
    if (!app) return c.json({ error: "App not found" }, 404);
    const body = await c.req.json<Webhook>();
    if (!body.event_types?.length) {
      return c.json({ error: "event_types is required" }, 400);
    }
    if (!body.url && !body.lambda_function && !body.lambda) {
      return c.json({ error: "url or lambda target is required" }, 400);
    }
    const list = [...(app.policy.webhooks ?? []), body];
    const updated = await repo.update(app.id, {
      policy: { ...app.policy, webhooks: list },
    });
    return c.json(toPublicApp(updated), 201);
  });

  webhooks.put("/:appId/webhooks/:index", async (c) => {
    const app = await repo.findById(c.req.param("appId"));
    if (!app) return c.json({ error: "App not found" }, 404);
    const index = Number(c.req.param("index"));
    const list = [...(app.policy.webhooks ?? [])];
    if (!Number.isInteger(index) || index < 0 || index >= list.length) {
      return c.json({ error: "Webhook not found" }, 404);
    }
    const body = await c.req.json<Webhook>();
    if (!body.event_types?.length) {
      return c.json({ error: "event_types is required" }, 400);
    }
    list[index] = body;
    const updated = await repo.update(app.id, {
      policy: { ...app.policy, webhooks: list },
    });
    return c.json(toPublicApp(updated));
  });

  webhooks.delete("/:appId/webhooks/:index", async (c) => {
    const app = await repo.findById(c.req.param("appId"));
    if (!app) return c.json({ error: "App not found" }, 404);
    const index = Number(c.req.param("index"));
    const list = [...(app.policy.webhooks ?? [])];
    if (!Number.isInteger(index) || index < 0 || index >= list.length) {
      return c.json({ error: "Webhook not found" }, 404);
    }
    list.splice(index, 1);
    const updated = await repo.update(app.id, {
      policy: { ...app.policy, webhooks: list },
    });
    return c.json(toPublicApp(updated));
  });

  return webhooks;
}
