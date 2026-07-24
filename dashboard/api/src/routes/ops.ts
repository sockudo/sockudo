import { Hono } from "hono";
import { createRequireAuth } from "../auth/middleware.ts";
import type { UsersRepository } from "../db/users-repository.ts";
import type { AppVariables } from "../types/hono.ts";
import { config } from "../config.ts";
import {
  fetchPrometheusMetrics,
  fetchSockudoHealth,
  fetchSockudoStats,
  fetchSockudoUsage,
  parsePrometheusText,
  summarizeMetrics,
} from "../services/sockudo.ts";

export function createOpsRoutes(usersRepo: UsersRepository) {
  const opsRoutes = new Hono<{ Variables: AppVariables }>();
  const requireAuth = createRequireAuth(usersRepo);

  opsRoutes.use("*", requireAuth);

  opsRoutes.get("/stats", async (c) => {
    try {
      return c.json(await fetchSockudoStats());
    } catch (err) {
      const message = err instanceof Error ? err.message : "Stats unavailable";
      return c.json({ error: message }, 502);
    }
  });

  opsRoutes.get("/usage", async (c) => {
    try {
      return c.json(await fetchSockudoUsage());
    } catch (err) {
      const message = err instanceof Error ? err.message : "Usage unavailable";
      return c.json({ error: message }, 502);
    }
  });

  opsRoutes.get("/health", async (c) => {
    try {
      const appId = c.req.query("app_id");
      return c.json(await fetchSockudoHealth(appId));
    } catch (err) {
      const message = err instanceof Error ? err.message : "Health check failed";
      return c.json({ error: message }, 502);
    }
  });

  opsRoutes.get("/metrics", async (c) => {
    try {
      const text = await fetchPrometheusMetrics();
      if (c.req.query("format") === "text") {
        return c.text(text);
      }
      return c.json({ samples: parsePrometheusText(text) });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Metrics unavailable";
      return c.json({ error: message }, 502);
    }
  });

  opsRoutes.get("/metrics/summary", async (c) => {
    try {
      const text = await fetchPrometheusMetrics();
      return c.json(summarizeMetrics(parsePrometheusText(text)));
    } catch (err) {
      const message = err instanceof Error ? err.message : "Metrics unavailable";
      return c.json({ error: message }, 502);
    }
  });

  opsRoutes.get("/config", (c) =>
    c.json({
      app_manager_driver: config.appManagerDriver,
      sockudo_http_url: config.sockudoHttpUrl,
      sockudo_metrics_url: config.sockudoMetricsUrl,
    }),
  );

  return opsRoutes;
}
