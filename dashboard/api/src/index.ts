import { Hono } from "hono";
import { cors } from "hono/cors";
import { config, getAppManagerDriver } from "./config.ts";
import { bootstrapDashboard } from "./bootstrap.ts";
import { createAppsRepository } from "./db/factory.ts";
import { createAuthRoutes } from "./routes/auth.ts";
import { createAppsRoutes } from "./routes/apps.ts";
import { createWebhooksRoutes } from "./routes/webhooks.ts";
import { createUsersRoutes } from "./routes/users.ts";
import { opsRoutes } from "./routes/ops.ts";

const appsRepo = createAppsRepository();
const dashboard = await bootstrapDashboard();

const app = new Hono();

app.use(
  "*",
  cors({
    origin: config.corsOrigin,
    credentials: true,
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type"],
  }),
);

app.get("/health", (c) =>
  c.json({
    status: "ok",
    app_manager_driver: getAppManagerDriver(),
    dashboard_db_driver: config.dashboardDbDriver,
  }),
);

app.route("/api/v1/auth", createAuthRoutes(dashboard.users));
app.route("/api/v1/users", createUsersRoutes(dashboard.users));
app.route("/api/v1/apps", createAppsRoutes(appsRepo));
app.route("/api/v1/apps", createWebhooksRoutes(appsRepo));
app.route("/api/v1/ops", opsRoutes);

const server = Bun.serve({
  port: config.port,
  fetch: app.fetch,
});

console.log(`
┌──────────────────────────────────────────────────┐
│  Sockudo Operator Dashboard API                  │
│                                                  │
│  http://localhost:${String(config.port).padEnd(28)}│
│  App store: ${getAppManagerDriver().padEnd(34)}│
│  Dashboard DB: ${config.dashboardDbDriver.padEnd(31)}│
│  Sockudo: ${config.sockudoHttpUrl.padEnd(35)}│
└──────────────────────────────────────────────────┘
`);

process.on("SIGINT", async () => {
  await appsRepo.close();
  await dashboard.db.close();
  server.stop();
  process.exit(0);
});

export default app;
