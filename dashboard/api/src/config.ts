import "./load-env.ts";
import { resolveSessionSecret } from "./auth/configuration.ts";

export type AppManagerDriver = "mysql" | "pgsql" | "dynamodb";
export type DashboardDbDriver = "pgsql" | "mysql" | "sqlite";

function normalizeAppDriver(raw: string): AppManagerDriver | "memory" {
  const value = raw.toLowerCase();
  if (value === "mysql") return "mysql";
  if (value === "pgsql" || value === "postgres") return "pgsql";
  if (value === "dynamodb") return "dynamodb";
  return "memory";
}

function resolveDashboardDbDriver(): DashboardDbDriver {
  const explicit = process.env.DASHBOARD_DATABASE_DRIVER?.toLowerCase();
  if (explicit === "sqlite") return "sqlite";
  if (explicit === "mysql") return "mysql";
  if (explicit === "pgsql" || explicit === "postgres") return "pgsql";

  const appDriver = normalizeAppDriver(process.env.APP_MANAGER_DRIVER ?? "pgsql");
  if (appDriver === "mysql") return "mysql";
  if (appDriver === "pgsql") return "pgsql";
  return "sqlite";
}

export const config = {
  port: parseInt(process.env.DASHBOARD_API_PORT ?? "3460", 10),
  corsOrigin: process.env.DASHBOARD_CORS_ORIGIN ?? "http://localhost:5174",
  sessionSecret: resolveSessionSecret(process.env),
  appManagerDriver: normalizeAppDriver(
    process.env.APP_MANAGER_DRIVER ?? "pgsql",
  ),
  dashboardDbDriver: resolveDashboardDbDriver(),
  sockudoHttpUrl: process.env.SOCKUDO_HTTP_URL ?? "http://127.0.0.1:6001",
  sockudoMetricsUrl: process.env.SOCKUDO_METRICS_URL ?? "http://127.0.0.1:9601",
  trustProxy: process.env.DASHBOARD_TRUST_PROXY === "true",
  database: {
    mysql: {
      host: process.env.DATABASE_MYSQL_HOST ?? "127.0.0.1",
      port: parseInt(process.env.DATABASE_MYSQL_PORT ?? "3306", 10),
      user: process.env.DATABASE_MYSQL_USERNAME ?? "sockudo",
      password: process.env.DATABASE_MYSQL_PASSWORD ?? "",
      database: process.env.DATABASE_MYSQL_DATABASE ?? "sockudo",
      table: process.env.DATABASE_MYSQL_TABLE_NAME ?? "applications",
    },
    postgres: {
      host: process.env.DATABASE_POSTGRES_HOST ?? "127.0.0.1",
      port: parseInt(process.env.DATABASE_POSTGRES_PORT ?? "5432", 10),
      user: process.env.DATABASE_POSTGRES_USERNAME ?? "sockudo",
      password: process.env.DATABASE_POSTGRES_PASSWORD ?? "",
      database: process.env.DATABASE_POSTGRES_DATABASE ?? "sockudo",
      table: process.env.DATABASE_POSTGRES_TABLE_NAME ?? "applications",
    },
    dynamodb: {
      region: process.env.DATABASE_DYNAMODB_REGION ?? "us-east-1",
      table: process.env.DATABASE_DYNAMODB_TABLE_NAME ?? "sockudo-applications",
      endpoint: process.env.DATABASE_DYNAMODB_ENDPOINT_URL,
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
    sqlite: {
      path:
        process.env.DASHBOARD_SQLITE_PATH ??
        `${process.cwd()}/data/dashboard.sqlite`,
    },
  },
};

export function getAppManagerDriver(): AppManagerDriver {
  if (config.appManagerDriver === "memory") {
    throw new Error(
      'Unsupported APP_MANAGER_DRIVER="memory". Dashboard requires mysql, pgsql, or dynamodb.',
    );
  }
  return config.appManagerDriver;
}
