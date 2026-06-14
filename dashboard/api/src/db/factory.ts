import { getAppManagerDriver } from "../config.ts";
import { DynamodbAppsRepository } from "./dynamodb.ts";
import { MysqlAppsRepository } from "./mysql.ts";
import { PostgresAppsRepository } from "./postgres.ts";
import type { AppsRepository } from "./types.ts";

export function createAppsRepository(): AppsRepository {
  switch (getAppManagerDriver()) {
    case "mysql":
      return new MysqlAppsRepository();
    case "pgsql":
      return new PostgresAppsRepository();
    case "dynamodb":
      return new DynamodbAppsRepository();
    default:
      throw new Error(`Unsupported app manager driver`);
  }
}
