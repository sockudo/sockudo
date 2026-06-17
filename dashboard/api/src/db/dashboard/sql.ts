import type { DashboardDbDriver } from "./types.ts";

export function prepareSql(sql: string, driver: DashboardDbDriver): string {
  if (driver !== "pgsql") return sql;
  let index = 0;
  return sql.replace(/\?/g, () => `$${++index}`);
}
