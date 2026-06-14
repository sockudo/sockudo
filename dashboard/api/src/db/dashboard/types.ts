export type DashboardDbDriver = "pgsql" | "mysql" | "sqlite";

export interface DashboardDb {
  driver: DashboardDbDriver;
  query<T>(sql: string, params?: (string | number | boolean | null)[]): Promise<T[]>;
  execute(sql: string, params?: (string | number | boolean | null)[]): Promise<number>;
  close(): Promise<void>;
}
