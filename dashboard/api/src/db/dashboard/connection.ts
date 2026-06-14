import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { Database } from "bun:sqlite";
import postgres from "postgres";
import mysql from "mysql2/promise";
import { config } from "../../config.ts";
import type { DashboardDb } from "./types.ts";
import { prepareSql } from "./sql.ts";

class PostgresDashboardDb implements DashboardDb {
  driver = "pgsql" as const;
  private sql: ReturnType<typeof postgres>;

  constructor() {
    const pg = config.database.postgres;
    this.sql = postgres({
      host: pg.host,
      port: pg.port,
      user: pg.user,
      password: pg.password,
      database: pg.database,
      max: 5,
    });
  }

  async query<T>(sql: string, params: (string | number | boolean | null)[] = []): Promise<T[]> {
    return (await this.sql.unsafe(prepareSql(sql, this.driver), params)) as T[];
  }

  async execute(sql: string, params: (string | number | boolean | null)[] = []): Promise<number> {
    const result = await this.sql.unsafe(prepareSql(sql, this.driver), params);
    return result.count;
  }

  async close(): Promise<void> {
    await this.sql.end();
  }
}

class MysqlDashboardDb implements DashboardDb {
  driver = "mysql" as const;
  private pool: mysql.Pool;

  constructor() {
    const my = config.database.mysql;
    this.pool = mysql.createPool({
      host: my.host,
      port: my.port,
      user: my.user,
      password: my.password,
      database: my.database,
      connectionLimit: 5,
    });
  }

  async query<T>(sql: string, params: (string | number | boolean | null)[] = []): Promise<T[]> {
    const [rows] = await this.pool.query(prepareSql(sql, this.driver), params);
    return rows as T[];
  }

  async execute(sql: string, params: (string | number | boolean | null)[] = []): Promise<number> {
    const [result] = await this.pool.query(prepareSql(sql, this.driver), params);
    return (result as { affectedRows?: number }).affectedRows ?? 0;
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

class SqliteDashboardDb implements DashboardDb {
  driver = "sqlite" as const;
  private db: Database;

  constructor() {
    const path = config.database.sqlite.path;
    mkdirSync(dirname(path), { recursive: true });
    this.db = new Database(path, { create: true });
    this.db.exec("PRAGMA foreign_keys = ON;");
  }

  async query<T>(sql: string, params: (string | number | boolean | null)[] = []): Promise<T[]> {
    return this.db.query(sql).all(...params) as T[];
  }

  async execute(sql: string, params: (string | number | boolean | null)[] = []): Promise<number> {
    return this.db.query(sql).run(...params).changes;
  }

  async close(): Promise<void> {
    this.db.close();
  }
}

export function createDashboardDb(): DashboardDb {
  switch (config.dashboardDbDriver) {
    case "pgsql":
      return new PostgresDashboardDb();
    case "mysql":
      return new MysqlDashboardDb();
    case "sqlite":
      return new SqliteDashboardDb();
    default:
      throw new Error(`Unsupported dashboard DB driver`);
  }
}
