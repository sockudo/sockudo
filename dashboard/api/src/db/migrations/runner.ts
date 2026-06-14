import type { DashboardDb, DashboardDbDriver } from "../dashboard/types.ts";

export interface Migration {
  id: string;
  up: Record<DashboardDbDriver, string[]>;
}

export const migrations: Migration[] = [
  {
    id: "001_dashboard_users",
    up: {
      pgsql: [
        `CREATE TABLE IF NOT EXISTS dashboard_migrations (
          id VARCHAR(255) PRIMARY KEY,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )`,
        `CREATE TABLE IF NOT EXISTS dashboard_users (
          id VARCHAR(36) PRIMARY KEY,
          email VARCHAR(255) NOT NULL UNIQUE,
          password_hash VARCHAR(255) NOT NULL,
          name VARCHAR(255) NOT NULL DEFAULT '',
          role VARCHAR(50) NOT NULL DEFAULT 'operator',
          active BOOLEAN NOT NULL DEFAULT TRUE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )`,
        `CREATE INDEX IF NOT EXISTS idx_dashboard_users_email ON dashboard_users(email)`,
      ],
      mysql: [
        `CREATE TABLE IF NOT EXISTS dashboard_migrations (
          id VARCHAR(255) PRIMARY KEY,
          applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )`,
        `CREATE TABLE IF NOT EXISTS dashboard_users (
          id VARCHAR(36) PRIMARY KEY,
          email VARCHAR(255) NOT NULL UNIQUE,
          password_hash VARCHAR(255) NOT NULL,
          name VARCHAR(255) NOT NULL DEFAULT '',
          role VARCHAR(50) NOT NULL DEFAULT 'operator',
          active TINYINT(1) NOT NULL DEFAULT 1,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )`,
        `CREATE INDEX idx_dashboard_users_email ON dashboard_users(email)`,
      ],
      sqlite: [
        `CREATE TABLE IF NOT EXISTS dashboard_migrations (
          id TEXT PRIMARY KEY,
          applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        )`,
        `CREATE TABLE IF NOT EXISTS dashboard_users (
          id TEXT PRIMARY KEY,
          email TEXT NOT NULL UNIQUE,
          password_hash TEXT NOT NULL,
          name TEXT NOT NULL DEFAULT '',
          role TEXT NOT NULL DEFAULT 'operator',
          active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )`,
        `CREATE INDEX IF NOT EXISTS idx_dashboard_users_email ON dashboard_users(email)`,
      ],
    },
  },
];

export async function runMigrations(db: DashboardDb): Promise<string[]> {
  const applied: string[] = [];

  for (const migration of migrations) {
    const existing = await db.query<{ id: string }>(
      "SELECT id FROM dashboard_migrations WHERE id = ?",
      [migration.id],
    ).catch(() => [] as { id: string }[]);

    if (existing.length > 0) continue;

    const statements = migration.up[db.driver];
    for (const sql of statements) {
      await db.execute(sql);
    }

    await db.execute(
      "INSERT INTO dashboard_migrations (id) VALUES (?)",
      [migration.id],
    );
    applied.push(migration.id);
  }

  return applied;
}
