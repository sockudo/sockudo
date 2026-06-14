import { createDashboardDb } from "./db/dashboard/connection.ts";
import { runMigrations } from "./db/migrations/runner.ts";
import { UsersRepository } from "./db/users-repository.ts";
import { hashPassword } from "./auth/password.ts";
import type { DashboardDb } from "./db/dashboard/types.ts";

export interface DashboardServices {
  db: DashboardDb;
  users: UsersRepository;
}

export async function bootstrapDashboard(): Promise<DashboardServices> {
  const db = createDashboardDb();
  const applied = await runMigrations(db);
  if (applied.length > 0) {
    console.log(`Applied dashboard migrations: ${applied.join(", ")}`);
  }
  const users = new UsersRepository(db);

  const seedEmail = process.env.DASHBOARD_SEED_EMAIL?.trim();
  const seedPassword = process.env.DASHBOARD_SEED_PASSWORD;
  if (seedEmail && seedPassword) {
    try {
      await seedAdminUser(users, {
        email: seedEmail,
        password: seedPassword,
        name: process.env.DASHBOARD_SEED_NAME?.trim(),
      });
    } catch (err) {
      console.warn(
        "Dashboard admin seed skipped:",
        err instanceof Error ? err.message : err,
      );
    }
  }

  return { db, users };
}

export async function seedAdminUser(
  users: UsersRepository,
  input: { email: string; password: string; name?: string },
): Promise<void> {
  const count = await users.count();
  if (count > 0) {
    console.log("Users already exist — skipping seed.");
    return;
  }

  if (input.password.length < 8) {
    throw new Error("Admin password must be at least 8 characters");
  }

  const created = await users.create({
    email: input.email,
    password: input.password,
    name: input.name ?? "Administrator",
    role: "admin",
    active: true,
  });

  console.log(`Seeded admin user: ${created.email}`);
}
