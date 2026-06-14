import { bootstrapDashboard, seedAdminUser } from "../bootstrap.ts";

const email = process.argv[2] ?? process.env.DASHBOARD_SEED_EMAIL;
const password = process.argv[3] ?? process.env.DASHBOARD_SEED_PASSWORD;
const name = process.argv[4] ?? process.env.DASHBOARD_SEED_NAME;

if (!email || !password) {
  console.error(
    "Usage: bun run seed:admin <email> <password> [name]\n" +
      "   or: DASHBOARD_SEED_EMAIL=... DASHBOARD_SEED_PASSWORD=... bun run seed:admin",
  );
  process.exit(1);
}

const { db, users } = await bootstrapDashboard();
try {
  await seedAdminUser(users, { email, password, name });
} finally {
  await db.close();
}
