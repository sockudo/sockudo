import { bootstrapDashboard } from "../bootstrap.ts";

const { db } = await bootstrapDashboard();
await db.close();
console.log("Dashboard migrations are up to date.");
