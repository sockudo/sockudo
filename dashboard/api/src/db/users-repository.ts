import type { DashboardDb } from "./dashboard/types.ts";
import { hashPassword } from "../auth/password.ts";
import type {
  CreateUserInput,
  DashboardUser,
  UpdateUserInput,
  UserRole,
} from "../types/user.ts";

function mapUser(row: Record<string, unknown>): DashboardUser {
  return {
    id: String(row.id),
    email: String(row.email),
    password_hash: String(row.password_hash),
    name: String(row.name ?? ""),
    role: String(row.role) as UserRole,
    active: row.active === true || row.active === 1 || row.active === "1",
    created_at: String(row.created_at),
    updated_at: String(row.updated_at),
  };
}

export class UsersRepository {
  constructor(private db: DashboardDb) {}

  async count(): Promise<number> {
    const rows = await this.db.query<{ count: number | string }>(
      "SELECT COUNT(*) AS count FROM dashboard_users",
    );
    return Number(rows[0]?.count ?? 0);
  }

  async list(): Promise<DashboardUser[]> {
    const rows = await this.db.query<Record<string, unknown>>(
      `SELECT id, email, password_hash, name, role, active, created_at, updated_at
       FROM dashboard_users ORDER BY created_at ASC`,
    );
    return rows.map(mapUser);
  }

  async findById(id: string): Promise<DashboardUser | null> {
    const rows = await this.db.query<Record<string, unknown>>(
      `SELECT id, email, password_hash, name, role, active, created_at, updated_at
       FROM dashboard_users WHERE id = ?`,
      [id],
    );
    return rows[0] ? mapUser(rows[0]) : null;
  }

  async findByEmail(email: string): Promise<DashboardUser | null> {
    const rows = await this.db.query<Record<string, unknown>>(
      `SELECT id, email, password_hash, name, role, active, created_at, updated_at
       FROM dashboard_users WHERE email = ?`,
      [email.toLowerCase()],
    );
    return rows[0] ? mapUser(rows[0]) : null;
  }

  async create(input: CreateUserInput): Promise<DashboardUser> {
    const id = crypto.randomUUID();
    const password_hash = await hashPassword(input.password);
    const email = input.email.trim().toLowerCase();
    const name = input.name?.trim() || email.split("@")[0];
    const role = input.role ?? "operator";
    const active = input.active ?? true;

    await this.db.execute(
      `INSERT INTO dashboard_users (id, email, password_hash, name, role, active)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [id, email, password_hash, name, role, active ? 1 : 0],
    );

    const created = await this.findById(id);
    if (!created) throw new Error("Failed to create user");
    return created;
  }

  async update(id: string, input: UpdateUserInput): Promise<DashboardUser> {
    const existing = await this.findById(id);
    if (!existing) throw new Error("User not found");

    const email = input.email?.trim().toLowerCase() ?? existing.email;
    const name = input.name?.trim() ?? existing.name;
    const role = input.role ?? existing.role;
    const active = input.active ?? existing.active;
    const password_hash = input.password
      ? await hashPassword(input.password)
      : existing.password_hash;

    await this.db.execute(
      `UPDATE dashboard_users
       SET email = ?, password_hash = ?, name = ?, role = ?, active = ?, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [email, password_hash, name, role, active ? 1 : 0, id],
    );

    const updated = await this.findById(id);
    if (!updated) throw new Error("Failed to update user");
    return updated;
  }

  async delete(id: string): Promise<void> {
    const affected = await this.db.execute(
      "DELETE FROM dashboard_users WHERE id = ?",
      [id],
    );
    if (!affected) throw new Error("User not found");
  }
}
