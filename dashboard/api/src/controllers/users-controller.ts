import type { UsersRepository } from "../db/users-repository.ts";
import { verifyPassword } from "../auth/password.ts";
import { toPublicUser, type CreateUserInput, type UpdateUserInput } from "../types/user.ts";
import type { AppContext } from "../types/hono.ts";

export class UsersController {
  constructor(private users: UsersRepository) {}

  list = async (c: AppContext) => {
    const records = await this.users.list();
    return c.json(records.map(toPublicUser));
  };

  get = async (c: AppContext) => {
    const session = c.get("session");
    const targetId = c.req.param("id") ?? "";
    if (session.role !== "admin" && session.userId !== targetId) {
      return c.json({ error: "Forbidden" }, 403);
    }
    const user = await this.users.findById(targetId);
    if (!user) return c.json({ error: "User not found" }, 404);
    return c.json(toPublicUser(user));
  };

  create = async (c: AppContext) => {
    const body = await c.req.json<CreateUserInput>();
    if (!body.email?.trim() || !body.password) {
      return c.json({ error: "email and password are required" }, 400);
    }
    if (body.password.length < 8) {
      return c.json({ error: "password must be at least 8 characters" }, 400);
    }
    try {
      const created = await this.users.create(body);
      return c.json(toPublicUser(created), 201);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Create failed";
      if (message.includes("UNIQUE") || message.includes("duplicate")) {
        return c.json({ error: "Email already in use" }, 409);
      }
      return c.json({ error: message }, 500);
    }
  };

  update = async (c: AppContext) => {
    const body = await c.req.json<UpdateUserInput>();
    const session = c.get("session");
    const targetId = c.req.param("id") ?? "";

    if (body.password && body.password.length < 8) {
      return c.json({ error: "password must be at least 8 characters" }, 400);
    }

    if (session.role !== "admin" && session.userId !== targetId) {
      return c.json({ error: "Forbidden" }, 403);
    }

    if (session.role !== "admin") {
      delete body.role;
      delete body.active;
      delete body.email;
    }

    try {
      const updated = await this.users.update(targetId, body);
      return c.json(toPublicUser(updated));
    } catch (err) {
      const message = err instanceof Error ? err.message : "Update failed";
      if (message === "User not found") return c.json({ error: message }, 404);
      if (message.includes("UNIQUE") || message.includes("duplicate")) {
        return c.json({ error: "Email already in use" }, 409);
      }
      return c.json({ error: message }, 500);
    }
  };

  remove = async (c: AppContext) => {
    const session = c.get("session");
    const targetId = c.req.param("id") ?? "";

    if (session.userId === targetId) {
      return c.json({ error: "Cannot delete your own account" }, 400);
    }

    try {
      await this.users.delete(targetId);
      return c.json({ ok: true });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Delete failed";
      if (message === "User not found") return c.json({ error: message }, 404);
      return c.json({ error: message }, 500);
    }
  };

  changePassword = async (c: AppContext) => {
    const session = c.get("session");
    const targetId = c.req.param("id") ?? "";
    const body = await c.req.json<{
      current_password?: string;
      new_password?: string;
    }>();

    if (!body.new_password || body.new_password.length < 8) {
      return c.json({ error: "new_password must be at least 8 characters" }, 400);
    }

    const user = await this.users.findById(targetId);
    if (!user) return c.json({ error: "User not found" }, 404);

    const selfService = session.userId === targetId;
    if (selfService) {
      if (!body.current_password) {
        return c.json({ error: "current_password is required" }, 400);
      }
      const valid = await verifyPassword(body.current_password, user.password_hash);
      if (!valid) return c.json({ error: "Invalid current password" }, 401);
    } else if (session.role !== "admin") {
      return c.json({ error: "Forbidden" }, 403);
    }

    const updated = await this.users.update(targetId, {
      password: body.new_password,
    });
    return c.json(toPublicUser(updated));
  };
}
