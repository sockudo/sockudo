import { Hono } from "hono";
import { UsersController } from "../controllers/users-controller.ts";
import type { UsersRepository } from "../db/users-repository.ts";
import { requireAdmin, requireAuth } from "../auth/middleware.ts";
import type { AppVariables } from "../types/hono.ts";

export function createUsersRoutes(usersRepo: UsersRepository) {
  const controller = new UsersController(usersRepo);
  const users = new Hono<{ Variables: AppVariables }>();

  users.use("*", requireAuth);

  users.get("/", requireAdmin, controller.list);
  users.post("/", requireAdmin, controller.create);
  users.get("/:id", controller.get);
  users.put("/:id", controller.update);
  users.delete("/:id", requireAdmin, controller.remove);
  users.post("/:id/change-password", controller.changePassword);

  return users;
}
