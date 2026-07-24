import type { Context } from "hono";
import type { SessionPayload } from "../auth/session.ts";
import type { DashboardUser } from "./user.ts";

export type AppVariables = {
  session: SessionPayload;
  currentUser: DashboardUser;
};

export type AppContext = Context<{ Variables: AppVariables }>;
