import type { Context } from "hono";
import type { SessionPayload } from "../auth/session.ts";

export type AppVariables = {
  session: SessionPayload;
};

export type AppContext = Context<{ Variables: AppVariables }>;
