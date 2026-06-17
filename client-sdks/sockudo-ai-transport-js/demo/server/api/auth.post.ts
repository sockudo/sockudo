import { capabilityJwt } from "../utils/sockudo";

export default defineEventHandler(async (event) => {
  const body = (await readBody(event)) as Record<string, unknown>;
  const clientId =
    typeof body.clientId === "string" && body.clientId.length > 0
      ? body.clientId
      : `nuxt-${crypto.randomUUID()}`;
  return capabilityJwt(clientId);
});
