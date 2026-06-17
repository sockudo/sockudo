import { proxyVersionedMessage } from "../utils/sockudo";

export default defineEventHandler(async (event) => {
  const body = (await readBody(event)) as Record<string, unknown>;
  return proxyVersionedMessage(body);
});
