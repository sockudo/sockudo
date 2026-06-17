import { channelAuth, isAllowedDemoChannel } from "../utils/sockudo";

export default defineEventHandler(async (event) => {
  const body = (await readBody(event)) as Record<string, unknown>;
  const socketId = stringValue(body.socket_id ?? body.socketId);
  const channelName = stringValue(body.channel_name ?? body.channelName);
  if (socketId === undefined || channelName === undefined) {
    throw createError({ statusCode: 400, statusMessage: "missing auth fields" });
  }
  if (!isAllowedDemoChannel(channelName)) {
    throw createError({ statusCode: 403, statusMessage: "unexpected channel" });
  }
  return channelAuth(socketId, channelName);
});

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}
