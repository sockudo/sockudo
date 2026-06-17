import { useSockudo } from "@sockudo/client/vue";
import { adaptSockudoClient, type SockudoClientPeer } from "./adapter.js";
import type { ClientLike } from "./types.js";

/**
 * Reads the outer `@sockudo/client/vue` provider and adapts its client into the
 * AI Transport realtime seam.
 */
export function useSockudoRealtimeClient(
  explicitClient?: ClientLike | SockudoClientPeer,
): ClientLike {
  const client = explicitClient ?? (useSockudo() as unknown);
  return isClientLike(client)
    ? client
    : adaptSockudoClient(client as SockudoClientPeer);
}

function isClientLike(client: unknown): client is ClientLike {
  if (typeof client !== "object" || client === null) {
    return false;
  }
  const record = client as Record<string, unknown>;
  const channels = record.channels;
  if (typeof channels !== "object" || channels === null) {
    return false;
  }
  const channelRecord = channels as Record<string, unknown>;
  return typeof channelRecord.get === "function";
}
