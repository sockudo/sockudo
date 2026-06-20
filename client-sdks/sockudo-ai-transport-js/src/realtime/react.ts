import { useMemo } from "react";
import { useSockudo } from "@sockudo/client/react";
import { adaptSockudoClient, type SockudoClientPeer } from "./adapter.js";
import type { ClientLike } from "./types.js";

/**
 * Reads the outer `@sockudo/client/react` provider and adapts its client into
 * the AI Transport realtime seam.
 *
 * This keeps all `@sockudo/client` imports under `src/realtime`; the React AI
 * layer consumes only the structural `ClientLike` returned here.
 */
export function useSockudoRealtimeClient(): ClientLike {
  const client = useSockudo() as unknown;
  return useMemo(
    () => (isClientLike(client) ? client : adaptSockudoClient(client as SockudoClientPeer)),
    [client],
  );
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
