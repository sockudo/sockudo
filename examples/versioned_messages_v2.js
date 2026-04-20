/**
 * Release 4.3 mutable-message consumption example.
 *
 * This example shows the intended client-side interpretation rules for
 * Protocol V2 mutable messages:
 *
 * - sockudo:message.update => replace local state with the full payload
 * - sockudo:message.delete => latest visible version is deleted
 * - sockudo:message.append => concatenate onto the current string state
 *
 * Run from a workspace that has @sockudo/client available.
 */

import Sockudo, {
  isMutableMessageEvent,
  reduceMutableMessageEvent,
} from "@sockudo/client";

const client = new Sockudo("app-key", {
  wsHost: "127.0.0.1",
  wsPort: 6001,
  forceTLS: false,
  protocolVersion: 2,
});

const channel = client.subscribe("chat:room-1");

let state = null;

channel.bind_global((eventName, payload) => {
  if (!isMutableMessageEvent(payload)) {
    console.log("non-mutable event", eventName, payload);
    return;
  }

  try {
    state = reduceMutableMessageEvent(state, payload);
    console.log("mutable state", {
      messageSerial: state.messageSerial,
      action: state.action,
      data: state.data,
      versionSerial: state.versionSerial,
    });
  } catch (error) {
    console.error("mutable message reduction failed", error);
    console.error(
      "If this was a message.append without a seeded string base, fetch the latest visible message first.",
    );
  }
});

client.connect();

console.log("Subscribed to chat:room-1 with Protocol V2 mutable-message handling.");
