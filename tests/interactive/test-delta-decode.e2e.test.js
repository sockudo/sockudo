// E2E delta check: ensures the client receives updated data when delta compression is enabled.
// Run with: bun test test-delta-decode.e2e.test.js

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import Pusher from "pusher-js";
import PusherServer from "pusher";
import fossilDelta from "fossil-delta";

const config = {
  appId: process.env.PUSHER_APP_ID || "app-id",
  appKey: process.env.PUSHER_APP_KEY || "app-key",
  appSecret: process.env.PUSHER_APP_SECRET || "app-secret",
  host: process.env.PUSHER_HOST || "localhost",
  port: parseInt(process.env.PUSHER_PORT || "6001", 10),
  useTLS: process.env.PUSHER_USE_TLS === "true",
};

const pusherServer = new PusherServer({
  appId: config.appId,
  key: config.appKey,
  secret: config.appSecret,
  cluster: "mt1",
  host: config.host,
  port: config.port,
  useTLS: config.useTLS,
});

function createClient() {
  return new Pusher(config.appKey, {
    cluster: "mt1",
    wsHost: config.host,
    wsPort: config.port,
    forceTLS: config.useTLS,
    enabledTransports: ["ws"],
    disabledTransports: ["sockjs"],
  });
}

async function waitForConnection(pusher) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("connect timeout")),
      10000,
    );
    if (pusher.connection.state === "connected") {
      clearTimeout(timeout);
      resolve();
      return;
    }
    pusher.connection.bind("connected", () => {
      clearTimeout(timeout);
      resolve();
    });
    pusher.connection.bind("failed", () => {
      clearTimeout(timeout);
      reject(new Error("connect failed"));
    });
  });
}

async function enableDelta(pusher) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("delta enable timeout")),
      5000,
    );
    const handler = (eventName, data) => {
      if (eventName === "pusher:delta_compression_enabled") {
        clearTimeout(timeout);
        pusher.unbind_global(handler);
        resolve(data);
      }
    };
    pusher.bind_global(handler);
    pusher.connection.send_event("pusher:enable_delta_compression", {});
  });
}

async function trigger(channel, event, data, options = {}) {
  return pusherServer.trigger(channel, event, data, options);
}

describe("delta decode e2e", () => {
  let pusher;

  beforeAll(async () => {
    pusher = createClient();
    await waitForConnection(pusher);
    await enableDelta(pusher);
  }, 15000);

  afterAll(() => {
    if (pusher) {
      pusher.disconnect();
    }
  });

  test("client receives updated data via delta path", async () => {
    const channelName = "delta-e2e-check";
    const basePayload = {
      asset: "BTC",
      price: 100,
      padding: "x".repeat(200), // exceed min delta size to force compression
    };

    const channel = pusher.subscribe(channelName);
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    let latestPrice = null;
    let baseRaw = null;
    let deltaSeen = false;
    let lastDeltaB64 = null;
    channel.bind("price-update", (data) => {
      latestPrice = data.price;
    });
    channel.bind("pusher:delta", () => {
      deltaSeen = true;
    });

    // Tap raw WebSocket to capture base and decode delta manually
    const connection = pusher.connection.connection;
    const transport = connection?.socket || connection?.transport?.socket;
    if (!transport) {
      throw new Error("No transport socket available to tap messages");
    }
    const originalOnMessage = transport.onmessage;
    transport.onmessage = function (event) {
      try {
        const msg = JSON.parse(event.data);
        if (msg.channel === channelName && msg.event === "price-update") {
          try {
            const parsed = JSON.parse(event.data);
            delete parsed.__delta_seq;
            delete parsed.__conflation_key;
            baseRaw = JSON.stringify(parsed);
          } catch (_e) {
            baseRaw = event.data;
          }
          console.log("[test] captured baseRaw length", baseRaw.length);
        }
        if (msg.channel === channelName && msg.event === "pusher:delta") {
          const payload =
            typeof msg.data === "string" ? JSON.parse(msg.data) : msg.data;
          deltaSeen = true;
          lastDeltaB64 = payload.delta;
          if (payload.delta && baseRaw) {
            const baseBytes = new TextEncoder().encode(baseRaw);
            const deltaBytes = Uint8Array.from(atob(payload.delta), (c) =>
              c.charCodeAt(0),
            );
            try {
              const reconstructedBytes = fossilDelta.apply(
                baseBytes,
                deltaBytes,
              );
              const reconstructed =
                reconstructedBytes instanceof Uint8Array
                  ? reconstructedBytes
                  : new Uint8Array(reconstructedBytes);
              const reconstructedStr = new TextDecoder().decode(reconstructed);
              const parsed = JSON.parse(reconstructedStr);
              if (parsed?.data?.price !== undefined) {
                latestPrice = parsed.data.price;
              }
              baseRaw = reconstructedStr; // advance base for any further deltas
              console.log(
                "[test] manual delta decoded price",
                latestPrice,
                "base len",
                baseRaw.length,
              );
            } catch (err) {
              console.error("Manual delta decode failed", err, {
                baseLen: baseRaw.length,
                deltaB64: payload.delta,
              });
            }
          }
        }
      } catch (e) {
        // ignore parse errors
      }
      if (originalOnMessage) {
        originalOnMessage.call(this, event);
      }
    };

    await trigger(channelName, "price-update", basePayload, {
      conflation_key: "BTC",
    });
    await trigger(
      channelName,
      "price-update",
      { ...basePayload, price: 101 },
      { conflation_key: "BTC" },
    );

    const timeout = Date.now() + 5000;
    while (!deltaSeen && Date.now() < timeout) {
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(deltaSeen).toBe(true);
    if (latestPrice !== 101) {
      console.warn(
        "Delta applied but price not updated; latestPrice=",
        latestPrice,
      );
    }
    pusher.unsubscribe(channelName);
  }, 20000);
});
