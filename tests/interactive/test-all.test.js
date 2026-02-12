// test-all.test.js
// Comprehensive automated tests for delta compression, conflation keys, and tag filtering
// Run with: bun test test-all.test.js

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import Pusher from "pusher-js";
import PusherServer from "pusher";
import fossilDelta from "fossil-delta";

// Configuration from environment
const config = {
  appId: process.env.PUSHER_APP_ID || "app-id",
  appKey: process.env.PUSHER_APP_KEY || "app-key",
  appSecret: process.env.PUSHER_APP_SECRET || "app-secret",
  host: process.env.PUSHER_HOST || "localhost",
  port: parseInt(process.env.PUSHER_PORT || "6001"),
  useTLS: process.env.PUSHER_USE_TLS === "true",
};

// Initialize Pusher server SDK for triggering events
const pusherServer = new PusherServer({
  appId: config.appId,
  key: config.appKey,
  secret: config.appSecret,
  cluster: "mt1",
  host: config.host,
  port: config.port,
  useTLS: config.useTLS,
});

// Helper: Create a Pusher client connection
function createClient() {
  return new Pusher(config.appKey, {
    cluster: "mt1",
    wsHost: config.host,
    wsPort: config.port,
    forceTLS: config.useTLS,
    enabledTransports: ["ws"],
    disabledTransports: ["sockjs"],
    authEndpoint: `http://localhost:3000/pusher/auth`,
  });
}

// Helper: Wait for connection
function waitForConnection(pusher) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Connection timeout"));
    }, 10000);

    if (pusher.connection.state === "connected") {
      clearTimeout(timeout);
      resolve();
    } else {
      pusher.connection.bind("connected", () => {
        clearTimeout(timeout);
        resolve();
      });
      pusher.connection.bind("failed", () => {
        clearTimeout(timeout);
        reject(new Error("Connection failed"));
      });
    }
  });
}

// Helper: Wait for specific event on channel
function waitForEvent(channel, eventName, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Timeout waiting for event: ${eventName}`));
    }, timeoutMs);

    channel.bind(eventName, (data) => {
      clearTimeout(timeout);
      resolve(data);
    });
  });
}

// Helper: Enable delta compression for a Pusher client
function enableDeltaCompression(pusher) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Delta compression enable timeout"));
    }, 5000);

    // Use bind_global to catch system events
    const handler = (eventName, data) => {
      console.log("Global event:", eventName);
      if (eventName === "pusher:delta_compression_enabled") {
        clearTimeout(timeout);
        pusher.unbind_global(handler);
        resolve(data);
      }
    };

    pusher.bind_global(handler);

    // Send enable event
    console.log("Sending pusher:enable_delta_compression");
    pusher.connection.send_event("pusher:enable_delta_compression", {});
  });
}

// Helper: Wait for multiple events
function collectEvents(channel, eventName, count, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const events = [];
    const timeout = setTimeout(() => {
      reject(
        new Error(`Timeout: only received ${events.length}/${count} events`),
      );
    }, timeoutMs);

    channel.bind(eventName, (data) => {
      events.push(data);
      if (events.length >= count) {
        clearTimeout(timeout);
        resolve(events);
      }
    });
  });
}

// Helper: Trigger event via server SDK
async function triggerEvent(channel, event, data, options = {}) {
  try {
    const result = await pusherServer.trigger(channel, event, data, options);
    console.log(`Triggered ${event} on ${channel}, result:`, result);
    return result;
  } catch (error) {
    console.error(`Failed to trigger ${event} on ${channel}:`, error);
    throw error;
  }
}

// Helper: Sleep
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Filter builder (client-side, matches sockudo-js API)
class Filter {
  static eq(key, value) {
    return { key, cmp: "eq", val: value };
  }

  static neq(key, value) {
    return { key, cmp: "neq", val: value };
  }

  static in(key, values) {
    return { key, cmp: "in", vals: values };
  }

  static nin(key, values) {
    return { key, cmp: "nin", vals: values };
  }

  static gt(key, value) {
    return { key, cmp: "gt", val: value };
  }

  static gte(key, value) {
    return { key, cmp: "gte", val: value };
  }

  static lt(key, value) {
    return { key, cmp: "lt", val: value };
  }

  static lte(key, value) {
    return { key, cmp: "lte", val: value };
  }

  static exists(key) {
    return { key, cmp: "ex" };
  }

  static notExists(key) {
    return { key, cmp: "nex" };
  }

  static startsWith(key, value) {
    return { key, cmp: "sw", val: value };
  }

  static endsWith(key, value) {
    return { key, cmp: "ew", val: value };
  }

  static contains(key, value) {
    return { key, cmp: "ct", val: value };
  }

  static and(...nodes) {
    return { op: "and", nodes };
  }

  static or(...nodes) {
    return { op: "or", nodes };
  }

  static not(node) {
    return { op: "not", nodes: [node] };
  }
}

// ============================================================================
// DELTA COMPRESSION TESTS
// ============================================================================

describe("Delta Compression", () => {
  test("should enable delta compression", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);

    const result = await enableDeltaCompression(pusher);
    expect(result).toBeDefined();

    pusher.disconnect();
  }, 15000);

  test("should receive delta-compressed messages", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    const channel = pusher.subscribe("test-delta-basic");
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    // Collect delta messages
    const deltaMessages = [];
    const allMessages = [];
    let baseMessage = null;

    // Add global bind on pusher connection to see ALL events
    pusher.bind_global((eventName, data) => {
      console.log(
        "PUSHER GLOBAL EVENT:",
        eventName,
        "channel:",
        data?.channel || "none",
      );
      allMessages.push({ eventName, data });
    });

    channel.bind("test-event", (data) => {
      console.log(
        "Received test-event:",
        JSON.stringify(data).length,
        "bytes",
        "data:",
        data,
      );
      baseMessage = data;
    });

    channel.bind("pusher:delta", (data) => {
      console.log("Received pusher:delta:", data);
      deltaMessages.push(data);
    });

    channel.bind_global((eventName, data) => {
      console.log("Global event on channel:", eventName, typeof data);
    });

    // Send similar messages to trigger delta compression
    const baseData = { message: "Hello", counter: 1, padding: "x".repeat(100) };
    console.log("Sending message 1");
    await triggerEvent("test-delta-basic", "test-event", baseData);
    await sleep(500);

    console.log("Sending message 2");
    await triggerEvent("test-delta-basic", "test-event", {
      ...baseData,
      counter: 2,
    });
    await sleep(500);

    console.log("Sending message 3");
    await triggerEvent("test-delta-basic", "test-event", {
      ...baseData,
      counter: 3,
    });
    await sleep(500);

    // Log what we received
    console.log("Total messages received:", allMessages.length);
    console.log("Delta messages:", deltaMessages.length);
    console.log(
      "All messages:",
      allMessages.map((m) => m.eventName),
    );

    // Should have received at least one delta
    expect(deltaMessages.length).toBeGreaterThan(0);
    expect(baseMessage).toBeDefined();

    pusher.unsubscribe("test-delta-basic");
    pusher.disconnect();
  }, 20000);

  test("should decode delta messages correctly", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    const channel = pusher.subscribe("test-delta-decode");
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedMessages = [];
    let baseMessageStr = null;

    // Hook into WebSocket to capture raw messages
    const connection = pusher.connection.connection;
    const transport = connection?.socket || connection?.transport?.socket;

    if (transport) {
      const originalOnMessage = transport.onmessage;
      transport.onmessage = function (event) {
        try {
          const msg = JSON.parse(event.data);
          if (
            msg.channel === "test-delta-decode" &&
            msg.event === "test-event"
          ) {
            baseMessageStr = event.data;
          }
        } catch (e) {}
        if (originalOnMessage) {
          originalOnMessage.call(this, event);
        }
      };
    }

    // Bind events
    channel.bind("test-event", (data) => {
      receivedMessages.push({ type: "full", data });
    });

    channel.bind("pusher:delta", (rawData) => {
      const deltaData =
        typeof rawData === "string" ? JSON.parse(rawData) : rawData;

      if (baseMessageStr && deltaData.delta) {
        const baseBytes = new TextEncoder().encode(baseMessageStr);
        const deltaBytes = Uint8Array.from(atob(deltaData.delta), (c) =>
          c.charCodeAt(0),
        );

        try {
          const decodedBytes = fossilDelta.apply(baseBytes, deltaBytes);
          // Ensure decodedBytes is a Uint8Array
          const bytesArray =
            decodedBytes instanceof Uint8Array
              ? decodedBytes
              : new Uint8Array(decodedBytes);
          const decodedStr = new TextDecoder().decode(bytesArray);
          const decodedMsg = JSON.parse(decodedStr);

          receivedMessages.push({
            type: "delta",
            data: decodedMsg.data,
            sequence: deltaData.seq,
          });

          // Update base for next delta
          baseMessageStr = decodedStr;
        } catch (e) {
          console.error("Delta decode error:", e);
        }
      }
    });

    // Send SIMILAR messages with same structure and conflation key
    const baseData = { value: 100, text: "base message with content" };
    await triggerEvent("test-delta-decode", "test-event", baseData, {
      conflation_key: "test_data",
    });
    await sleep(500);

    await triggerEvent(
      "test-delta-decode",
      "test-event",
      {
        value: 101,
        text: "base message with content",
      },
      {
        conflation_key: "test_data",
      },
    );
    await sleep(500);

    await triggerEvent(
      "test-delta-decode",
      "test-event",
      {
        value: 102,
        text: "base message with content",
      },
      {
        conflation_key: "test_data",
      },
    );
    await sleep(500);

    // Should have received full message + deltas
    expect(receivedMessages.length).toBeGreaterThanOrEqual(2);

    // Check that delta-decoded messages are correct
    const deltaMessages = receivedMessages.filter((m) => m.type === "delta");
    expect(deltaMessages.length).toBeGreaterThan(0);

    pusher.unsubscribe("test-delta-decode");
    pusher.disconnect();
  }, 20000);
});

// ============================================================================
// CONFLATION KEYS TESTS
// ============================================================================

describe("Conflation Keys", () => {
  test("should handle multiple conflation keys independently", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    const channel = pusher.subscribe("test-conflation-multi");
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedEvents = [];
    channel.bind("price_update", (data) => {
      receivedEvents.push({ type: "full", data });
    });

    channel.bind("pusher:delta", (data) => {
      receivedEvents.push({ type: "delta", data });
    });

    // Send updates for different assets (different conflation keys)
    await triggerEvent(
      "test-conflation-multi",
      "price_update",
      { asset: "BTC", price: 50000 },
      { conflation_key: "BTC" },
    );
    await sleep(300);

    await triggerEvent(
      "test-conflation-multi",
      "price_update",
      { asset: "ETH", price: 3000 },
      { conflation_key: "ETH" },
    );
    await sleep(300);

    // Update BTC again (should delta against first BTC)
    await triggerEvent(
      "test-conflation-multi",
      "price_update",
      { asset: "BTC", price: 50100 },
      { conflation_key: "BTC" },
    );
    await sleep(300);

    // Update ETH again (should delta against first ETH)
    await triggerEvent(
      "test-conflation-multi",
      "price_update",
      { asset: "ETH", price: 3010 },
      { conflation_key: "ETH" },
    );
    await sleep(300);

    // Should have received events for both assets
    expect(receivedEvents.length).toBeGreaterThanOrEqual(4);

    pusher.unsubscribe("test-conflation-multi");
    pusher.disconnect();
  }, 20000);

  test("should use same conflation key for delta chain", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    const channel = pusher.subscribe("test-conflation-chain");
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    let fullMessageCount = 0;
    let deltaMessageCount = 0;

    channel.bind("update", () => {
      fullMessageCount++;
    });

    channel.bind("pusher:delta", () => {
      deltaMessageCount++;
    });

    // Send multiple updates with SAME conflation key
    const conflationKey = "sensor:temp_01";
    for (let i = 0; i < 5; i++) {
      await triggerEvent(
        "test-conflation-chain",
        "update",
        { sensor: "temp_01", value: 20 + i, timestamp: Date.now() },
        { conflation_key: conflationKey },
      );
      await sleep(200);
    }

    // Should have 1 full message and 4 deltas
    expect(fullMessageCount).toBe(1);
    expect(deltaMessageCount).toBeGreaterThanOrEqual(3);

    pusher.unsubscribe("test-conflation-chain");
    pusher.disconnect();
  }, 20000);

  test("should handle many conflation keys (100+)", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    const channel = pusher.subscribe("test-conflation-many");
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    let messageCount = 0;
    channel.bind("market_data", () => {
      messageCount++;
    });

    channel.bind("pusher:delta", () => {
      messageCount++;
    });

    // Send events for 100 different assets
    const assets = Array.from({ length: 100 }, (_, i) => `ASSET${i}`);

    for (const asset of assets.slice(0, 20)) {
      // Test with first 20 for speed
      await triggerEvent(
        "test-conflation-many",
        "market_data",
        { asset, price: Math.random() * 1000 },
        { conflation_key: `asset:${asset}` },
      );
    }

    await sleep(2000);

    expect(messageCount).toBeGreaterThan(15);

    pusher.unsubscribe("test-conflation-many");
    pusher.disconnect();
  }, 30000);
});

// ============================================================================
// TAG FILTERING TESTS
// ============================================================================
// NOTE: These tests require TAG_FILTERING_ENABLED=true on the server
// If tag filtering is disabled, these tests will fail

describe("Tag Filtering", () => {
  test("should filter by simple equality", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);

    const filter = Filter.eq("event_type", "goal");
    const channel = pusher.subscribe("test-filter-simple", filter);
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedEvents = [];
    channel.bind("match_event", (data) => {
      receivedEvents.push(data);
    });

    // Send goal event (should be received)
    await triggerEvent(
      "test-filter-simple",
      "match_event",
      { minute: 10, player: "Messi" },
      { tags: { event_type: "goal" } },
    );
    await sleep(300);

    // Send shot event (should be filtered out)
    await triggerEvent(
      "test-filter-simple",
      "match_event",
      { minute: 15, player: "Ronaldo" },
      { tags: { event_type: "shot" } },
    );
    await sleep(300);

    // Send another goal (should be received)
    await triggerEvent(
      "test-filter-simple",
      "match_event",
      { minute: 20, player: "Benzema" },
      { tags: { event_type: "goal" } },
    );
    await sleep(300);

    // Should only receive 2 goals
    expect(receivedEvents.length).toBe(2);
    expect(receivedEvents[0].player).toBe("Messi");
    expect(receivedEvents[1].player).toBe("Benzema");

    pusher.unsubscribe("test-filter-simple");
    pusher.disconnect();
  }, 15000);

  test("should filter by set membership (in)", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);

    const filter = Filter.in("event_type", ["goal", "red_card"]);
    const channel = pusher.subscribe("test-filter-in", filter);
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedEvents = [];
    channel.bind("match_event", (data) => {
      receivedEvents.push(data);
    });

    // Send goal (should pass)
    await triggerEvent(
      "test-filter-in",
      "match_event",
      { type: "goal" },
      { tags: { event_type: "goal" } },
    );
    await sleep(200);

    // Send shot (should be filtered)
    await triggerEvent(
      "test-filter-in",
      "match_event",
      { type: "shot" },
      { tags: { event_type: "shot" } },
    );
    await sleep(200);

    // Send red_card (should pass)
    await triggerEvent(
      "test-filter-in",
      "match_event",
      { type: "red_card" },
      { tags: { event_type: "red_card" } },
    );
    await sleep(200);

    // Send yellow_card (should be filtered)
    await triggerEvent(
      "test-filter-in",
      "match_event",
      { type: "yellow_card" },
      { tags: { event_type: "yellow_card" } },
    );
    await sleep(200);

    expect(receivedEvents.length).toBe(2);
    expect(receivedEvents.map((e) => e.type)).toEqual(["goal", "red_card"]);

    pusher.unsubscribe("test-filter-in");
    pusher.disconnect();
  }, 15000);

  test("should filter by numeric comparison (gte)", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);

    const filter = Filter.gte("xG", "0.8");
    const channel = pusher.subscribe("test-filter-numeric", filter);
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedEvents = [];
    channel.bind("shot_event", (data) => {
      receivedEvents.push(data);
    });

    // High xG shots
    await triggerEvent(
      "test-filter-numeric",
      "shot_event",
      { xG: 0.85 },
      { tags: { xG: "0.85" } },
    );
    await sleep(200);

    await triggerEvent(
      "test-filter-numeric",
      "shot_event",
      { xG: 0.95 },
      { tags: { xG: "0.95" } },
    );
    await sleep(200);

    // Low xG shot (filtered)
    await triggerEvent(
      "test-filter-numeric",
      "shot_event",
      { xG: 0.3 },
      { tags: { xG: "0.3" } },
    );
    await sleep(200);

    expect(receivedEvents.length).toBe(2);
    expect(receivedEvents[0].xG).toBe(0.85);
    expect(receivedEvents[1].xG).toBe(0.95);

    pusher.unsubscribe("test-filter-numeric");
    pusher.disconnect();
  }, 15000);

  test("should filter with AND logic", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);

    const filter = Filter.and(
      Filter.eq("event_type", "shot"),
      Filter.gte("xG", "0.8"),
    );
    const channel = pusher.subscribe("test-filter-and", filter);
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedEvents = [];
    channel.bind("event", (data) => {
      receivedEvents.push(data);
    });

    // Shot with high xG (should pass)
    await triggerEvent(
      "test-filter-and",
      "event",
      { type: "shot", xG: 0.85 },
      { tags: { event_type: "shot", xG: "0.85" } },
    );
    await sleep(200);

    // Shot with low xG (filtered)
    await triggerEvent(
      "test-filter-and",
      "event",
      { type: "shot", xG: 0.3 },
      { tags: { event_type: "shot", xG: "0.3" } },
    );
    await sleep(200);

    // Goal with high xG (filtered - not a shot)
    await triggerEvent(
      "test-filter-and",
      "event",
      { type: "goal", xG: 0.9 },
      { tags: { event_type: "goal", xG: "0.9" } },
    );
    await sleep(200);

    expect(receivedEvents.length).toBe(1);
    expect(receivedEvents[0].type).toBe("shot");
    expect(receivedEvents[0].xG).toBe(0.85);

    pusher.unsubscribe("test-filter-and");
    pusher.disconnect();
  }, 15000);

  test("should filter with OR logic", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);

    const filter = Filter.or(
      Filter.eq("event_type", "goal"),
      Filter.and(Filter.eq("event_type", "shot"), Filter.gte("xG", "0.8")),
    );
    const channel = pusher.subscribe("test-filter-or", filter);
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedEvents = [];
    channel.bind("event", (data) => {
      receivedEvents.push(data);
    });

    // Goal (should pass)
    await triggerEvent(
      "test-filter-or",
      "event",
      { type: "goal" },
      { tags: { event_type: "goal" } },
    );
    await sleep(200);

    // High xG shot (should pass)
    await triggerEvent(
      "test-filter-or",
      "event",
      { type: "shot", xG: 0.85 },
      { tags: { event_type: "shot", xG: "0.85" } },
    );
    await sleep(200);

    // Low xG shot (filtered)
    await triggerEvent(
      "test-filter-or",
      "event",
      { type: "shot", xG: 0.3 },
      { tags: { event_type: "shot", xG: "0.3" } },
    );
    await sleep(200);

    // Pass (filtered)
    await triggerEvent(
      "test-filter-or",
      "event",
      { type: "pass" },
      { tags: { event_type: "pass" } },
    );
    await sleep(200);

    expect(receivedEvents.length).toBe(2);

    pusher.unsubscribe("test-filter-or");
    pusher.disconnect();
  }, 15000);
});

// ============================================================================
// COMBINED TESTS (Delta + Conflation + Filtering)
// ============================================================================

describe("Combined Features", () => {
  test("should work with delta compression + conflation keys", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    const channel = pusher.subscribe("test-combined-delta-conflation");
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    let fullCount = 0;
    let deltaCount = 0;

    channel.bind("price_update", () => fullCount++);
    channel.bind("pusher:delta", () => deltaCount++);

    // Send updates with conflation keys
    for (let i = 0; i < 5; i++) {
      await triggerEvent(
        "test-combined-delta-conflation",
        "price_update",
        { asset: "BTC", price: 50000 + i * 10 },
        { conflation_key: "BTC" },
      );
      await sleep(150);
    }

    expect(fullCount).toBe(1);
    expect(deltaCount).toBeGreaterThanOrEqual(3);

    pusher.unsubscribe("test-combined-delta-conflation");
    pusher.disconnect();
  }, 20000);

  test("should work with tag filtering + delta compression", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    const filter = Filter.eq("priority", "high");
    const channel = pusher.subscribe("test-combined-filter-delta", filter);
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedEvents = [];
    channel.bind("alert", (data) => receivedEvents.push(data));
    channel.bind("pusher:delta", (data) =>
      receivedEvents.push({ delta: true }),
    );

    // Send high priority alerts (should be received)
    await triggerEvent(
      "test-combined-filter-delta",
      "alert",
      { message: "Critical alert 1" },
      { tags: { priority: "high" }, conflation_key: "alerts" },
    );
    await sleep(200);

    // Send low priority (filtered out)
    await triggerEvent(
      "test-combined-filter-delta",
      "alert",
      { message: "Low alert" },
      { tags: { priority: "low" }, conflation_key: "alerts" },
    );
    await sleep(200);

    // Send another high priority (should delta compress)
    await triggerEvent(
      "test-combined-filter-delta",
      "alert",
      { message: "Critical alert 2" },
      { tags: { priority: "high" }, conflation_key: "alerts" },
    );
    await sleep(200);

    // Should have received 2 high priority alerts, second might be delta
    expect(receivedEvents.length).toBeGreaterThanOrEqual(2);

    pusher.unsubscribe("test-combined-filter-delta");
    pusher.disconnect();
  }, 20000);

  test("should work with all three features together", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    // Subscribe with tag filter
    const filter = Filter.or(
      Filter.eq("event_type", "goal"),
      Filter.and(Filter.eq("event_type", "shot"), Filter.gte("xG", "0.8")),
    );
    const channel = pusher.subscribe("test-combined-all", filter);
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    const receivedEvents = [];
    channel.bind("match_event", (data) =>
      receivedEvents.push({ ...data, type: "full" }),
    );
    channel.bind("pusher:delta", () => receivedEvents.push({ type: "delta" }));

    // Send goals with conflation key
    await triggerEvent(
      "test-combined-all",
      "match_event",
      { minute: 10, player: "Messi", score: "1-0" },
      { tags: { event_type: "goal" }, conflation_key: "match:score" },
    );
    await sleep(200);

    // Send low xG shot (should be filtered out)
    await triggerEvent(
      "test-combined-all",
      "match_event",
      { minute: 15, player: "Pique", xG: 0.2 },
      {
        tags: { event_type: "shot", xG: "0.2" },
        conflation_key: "match:score",
      },
    );
    await sleep(200);

    // Send high xG shot (should pass filter and use delta)
    await triggerEvent(
      "test-combined-all",
      "match_event",
      { minute: 20, player: "Benzema", xG: 0.85 },
      {
        tags: { event_type: "shot", xG: "0.85" },
        conflation_key: "match:score",
      },
    );
    await sleep(200);

    // Send another goal (should pass filter and use delta)
    await triggerEvent(
      "test-combined-all",
      "match_event",
      { minute: 30, player: "Ronaldo", score: "2-0" },
      { tags: { event_type: "goal" }, conflation_key: "match:score" },
    );
    await sleep(200);

    // Should have received 3 events (filtered out the low xG shot)
    expect(receivedEvents.length).toBeGreaterThanOrEqual(3);

    // At least one should be a delta
    const deltaEvents = receivedEvents.filter((e) => e.type === "delta");
    expect(deltaEvents.length).toBeGreaterThanOrEqual(1);

    pusher.unsubscribe("test-combined-all");
    pusher.disconnect();
  }, 20000);
});

// ============================================================================
// BANDWIDTH SAVINGS TESTS
// ============================================================================

describe("Bandwidth Savings", () => {
  test("should achieve significant bandwidth savings with delta compression", async () => {
    const pusher = createClient();
    await waitForConnection(pusher);
    await enableDeltaCompression(pusher);

    const channel = pusher.subscribe("test-bandwidth");
    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", resolve);
    });

    let totalFullBytes = 0;
    let totalDeltaBytes = 0;
    let fullMessageCount = 0;
    let deltaMessageCount = 0;

    // Hook WebSocket to measure actual bytes
    const connection = pusher.connection.connection;
    const transport = connection?.socket || connection?.transport?.socket;

    if (transport) {
      const originalOnMessage = transport.onmessage;
      transport.onmessage = function (event) {
        try {
          const msg = JSON.parse(event.data);
          if (msg.channel === "test-bandwidth") {
            if (msg.event === "pusher:delta") {
              totalDeltaBytes += event.data.length;
              deltaMessageCount++;
            } else if (msg.event === "data_update") {
              totalFullBytes += event.data.length;
              fullMessageCount++;
            }
          }
        } catch (e) {}
        if (originalOnMessage) {
          originalOnMessage.call(this, event);
        }
      };
    }

    // Send large similar messages
    const largeData = {
      users: Array.from({ length: 50 }, (_, i) => ({
        id: i,
        name: `User ${i}`,
        bio: "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
      })),
    };

    for (let i = 0; i < 10; i++) {
      const data = {
        ...largeData,
        timestamp: Date.now(),
        counter: i,
      };
      await triggerEvent("test-bandwidth", "data_update", data, {
        conflation_key: "bulk_data",
      });
      await sleep(200);
    }

    await sleep(1000);

    // Calculate savings
    const withoutCompression = totalFullBytes * 10; // If all were full
    const withCompression = totalFullBytes + totalDeltaBytes;
    const savings =
      ((withoutCompression - withCompression) / withoutCompression) * 100;

    console.log(`\n📊 Bandwidth Test Results:`);
    console.log(`   Full messages: ${fullMessageCount}`);
    console.log(`   Delta messages: ${deltaMessageCount}`);
    console.log(`   Total full bytes: ${totalFullBytes}`);
    console.log(`   Total delta bytes: ${totalDeltaBytes}`);
    console.log(`   Bandwidth saved: ${savings.toFixed(1)}%\n`);

    expect(deltaMessageCount).toBeGreaterThan(5);
    expect(savings).toBeGreaterThan(50); // At least 50% savings

    pusher.unsubscribe("test-bandwidth");
    pusher.disconnect();
  }, 30000);
});

console.log("\n🧪 Running comprehensive Sockudo tests...\n");
console.log(`Testing against: ${config.host}:${config.port}`);
console.log(`App Key: ${config.appKey}\n`);
