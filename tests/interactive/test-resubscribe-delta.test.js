/**
 * Test for delta compression on resubscribe
 *
 * This test verifies that when a client unsubscribes and then resubscribes
 * to the same channel on the same connection, delta compression works correctly
 * without checksum errors.
 *
 * Prerequisites: Run the sender benchmark to generate messages:
 *   cd benchmarks/sender && cargo run --release
 *
 * Bug: Previously, the client kept stale delta state after unsubscribe,
 * causing delta decode failures when resubscribing.
 */

const Pusher = require("./sockudo-js");

// Test configuration
const APP_KEY = "app-key";
const CHANNEL = "public-test";
const PUSHER_PORT = 6001;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createClient() {
  return new Pusher(APP_KEY, {
    wsHost: "localhost",
    wsPort: PUSHER_PORT,
    forceTLS: false,
    disableStats: true,
    enabledTransports: ["ws"],
    cluster: "",
  });
}

describe("Delta Compression Resubscribe Tests", () => {
  test("should handle resubscribe without delta decode errors", async () => {
    const pusher = createClient();
    let receivedMessages = [];
    let deltaMessages = 0;
    let fullMessages = 0;
    let deltaErrors = [];

    console.log("\n🔌 Connecting to server...");

    // Wait for connection
    await new Promise((resolve) => {
      pusher.connection.bind("connected", () => {
        console.log("✅ Connected");
        resolve();
      });
    });

    // Enable delta compression
    console.log("📊 Enabling delta compression...");
    pusher.connection.send_event("pusher:enable_delta_compression", {});
    await sleep(200);

    // Listen for delta errors globally
    pusher.connection.bind("message", (msg) => {
      if (msg.event === "pusher:error" && msg.data && msg.data.message) {
        const errMsg = msg.data.message.toLowerCase();
        if (errMsg.includes("delta") || errMsg.includes("checksum")) {
          console.error("❌ DELTA ERROR DETECTED:", msg.data.message);
          deltaErrors.push(msg.data);
        }
      }
    });

    console.log("\n=== PHASE 1: Initial subscription ===");

    // Subscribe to channel
    const channel = pusher.subscribe(CHANNEL);

    await new Promise((resolve) => {
      channel.bind("pusher:subscription_succeeded", () => {
        console.log("✅ Subscribed to", CHANNEL);
        resolve();
      });
    });

    // Bind event handler
    channel.bind_global((event, data) => {
      if (event.startsWith("pusher:") || event.startsWith("pusher_internal:")) {
        return; // Skip internal events
      }

      receivedMessages.push({ event, data });

      // Try to detect message type from metadata
      const msg = JSON.stringify(data);
      if (msg.includes("__delta_seq")) {
        if (msg.includes("__delta_full")) {
          fullMessages++;
        } else {
          deltaMessages++;
        }
      }
    });

    // Wait for messages from sender (should already be running)
    console.log("⏳ Waiting for messages (sender should be running)...");
    await sleep(5000);

    console.log(`📨 Phase 1 results:
  - Total messages: ${receivedMessages.length}
  - Full messages: ${fullMessages}
  - Delta messages: ${deltaMessages}
  - Delta errors: ${deltaErrors.length}`);

    // Verify we received messages
    expect(receivedMessages.length).toBeGreaterThan(0);
    expect(deltaErrors.length).toBe(0);

    // CRITICAL: Unsubscribe from the channel
    console.log("\n⚠️  UNSUBSCRIBING from channel (clearing delta state)...");
    pusher.unsubscribe(CHANNEL);
    await sleep(500);

    // Reset counters
    receivedMessages = [];
    deltaMessages = 0;
    fullMessages = 0;

    // CRITICAL: Resubscribe to the SAME channel on the SAME connection
    console.log("\n=== PHASE 2: Resubscribe (THIS IS THE TEST) ===");
    const channel2 = pusher.subscribe(CHANNEL);

    await new Promise((resolve) => {
      channel2.bind("pusher:subscription_succeeded", () => {
        console.log("✅ Resubscribed to", CHANNEL);
        resolve();
      });
    });

    // Bind event handler
    channel2.bind_global((event, data) => {
      if (event.startsWith("pusher:") || event.startsWith("pusher_internal:")) {
        return;
      }

      receivedMessages.push({ event, data });

      const msg = JSON.stringify(data);
      if (msg.includes("__delta_seq")) {
        if (msg.includes("__delta_full")) {
          fullMessages++;
        } else {
          deltaMessages++;
        }
      }
    });

    // Wait for messages after resubscribe
    console.log("⏳ Waiting for messages after resubscribe...");
    await sleep(5000);

    console.log(`\n📨 Phase 2 results:
  - Total messages: ${receivedMessages.length}
  - Full messages: ${fullMessages}
  - Delta messages: ${deltaMessages}
  - Delta errors: ${deltaErrors.length}`);

    // THE KEY ASSERTION: No delta errors should occur after resubscribe
    if (deltaErrors.length > 0) {
      console.error(
        "\n❌ TEST FAILED: Delta errors detected after resubscribe",
      );
      deltaErrors.forEach((err) => console.error("  -", err));
    } else {
      console.log("\n✅ TEST PASSED: No delta errors after resubscribe!");
    }

    expect(deltaErrors.length).toBe(0);
    expect(receivedMessages.length).toBeGreaterThan(0);

    pusher.disconnect();
    console.log("\n🔌 Disconnected\n");
  }, 20000);
});
