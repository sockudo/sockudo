// Conflation Keys Test Suite
// Tests delta compression with 100+ conflation keys

class ConflationTestSuite {
  constructor() {
    this.pusher = null;
    this.pusher = null;
    this.channel = null;
    this.stats = {
      messagesSent: 0,
      messagesReceived: 0,
      deltaMessages: 0,
      fullMessages: 0,
      uniqueKeys: new Set(),
      totalBytesWithout: 0,
      totalBytesWith: 0,
    };
    this.testResults = {};
    this.consoleLines = [];

    this.init();
  }

  init() {
    // Bind UI controls
    document
      .getElementById("connect-btn")
      .addEventListener("click", () => this.connect());
    document
      .getElementById("disconnect-btn")
      .addEventListener("click", () => this.disconnect());
    document
      .getElementById("run-all-tests")
      .addEventListener("click", () => this.runAllTests());
    document
      .getElementById("run-test1")
      .addEventListener("click", () => this.runTest1());
    document
      .getElementById("run-test2")
      .addEventListener("click", () => this.runTest2());
    document
      .getElementById("run-test3")
      .addEventListener("click", () => this.runTest3());
    document
      .getElementById("run-test4")
      .addEventListener("click", () => this.runTest4());
    document
      .getElementById("clear-console")
      .addEventListener("click", () => this.clearConsole());
    document
      .getElementById("delta-toggle")
      .addEventListener("change", (e) =>
        this.toggleDeltaCompression(e.target.checked),
      );
  }

  log(message, level = "info") {
    const timestamp = new Date().toLocaleTimeString();
    const line = document.createElement("div");
    line.className = `console-line console-${level}`;
    line.innerHTML = `<span class="console-timestamp">[${timestamp}]</span>${message}`;

    const console = document.getElementById("console");
    console.appendChild(line);
    console.scrollTop = console.scrollHeight;

    this.consoleLines.push({ timestamp, message, level });
  }

  clearConsole() {
    document.getElementById("console").innerHTML = "";
    this.consoleLines = [];
  }

  updateStats() {
    document.getElementById("messages-sent").textContent =
      this.stats.messagesSent;
    document.getElementById("messages-received").textContent =
      this.stats.messagesReceived;
    document.getElementById("delta-count").textContent =
      this.stats.deltaMessages;
    document.getElementById("full-count").textContent = this.stats.fullMessages;
    document.getElementById("unique-keys").textContent =
      this.stats.uniqueKeys.size;

    const bandwidthSaved =
      this.stats.totalBytesWithout > 0
        ? (
          (1 - this.stats.totalBytesWith / this.stats.totalBytesWithout) *
          100
        ).toFixed(1)
        : 0;
    document.getElementById("bandwidth-saved").textContent =
      bandwidthSaved + "%";

    const avgCompression =
      this.stats.totalBytesWithout > 0
        ? (
          (this.stats.totalBytesWith / this.stats.totalBytesWithout) *
          100
        ).toFixed(1)
        : 0;
    document.getElementById("avg-compression").textContent =
      avgCompression + "%";
  }

  async connect() {
    this.log("Connecting to Sockudo server...", "info");

    // Load configuration from backend server
    try {
      const configResponse = await fetch("/config");
      if (!configResponse.ok) {
        throw new Error(`Failed to load config: ${configResponse.status}`);
      }
      const config = await configResponse.json();
      this.log(`✅ Loaded config from backend server`, "success");

      this.pusher = new Pusher(config.pusherKey, {
        wsHost: config.pusherHost,
        wsPort: config.pusherPort,
        forceTLS: config.pusherUseTLS,
        enabledTransports: ["ws"],
        disabledTransports: ["sockjs"],
        authEndpoint: config.authEndpoint,
        cluster: config.pusherCluster || "mt1",
        // Enable built-in delta compression
        deltaCompression: {
          enabled: false, // Start disabled, toggle via UI
          algorithms: ["fossil", "xdelta3"],
          debug: true,
          onStats: (stats) => {
            this.stats.deltaMessages = stats.deltaMessages;
            this.stats.fullMessages = stats.fullMessages;
            this.stats.totalBytesWith = stats.totalBytesWithCompression;
            this.stats.totalBytesWithout = stats.totalBytesWithoutCompression;
            // Total messages received = full messages + delta messages
            // We only count the messages that come through price-update event separately
            // So we need to add delta messages to get the true total
            this.updateStats();
          },
        }
      });

      // BIND GLOBAL LOGGING ONLY (Delta stats handled by built-in manager)
      this.pusher.bind_global((eventName, data) => {
        // Debug: Log all events to see what we're receiving
        if (
          eventName !== "pusher:ping" &&
          eventName !== "pusher:pong" &&
          !eventName.startsWith("pusher_internal") &&
          eventName !== "pusher:delta" // Delta events are handled internally
        ) {
          console.log(
            "🔔 Global event:",
            eventName,
            "| Data keys:",
            typeof data === "object" && data !== null
              ? Object.keys(data).slice(0, 5)
              : typeof data,
          );
        }
      });
    } catch (error) {
      this.log(`❌ Configuration error: ${error.message}`, "error");
      this.log(
        `Make sure to run 'npm start' in test/interactive directory`,
        "error",
      );
      return;
    }

    this.pusher.connection.bind("connected", () => {
      this.log("✅ Connected to Sockudo server", "success");
      document.getElementById("connection-status").textContent = "Connected";
      document.getElementById("connection-status").className =
        "status-value connected";
      document.getElementById("connect-btn").disabled = true;
      document.getElementById("disconnect-btn").disabled = false;
      document.getElementById("run-all-tests").disabled = false;
      document.getElementById("run-test1").disabled = false;
      document.getElementById("run-test2").disabled = false;
      document.getElementById("run-test3").disabled = false;
      document.getElementById("run-test4").disabled = false;
      document.getElementById("delta-toggle").disabled = false;
    });

    this.pusher.connection.bind("disconnected", () => {
      this.log("⚠️ Disconnected from server", "warning");
      document.getElementById("connection-status").textContent = "Disconnected";
      document.getElementById("connection-status").className =
        "status-value disconnected";
    });

    this.pusher.connection.bind("error", (err) => {
      this.log(`❌ Connection error: ${err.message}`, "error");
    });

    // Subscribe to test channel with conflation keys
    this.channel = this.pusher.subscribe("market-data");

    this.channel.bind("pusher:subscription_succeeded", () => {
      this.log("✅ Subscribed to market-data channel", "success");
    });

    this.channel.bind("pusher:delta_cache_sync", (data) => {
      this.log("🔄 Received delta cache sync", "info");

      // Parse data if it's a string
      let parsedData = data;
      if (typeof data === "string") {
        try {
          parsedData = JSON.parse(data);
        } catch (e) {
          console.error("Failed to parse cache sync data:", e);
        }
      }

      console.log("Cache sync data:", parsedData);
    });

    this.channel.bind("price-update", (data) => {
      this.stats.messagesReceived++;
      console.log(
        "📨 price-update event received, total count:",
        this.stats.messagesReceived,
      );

      if (data.asset) {
        this.stats.uniqueKeys.add(data.asset);
      }

      // Debug: Check if this is a full message
      if (data && typeof data === "object" && "__delta_seq" in data) {
        console.log("🟢 FULL MESSAGE DETECTED:", {
          seq: data.__delta_seq,
          full: data.__delta_full,
          conflation_key: data.__conflation_key,
          asset: data.asset,
          allKeys: Object.keys(data),
        });
      }

      this.updateStats();
    });
  }

  disconnect() {
    if (this.pusher) {
      this.pusher.disconnect();
      this.pusher = null;
      this.log("Disconnected from server", "info");
      document.getElementById("connect-btn").disabled = false;
      document.getElementById("disconnect-btn").disabled = true;
      document.getElementById("run-all-tests").disabled = true;
      document.getElementById("run-test1").disabled = true;
      document.getElementById("run-test2").disabled = true;
      document.getElementById("run-test3").disabled = true;
      document.getElementById("run-test4").disabled = true;
      document.getElementById("delta-toggle").disabled = true;
      document.getElementById("delta-toggle").checked = false;
      this.updateToggleUI(false);
    }
  }

  toggleDeltaCompression(enabled) {
    if (!this.pusher || this.pusher.connection.state !== "connected") {
      this.log("❌ Cannot toggle delta compression: Not connected", "error");
      document.getElementById("delta-toggle").checked = false;
      this.updateToggleUI(false);
      return;
    }

    if (enabled) {
      this.log("🗜️ Enabling delta compression...", "info");
      this.pusher.deltaCompression.enable();
      this.updateToggleUI(true);
    } else {
      this.log("⚠️ Disabling delta compression...", "warning");
      this.pusher.deltaCompression.disable();
      this.updateToggleUI(false);
    }
  }

  updateToggleUI(enabled) {
    const slider = document.getElementById("delta-toggle-slider");
    const knob = document.getElementById("delta-toggle-knob");

    if (enabled) {
      slider.style.backgroundColor = "#667eea";
      knob.style.transform = "translateX(26px)";
    } else {
      slider.style.backgroundColor = "#ccc";
      knob.style.transform = "translateX(0px)";
    }
  }

  async runAllTests() {
    this.log("🚀 Running all tests...", "info");
    await this.runTest1();
    await this.sleep(2000);
    await this.runTest2();
    await this.sleep(2000);
    await this.runTest3();
    await this.sleep(2000);
    await this.runTest4();
    this.log("✅ All tests completed!", "success");
  }

  async runTest1() {
    this.log(
      "🧪 Test 1: Sending messages for 100 different conflation keys...",
      "info",
    );
    this.setTestStatus("test1", "running");
    this.clearTestResults("test1");

    const assets = this.generateAssetList(100);
    const startTime = Date.now();

    try {
      for (let i = 0; i < assets.length; i++) {
        const asset = assets[i];
        await this.sendPriceUpdate(asset, 100 + i, 1000 + i * 10);
        this.addTestResult(
          "test1",
          `Sent: ${asset} - Price: ${100 + i}`,
          "info",
        );
        await this.sleep(50); // Small delay between messages
      }

      const duration = Date.now() - startTime;
      this.log(`✅ Test 1 completed in ${duration}ms`, "success");
      this.addTestResult(
        "test1",
        `✅ Sent ${assets.length} messages`,
        "success",
      );
      this.addTestResult("test1", `Duration: ${duration}ms`, "info");
      this.setTestStatus("test1", "passed");
    } catch (error) {
      this.log(`❌ Test 1 failed: ${error.message}`, "error");
      this.addTestResult("test1", `❌ Error: ${error.message}`, "error");
      this.setTestStatus("test1", "failed");
    }
  }

  async runTest2() {
    this.log(
      "🧪 Test 2: Sending interleaved messages for multiple assets...",
      "info",
    );
    this.setTestStatus("test2", "running");
    this.clearTestResults("test2");

    const assets = ["BTC", "ETH", "ADA", "DOT", "SOL"];
    const startTime = Date.now();

    try {
      // Send 10 rounds of updates for each asset (interleaved)
      for (let round = 0; round < 10; round++) {
        for (const asset of assets) {
          const price = 100 + round + Math.random() * 10;
          const volume = 1000 + round * 100;
          await this.sendPriceUpdate(asset, price, volume);
          this.addTestResult(
            "test2",
            `Round ${round + 1}: ${asset} = $${price.toFixed(2)}`,
            "info",
          );
          await this.sleep(30);
        }
      }

      const duration = Date.now() - startTime;
      this.log(`✅ Test 2 completed in ${duration}ms`, "success");
      this.addTestResult(
        "test2",
        `✅ Sent ${assets.length * 10} interleaved messages`,
        "success",
      );
      this.addTestResult("test2", `Duration: ${duration}ms`, "info");
      this.setTestStatus("test2", "passed");
    } catch (error) {
      this.log(`❌ Test 2 failed: ${error.message}`, "error");
      this.addTestResult("test2", `❌ Error: ${error.message}`, "error");
      this.setTestStatus("test2", "failed");
    }
  }

  async runTest3() {
    this.log("🧪 Test 3: Testing cache sync on subscription...", "info");
    this.setTestStatus("test3", "running");
    this.clearTestResults("test3");

    try {
      // Make sure delta compression is enabled
      if (!document.getElementById("delta-toggle").checked) {
        this.addTestResult(
          "test3",
          "⚠️ Delta compression not enabled, enabling now...",
          "info",
        );
        document.getElementById("delta-toggle").checked = true;
        this.toggleDeltaCompression(true);
        await this.sleep(500);
      }

      // First, send some messages with conflation keys
      this.addTestResult(
        "test3",
        "Sending initial messages with conflation keys...",
        "info",
      );
      await this.sendPriceUpdate("BTC", 50000, 1000);
      await this.sleep(200);
      await this.sendPriceUpdate("BTC", 50100, 1100);
      await this.sleep(200);
      await this.sendPriceUpdate("ETH", 3000, 500);
      await this.sleep(200);
      await this.sendPriceUpdate("ETH", 3050, 550);
      await this.sleep(500);

      this.addTestResult(
        "test3",
        "Initial messages sent, waiting for processing...",
        "info",
      );
      await this.sleep(1000);

      // Unsubscribe and resubscribe to test cache sync
      this.addTestResult("test3", "Unsubscribing from channel...", "info");
      this.pusher.unsubscribe("market-data");
      await this.sleep(1000);

      this.addTestResult("test3", "Resubscribing to channel...", "info");

      // Listen for cache sync event
      const cacheSyncReceived = new Promise((resolve) => {
        const newChannel = this.pusher.subscribe("market-data");

        // IMPORTANT: Rebind the price-update handler to the new channel
        newChannel.bind("price-update", (data) => {
          this.stats.messagesReceived++;
          console.log(
            "📨 price-update event received, total count:",
            this.stats.messagesReceived,
          );

          if (data.asset) {
            this.stats.uniqueKeys.add(data.asset);
          }

          // Debug: Check if this is a full message
          if (data && typeof data === "object" && "__delta_seq" in data) {
            console.log("🟢 FULL MESSAGE DETECTED:", {
              seq: data.__delta_seq,
              full: data.__delta_full,
              conflation_key: data.__conflation_key,
              asset: data.asset,
              allKeys: Object.keys(data),
            });
          }

          this.updateStats();
        });

        newChannel.bind("pusher:delta_cache_sync", (data) => {
          this.log("✅ Received cache sync!", "success");

          // Parse data if it's a string
          let parsedData = data;
          if (typeof data === "string") {
            try {
              parsedData = JSON.parse(data);
            } catch (e) {
              console.error("Failed to parse cache sync data:", e);
            }
          }

          this.addTestResult("test3", `✅ Cache sync received!`, "success");
          this.addTestResult(
            "test3",
            `Conflation key: ${parsedData.conflation_key || "none"}`,
            "info",
          );
          this.addTestResult(
            "test3",
            `Max messages per key: ${parsedData.max_messages_per_key || 0}`,
            "info",
          );

          if (parsedData.states) {
            const stateKeys = Object.keys(parsedData.states);
            this.addTestResult(
              "test3",
              `States for ${stateKeys.length} keys: ${stateKeys.join(", ")}`,
              "info",
            );
          }

          this.channel = newChannel;
          resolve(true);
        });

        newChannel.bind("pusher:subscription_succeeded", () => {
          this.addTestResult(
            "test3",
            "✅ Subscription succeeded, waiting for cache sync...",
            "info",
          );
        });

        // Timeout after 5 seconds
        setTimeout(() => {
          this.channel = newChannel;
          resolve(false);
        }, 5000);
      });

      const received = await cacheSyncReceived;

      if (received) {
        this.log("✅ Test 3 completed", "success");
        this.setTestStatus("test3", "passed");
      } else {
        this.log(
          "⚠️ Test 3: Cache sync not received (cache might be empty or feature disabled)",
          "warning",
        );
        this.addTestResult(
          "test3",
          "⚠️ Cache sync not received within 5 seconds",
          "error",
        );
        this.addTestResult(
          "test3",
          "Note: Cache sync requires delta compression enabled and existing cached messages",
          "info",
        );
        this.setTestStatus("test3", "failed");
      }
    } catch (error) {
      this.log(`❌ Test 3 failed: ${error.message}`, "error");
      this.addTestResult("test3", `❌ Error: ${error.message}`, "error");
      this.setTestStatus("test3", "failed");
    }
  }

  async runTest4() {
    this.log(
      "🧪 Test 4: Comparing bandwidth with/without conflation...",
      "info",
    );
    this.setTestStatus("test4", "running");
    this.clearTestResults("test4");

    try {
      const beforeStats = {
        totalBytes: this.stats.totalBytesWith,
        messages: this.stats.messagesReceived,
      };

      // Send test data
      this.addTestResult("test4", "Sending test messages...", "info");
      const assets = ["BTC", "ETH", "ADA"];

      for (let i = 0; i < 20; i++) {
        for (const asset of assets) {
          await this.sendPriceUpdate(asset, 100 + i, 1000);
          await this.sleep(20);
        }
      }

      // Wait for all messages to be received via WebSocket
      // Since we send messages with 20ms delay, and there's network latency,
      // we need to wait longer to ensure all messages arrive
      this.addTestResult(
        "test4",
        "Waiting for all messages to arrive...",
        "info",
      );
      await this.sleep(2000);

      const afterStats = {
        totalBytes: this.stats.totalBytesWith,
        messages: this.stats.messagesReceived,
      };

      const messagesSent = afterStats.messages - beforeStats.messages;
      const bytesUsed = afterStats.totalBytes - beforeStats.totalBytes;
      const avgBytesPerMessage =
        messagesSent > 0 ? (bytesUsed / messagesSent).toFixed(0) : 0;

      this.addTestResult("test4", `✅ Test completed`, "success");
      this.addTestResult("test4", `Messages sent: ${messagesSent}`, "info");
      this.addTestResult("test4", `Bytes used: ${bytesUsed}`, "info");
      this.addTestResult(
        "test4",
        `Avg bytes/message: ${avgBytesPerMessage}`,
        "info",
      );

      this.log(`✅ Test 4 completed`, "success");
      this.setTestStatus("test4", "passed");
    } catch (error) {
      this.log(`❌ Test 4 failed: ${error.message}`, "error");
      this.addTestResult("test4", `❌ Error: ${error.message}`, "error");
      this.setTestStatus("test4", "failed");
    }
  }

  async sendPriceUpdate(asset, price, volume) {
    const message = {
      asset: asset,
      price: price.toFixed(2),
      volume: volume,
      timestamp: new Date().toISOString(),
      market: "SPOT",
      exchange: "TEST",
    };

    try {
      // Send via backend server which uses Pusher SDK with proper authentication
      const response = await fetch("/trigger-event", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          channel: "market-data",
          event: "price-update",
          data: message,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      }

      const result = await response.json();
      if (!result.success) {
        throw new Error(result.error || "Failed to send event");
      }

      this.stats.messagesSent++;
      console.log(
        "📤 Message sent successfully, total sent:",
        this.stats.messagesSent,
        "| Asset:",
        asset,
      );
      this.updateStats();

      return message;
    } catch (error) {
      this.log(`❌ Failed to send message: ${error.message}`, "error");
      throw error;
    }
  }

  generateAssetList(count) {
    const cryptos = [
      "BTC",
      "ETH",
      "ADA",
      "DOT",
      "SOL",
      "AVAX",
      "MATIC",
      "ATOM",
      "LINK",
      "UNI",
      "XRP",
      "LTC",
      "BCH",
      "XLM",
      "DOGE",
      "SHIB",
      "TRX",
      "ETC",
      "FIL",
      "AAVE",
      "ALGO",
      "VET",
      "ICP",
      "THETA",
      "XTZ",
      "EOS",
      "CAKE",
      "RUNE",
      "EGLD",
      "FTM",
      "NEAR",
      "FLOW",
      "MANA",
      "SAND",
      "AXS",
      "CHZ",
      "ENJ",
      "BAT",
      "ZIL",
      "COMP",
      "MKR",
      "SNX",
      "YFI",
      "UMA",
      "CRV",
      "BAL",
      "LRC",
      "SUSHI",
      "1INCH",
      "REN",
      "KNC",
      "ANT",
      "NMR",
      "MLN",
      "STORJ",
      "GRT",
      "OMG",
      "ZRX",
      "REP",
      "BNT",
    ];

    // Generate more if needed
    const result = [...cryptos];
    for (let i = cryptos.length; i < count; i++) {
      result.push(`TOKEN${i + 1}`);
    }

    return result.slice(0, count);
  }

  setTestStatus(testId, status) {
    const statusEl = document.getElementById(`${testId}-status`);
    statusEl.className = `test-status ${status}`;
  }

  clearTestResults(testId) {
    document.getElementById(`${testId}-results`).innerHTML = "";
  }

  addTestResult(testId, message, level = "info") {
    const resultsEl = document.getElementById(`${testId}-results`);
    const line = document.createElement("div");
    line.className = `test-log ${level}`;
    line.textContent = message;
    resultsEl.appendChild(line);
    resultsEl.scrollTop = resultsEl.scrollHeight;
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Initialize test suite when page loads
window.addEventListener("DOMContentLoaded", () => {
  window.testSuite = new ConflationTestSuite();
});
