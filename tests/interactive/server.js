// server.js
require("dotenv").config();
const express = require("express");
const bodyParser = require("body-parser");
const Pusher = require("pusher");
const crypto = require("crypto");

const app = express();
const port = process.env.BACKEND_PORT || 3000;

// Configure Pusher for your WebSocket server
const pusher = new Pusher({
  appId: process.env.PUSHER_APP_ID,
  key: process.env.PUSHER_APP_KEY,
  secret: process.env.PUSHER_APP_SECRET,
  cluster: "mt1",
  host: process.env.PUSHER_HOST,
  port: process.env.PUSHER_PORT,
  useTLS: process.env.PUSHER_USE_TLS === "true",
});

console.log("Pusher Server SDK configured for:", {
  appId: process.env.PUSHER_APP_ID,
  key: process.env.PUSHER_APP_KEY ? "***" : "Not Set",
  secret: process.env.PUSHER_APP_SECRET ? "***" : "Not Set",
  host: process.env.PUSHER_HOST,
  port: process.env.PUSHER_PORT,
  useTLS: process.env.PUSHER_USE_TLS === "true",
});

// Store received webhooks and active channels
let receivedWebhooks = [];
let activeChannels = new Set();
let eventHistory = [];

// Middleware
app.use(express.static("public"));
app.use(
  bodyParser.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    },
  }),
);
app.use(bodyParser.urlencoded({ extended: false }));

// Routes

// Frontend config endpoint
app.get("/config", (req, res) => {
  res.json({
    pusherKey: process.env.PUSHER_APP_KEY || "app-key",
    pusherHost: process.env.PUSHER_HOST || "localhost",
    pusherPort: process.env.PUSHER_PORT || 6001,
    pusherUseTLS: process.env.PUSHER_USE_TLS === "true" || false,
    authEndpoint: `${process.env.BACKEND_BASE_URL || "http://localhost:3000"}/pusher/auth`,
    pusherCluster: process.env.PUSHER_CLUSTER || "mt1",
  });
});

// Authentication endpoint
app.post("/pusher/auth", (req, res) => {
  const socketId = req.body.socket_id;
  const channel = req.body.channel_name;

  console.log(`Auth attempt for socket_id: ${socketId}, channel: ${channel}`);

  const MOCK_USER_ID = `user_${Math.random().toString(36).substr(2, 9)}`;
  const MOCK_USER_INFO = {
    name: `Test User ${MOCK_USER_ID.split("_")[1]}`,
    id: MOCK_USER_ID,
    avatar: `https://ui-avatars.com/api/?name=${MOCK_USER_ID}&background=random`,
  };

  try {
    if (channel.startsWith("private-")) {
      const authResponse = pusher.authorizeChannel(socketId, channel);
      console.log("Auth success (private):", authResponse);
      res.send(authResponse);
    } else if (channel.startsWith("presence-")) {
      const presenceData = {
        user_id: MOCK_USER_ID,
        user_info: MOCK_USER_INFO,
      };
      const authResponse = pusher.authorizeChannel(
        socketId,
        channel,
        presenceData,
      );
      console.log("Auth success (presence):", authResponse);
      res.send(authResponse);
    } else {
      console.error(
        `Auth failed: Channel ${channel} is not private or presence.`,
      );
      res.status(403).send("Forbidden: Channel is not private or presence");
    }
  } catch (error) {
    console.error("Auth error:", error);
    res.status(500).send(`Authentication error: ${error.message}`);
  }
});

// Webhook endpoint
app.post("/pusher/webhooks", (req, res) => {
  console.log("\n--- Webhook Received ---");

  const receivedSignature = req.headers["x-pusher-signature"];
  const expectedSignature = crypto
    .createHmac("sha256", process.env.PUSHER_APP_SECRET)
    .update(req.rawBody)
    .digest("hex");

  if (receivedSignature && receivedSignature !== expectedSignature) {
    console.error("Webhook signature invalid!");
    return res.status(403).send("Webhook signature invalid");
  }

  const webhookBody = req.body;
  console.log("Webhook Body:", JSON.stringify(webhookBody, null, 2));

  const webhookData = {
    timestamp: new Date().toISOString(),
    headers: req.headers,
    body: webhookBody,
  };
  receivedWebhooks.unshift(webhookData);
  if (receivedWebhooks.length > 100) {
    receivedWebhooks.pop();
  }

  res.status(200).json({ message: "Webhook received" });
});

// Server-side event trigger endpoint
app.post("/trigger-event", async (req, res) => {
  try {
    const { channel, event, data, tags, conflation_key } = req.body;

    if (!channel || !event) {
      return res.status(400).json({ error: "Channel and event are required" });
    }

    console.log(`Triggering server event: ${event} on ${channel}`);
    if (tags) console.log(`  Tags: ${JSON.stringify(tags)}`);
    if (conflation_key) console.log(`  Conflation Key: ${conflation_key}`);

    // Build the trigger options
    const triggerOptions = {};
    if (tags) triggerOptions.tags = tags;
    if (conflation_key) triggerOptions.conflation_key = conflation_key;

    const result = await pusher.trigger(
      channel,
      event,
      data || {},
      triggerOptions,
    );

    const eventRecord = {
      timestamp: new Date().toISOString(),
      type: "server-triggered",
      channel,
      event,
      data: data || {},
      tags: tags || null,
      conflation_key: conflation_key || null,
      result,
    };
    eventHistory.unshift(eventRecord);
    if (eventHistory.length > 100) {
      eventHistory.pop();
    }

    res.json({ success: true, result });
  } catch (error) {
    console.error("Error triggering event:", error);
    res.status(500).json({ error: error.message });
  }
});

// Batch events endpoint
app.post("/trigger-batch-events", async (req, res) => {
  try {
    const { channel, count = 5, delay = 1000 } = req.body;

    if (!channel) {
      return res.status(400).json({ error: "Channel is required" });
    }

    res.json({ success: true, message: `Triggering ${count} events...` });

    // Trigger events with delay
    for (let i = 1; i <= count; i++) {
      setTimeout(
        async () => {
          try {
            await pusher.trigger(channel, "batch-test", {
              message: `Batch message ${i} of ${count}`,
              timestamp: new Date().toISOString(),
              sequence: i,
            });
            console.log(`Batch event ${i}/${count} sent to ${channel}`);
          } catch (error) {
            console.error(`Error sending batch event ${i}:`, error);
          }
        },
        (i - 1) * delay,
      );
    }
  } catch (error) {
    console.error("Error triggering batch events:", error);
    res.status(500).json({ error: error.message });
  }
});

// Get various logs
app.get("/webhooks-log", (req, res) => {
  res.json(receivedWebhooks);
});

app.get("/event-history", (req, res) => {
  res.json(eventHistory);
});

app.get("/channel-info", async (req, res) => {
  try {
    const { channel } = req.query;
    if (!channel) {
      return res.status(400).json({ error: "Channel parameter required" });
    }

    const info = await pusher.get({ path: `/channels/${channel}` });
    res.json(info);
  } catch (error) {
    console.error("Error getting channel info:", error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/channels", async (req, res) => {
  try {
    const channels = await pusher.get({ path: "/channels" });
    res.json(channels);
  } catch (error) {
    console.error("Error getting channels:", error);
    res.status(500).json({ error: error.message });
  }
});

// Start server
app.listen(port, () => {
  console.log(`\n🚀 Pusher Test Backend Server`);
  console.log(`📡 Running at ${process.env.BACKEND_BASE_URL}`);
  console.log(`🔐 Auth Endpoint: POST /pusher/auth`);
  console.log(`🪝 Webhook Endpoint: POST /pusher/webhooks`);
  console.log(`⚡ Trigger Event: POST /trigger-event`);
  console.log(`📊 Dashboard available at: ${process.env.BACKEND_BASE_URL}`);
  console.log(`================================\n`);
});
