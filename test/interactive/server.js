// server.js
require("dotenv").config(); // Load .env file
const express = require("express");
const bodyParser = require("body-parser");
const Pusher = require("pusher"); // Use Pusher library for convenience, especially auth
const crypto = require("crypto");

const app = express();
const port = process.env.BACKEND_PORT || 3000;

// --- IMPORTANT: Configure with YOUR WebSocket server's credentials ---
const pusher = new Pusher({
  appId: process.env.PUSHER_APP_ID,
  key: process.env.PUSHER_APP_KEY,
  secret: process.env.PUSHER_APP_SECRET,
  // Cluster is usually irrelevant for self-hosted, but required by the library
  cluster: "mt1", // Provide a dummy cluster if needed, or omit if library allows
  // --- Crucial for self-hosted ---
  host: process.env.PUSHER_HOST,
  port: process.env.PUSHER_PORT,
  useTLS: process.env.PUSHER_USE_TLS === "true",
  // Use httpAgent/httpsAgent for custom TLS settings if needed
});

console.log("Pusher Server SDK configured for:", {
  appId: process.env.PUSHER_APP_ID,
  key: process.env.PUSHER_APP_KEY ? "***" : "Not Set", // Hide key in logs
  secret: process.env.PUSHER_APP_SECRET ? "***" : "Not Set", // Hide secret in logs
  host: process.env.PUSHER_HOST,
  port: process.env.PUSHER_PORT,
  useTLS: process.env.PUSHER_USE_TLS === "true",
});

// --- Store received webhooks temporarily ---
let receivedWebhooks = [];

// --- Middleware ---
// Serve static files from 'public' directory
app.use(express.static("public"));
// Need raw body for webhook verification
app.use(
  bodyParser.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    },
  }),
);
// Use urlencoded parser for auth endpoint POST data
app.use(bodyParser.urlencoded({ extended: false }));

// --- Routes ---

// Endpoint for frontend config
app.get("/config", (req, res) => {
  res.json({
    pusherKey: process.env.PUSHER_APP_KEY,
    pusherHost: process.env.PUSHER_HOST,
    pusherPort: process.env.PUSHER_PORT,
    pusherUseTLS: process.env.PUSHER_USE_TLS === "true",
    authEndpoint: `${process.env.BACKEND_BASE_URL}/pusher/auth`,
    // Cluster is often needed by pusher-js even if server doesn't use it
    pusherCluster: "mt1", // Or make this configurable if needed
  });
});

// 1. AUTHENTICATION Endpoint (for Private and Presence channels)
app.post("/pusher/auth", (req, res) => {
  const socketId = req.body.socket_id;
  const channel = req.body.channel_name;

  console.log(`Auth attempt for socket_id: ${socketId}, channel: ${channel}`);

  // --- !!! Basic Mock Authentication - Replace with real logic if needed !!! ---
  // For testing, we'll authenticate anyone. In a real app, you'd check session/token.
  const MOCK_USER_ID = `user_${Date.now()}`; // Simple unique ID
  const MOCK_USER_INFO = {
    name: `Test User ${MOCK_USER_ID}`,
    id: MOCK_USER_ID,
  };
  // --- End Mock Authentication ---

  try {
    if (channel.startsWith("private-")) {
      const authResponse = pusher.authorizeChannel(socketId, channel);
      console.log("Auth success (private):", authResponse);
      res.send(authResponse);
    } else if (channel.startsWith("presence-")) {
      // For presence channels, you need presenceData
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
      // If it's not private or presence, deny auth request (shouldn't happen)
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

// 2. WEBHOOK Endpoint (Receives events FROM your WebSocket server)
app.post("/pusher/webhooks", (req, res) => {
  console.log("\n--- Webhook Received ---");

  // --- IMPORTANT: Verify Webhook Signature (Security!) ---
  // Pusher (and compatible servers) send signatures in headers
  const receivedSignature = req.headers["x-pusher-signature"];
  const expectedSignature = crypto
    .createHmac("sha256", process.env.PUSHER_APP_SECRET)
    .update(req.rawBody) // Use the raw body buffer
    .digest("hex");

  if (!receivedSignature) {
    console.warn(
      "Webhook received without signature. Skipping verification (Configure your server to send signatures!).",
    );
    // Decide if you want to proceed without signature for testing
    // return res.status(400).send('Webhook signature missing');
  } else if (receivedSignature !== expectedSignature) {
    console.error("Webhook signature invalid!");
    console.error(`Received: ${receivedSignature}`);
    console.error(`Expected: ${expectedSignature}`);
    return res.status(403).send("Webhook signature invalid");
  } else {
    console.log("Webhook signature verified successfully.");
  }
  // --- End Signature Verification ---

  const webhookBody = req.body; // Already parsed by bodyParser.json
  console.log("Webhook Body:", JSON.stringify(webhookBody, null, 2));

  // Store webhook for potential display on frontend
  const webhookData = {
    timestamp: new Date().toISOString(),
    headers: req.headers,
    body: webhookBody,
  };
  receivedWebhooks.unshift(webhookData); // Add to beginning
  // Keep only the latest N webhooks (e.g., 50)
  if (receivedWebhooks.length > 50) {
    receivedWebhooks.pop();
  }

  // Respond to the webhook sender (your WebSocket server)
  res.status(200).json({ message: "Webhook received" });
});

// Endpoint for frontend to fetch received webhooks
app.get("/webhooks-log", (req, res) => {
  res.json(receivedWebhooks);
});

// --- Start Server ---
app.listen(port, () => {
  console.log(`\n--- Pusher Test Backend Server ---`);
  console.log(
    `Authentication & Webhook server running at ${process.env.BACKEND_BASE_URL}`,
  );
  console.log(` - Serving frontend from: ./public`);
  console.log(
    ` - Auth Endpoint: POST ${process.env.BACKEND_BASE_URL}/pusher/auth`,
  );
  console.log(
    ` - Webhook Endpoint: POST ${process.env.BACKEND_BASE_URL}/pusher/webhooks`,
  );
  console.log(` - Config Endpoint: GET ${process.env.BACKEND_BASE_URL}/config`);
  console.log(
    ` - Webhook Log: GET ${process.env.BACKEND_BASE_URL}/webhooks-log`,
  );
  console.log(`----------------------------------\n`);
});
