const Pusher = require("pusher-js");
const crypto = require("crypto");

const CONFIG = {
  host: "localhost",
  port: 6001,
  appId: "app-id",
  appKey: "app-key",
  appSecret: "app-secret",
  channel: "test-channel",
  eventName: "test-event",
};

function generateAuthParams(method, path, body) {
  const timestamp = Math.floor(Date.now() / 1000);
  const bodyMd5 = crypto.createHash("md5").update(body).digest("hex");
  const params = {
    auth_key: CONFIG.appKey,
    auth_timestamp: timestamp,
    auth_version: "1.0",
    body_md5: bodyMd5,
  };
  const queryString = Object.keys(params).sort().map((k) => `${k}=${params[k]}`).join("&");
  const stringToSign = `${method}\n${path}\n${queryString}`;
  const signature = crypto.createHmac("sha256", CONFIG.appSecret).update(stringToSign).digest("hex");
  params.auth_signature = signature;
  return params;
}

async function triggerEvent(channel, event, data) {
  const path = `/apps/${CONFIG.appId}/events`;
  const url = `http://${CONFIG.host}:${CONFIG.port}${path}`;
  const body = JSON.stringify({ name: event, channel: channel, data: JSON.stringify(data) });
  const authParams = generateAuthParams("POST", path, body);
  const queryString = Object.keys(authParams).map((k) => `${k}=${encodeURIComponent(authParams[k])}`).join("&");
  const response = await fetch(`${url}?${queryString}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: body });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function run() {
  console.log("Creating client...");
  
  const pusher = new Pusher(CONFIG.appKey, {
    cluster: "mt1",
    wsHost: CONFIG.host,
    wsPort: CONFIG.port,
    forceTLS: false,
    enabledTransports: ["ws"],
    disableStats: true,
    deltaCompression: { enabled: true, algorithms: ["fossil"], debug: true },
  });

  await new Promise((resolve, reject) => {
    pusher.connection.bind("connected", resolve);
    pusher.connection.bind("error", reject);
    setTimeout(() => reject(new Error("timeout")), 5000);
  });
  console.log("Connected");

  const channel = pusher.subscribe(CONFIG.channel);
  await new Promise((resolve, reject) => {
    channel.bind("pusher:subscription_succeeded", resolve);
    setTimeout(() => reject(new Error("timeout")), 5000);
  });
  console.log("Subscribed");

  // Simple test data - small and predictable
  const testData = { id: "abc123", value: 42 };
  
  console.log("\n=== Sending test message ===");
  console.log("Data sent to HTTP API:", JSON.stringify(testData));
  
  await triggerEvent(CONFIG.channel, CONFIG.eventName, testData);
  
  // Wait for message
  await new Promise(r => setTimeout(r, 1000));
  
  console.log("\n=== Now sending second message to trigger delta ===");
  const testData2 = { id: "abc123", value: 43 };
  console.log("Data sent to HTTP API:", JSON.stringify(testData2));
  
  await triggerEvent(CONFIG.channel, CONFIG.eventName, testData2);
  
  await new Promise(r => setTimeout(r, 2000));
  
  pusher.disconnect();
  console.log("\nDone");
}

run().catch(console.error);
