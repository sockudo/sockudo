const Pusher = require("pusher-js");
const crypto = require("crypto");

const CONFIG = {
  host: "localhost",
  port: 6001,
  appId: "app-id",
  appKey: "app-key",
  appSecret: "app-secret",
  channel: "test-ch2",
  eventName: "ev",
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
  
  let receivedMessages = [];
  
  const pusher = new Pusher(CONFIG.appKey, {
    cluster: "mt1",
    wsHost: CONFIG.host,
    wsPort: CONFIG.port,
    forceTLS: false,
    enabledTransports: ["ws"],
    disableStats: true,
    deltaCompression: { 
      enabled: true, 
      algorithms: ["fossil"], 
      debug: true,
      onStats: (stats) => console.log("Delta stats:", stats),
      onError: (err) => console.log("Delta error:", err)
    },
  });

  await new Promise((resolve, reject) => {
    pusher.connection.bind("connected", resolve);
    pusher.connection.bind("error", reject);
    setTimeout(() => reject(new Error("timeout")), 5000);
  });
  console.log("Connected");

  const channel = pusher.subscribe(CONFIG.channel);
  
  channel.bind(CONFIG.eventName, (data) => {
    console.log(">>> RECEIVED EVENT DATA:", JSON.stringify(data));
    receivedMessages.push(data);
  });
  
  await new Promise((resolve, reject) => {
    channel.bind("pusher:subscription_succeeded", resolve);
    setTimeout(() => reject(new Error("timeout")), 5000);
  });
  console.log("Subscribed");

  // Send 3 messages
  for (let i = 1; i <= 3; i++) {
    const testData = { id: "abc", val: i };
    console.log(`\n>>> SENDING message ${i}:`, JSON.stringify(testData));
    await triggerEvent(CONFIG.channel, CONFIG.eventName, testData);
    await new Promise(r => setTimeout(r, 500));
  }
  
  await new Promise(r => setTimeout(r, 1000));
  
  console.log("\n=== SUMMARY ===");
  console.log("Messages received:", receivedMessages.length);
  receivedMessages.forEach((m, i) => console.log(`  ${i+1}:`, JSON.stringify(m)));
  
  pusher.disconnect();
}

run().catch(console.error);
