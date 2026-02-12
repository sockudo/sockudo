/**
 * Generic delta-compression smoke test.
 *
 * This intentionally uses only environment-based credentials and synthetic data.
 */

const crypto = require("crypto");
const WebSocket = require("ws");
const fetch = require("node-fetch");

const CONFIG = {
  host: process.env.PUSHER_HOST || "127.0.0.1",
  port: Number(process.env.PUSHER_PORT || 6001),
  appId: process.env.PUSHER_APP_ID || "app-id",
  appKey: process.env.PUSHER_APP_KEY || "app-key",
  appSecret: process.env.PUSHER_APP_SECRET || "app-secret",
  channel: process.env.TEST_CHANNEL || "benchmark",
  eventName: process.env.TEST_EVENT || "timed-message",
  totalMessages: Number(process.env.TEST_MESSAGES || 20),
};

function authParams(method, path, body) {
  const bodyMd5 = crypto.createHash("md5").update(body).digest("hex");
  const params = {
    auth_key: CONFIG.appKey,
    auth_timestamp: Math.floor(Date.now() / 1000),
    auth_version: "1.0",
    body_md5: bodyMd5,
  };

  const qs = Object.keys(params)
    .sort()
    .map((k) => `${k}=${params[k]}`)
    .join("&");

  const sig = crypto
    .createHmac("sha256", CONFIG.appSecret)
    .update(`${method}\n${path}\n${qs}`)
    .digest("hex");

  return { ...params, auth_signature: sig };
}

async function publish(data) {
  const path = `/apps/${CONFIG.appId}/events`;
  const body = JSON.stringify({
    name: CONFIG.eventName,
    channel: CONFIG.channel,
    data: JSON.stringify(data),
  });

  const qp = new URLSearchParams(authParams("POST", path, body));
  const url = `http://${CONFIG.host}:${CONFIG.port}${path}?${qp.toString()}`;

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });

  if (!res.ok) {
    throw new Error(`Publish failed: ${res.status} ${await res.text()}`);
  }
}

function buildWsUrl() {
  return `ws://${CONFIG.host}:${CONFIG.port}/app/${CONFIG.appKey}?protocol=7&client=js&version=8.4.0&flash=false`;
}

async function run() {
  const ws = new WebSocket(buildWsUrl());

  let connected = false;
  let subscribed = false;
  let received = 0;

  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("WS timeout")), 8000);

    ws.on("message", (raw) => {
      const msg = JSON.parse(raw.toString());

      if (msg.event === "pusher:connection_established") {
        connected = true;
        ws.send(
          JSON.stringify({
            event: "pusher:enable_delta_compression",
            data: JSON.stringify({ algorithms: ["fossil", "xdelta3"] }),
          }),
        );

        ws.send(
          JSON.stringify({
            event: "pusher:subscribe",
            data: { channel: CONFIG.channel },
          }),
        );
      }

      if (msg.event === "pusher_internal:subscription_succeeded") {
        subscribed = true;
        clearTimeout(timeout);
        resolve();
      }

      if (msg.channel === CONFIG.channel && msg.event === CONFIG.eventName) {
        received += 1;
      }
    });
  });

  for (let i = 0; i < CONFIG.totalMessages; i += 1) {
    await publish({
      id: i % 3,
      seq: i,
      value: 1000 + i,
      t: Date.now(),
    });
  }

  await new Promise((r) => setTimeout(r, 1500));
  ws.close();

  if (!connected || !subscribed) {
    throw new Error("Did not establish websocket + subscription");
  }

  if (received === 0) {
    throw new Error("No messages received on subscription");
  }

  console.log(
    `[delta-smoke] ok: connected=${connected} subscribed=${subscribed} received=${received}`,
  );
}

run().catch((err) => {
  console.error("[delta-smoke] failed:", err.message);
  process.exit(1);
});
