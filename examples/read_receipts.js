/**
 * Release 4.4 annotation summaries: read receipts.
 *
 * Run after setting `annotations_enabled = true` on the channel rule:
 *
 *   HISTORY_ENABLED=true VERSIONED_MESSAGES_ENABLED=true ANNOTATIONS_ENABLED=true cargo run -p sockudo
 *   node examples/read_receipts.js
 *
 * `flag.v1` requires an identified `client_id`. The summary is a total plus
 * the contributing `clientIds`.
 *
 * This example subscribes normally. Do not set `modes` unless you need raw
 * annotation events; setting modes replaces defaults.
 */

const crypto = require("node:crypto");

const HTTP_URL = process.env.SOCKUDO_HTTP_URL || "http://127.0.0.1:6001";
const WS_URL = process.env.SOCKUDO_WS_URL || "ws://127.0.0.1:6001";
const APP_ID = process.env.SOCKUDO_APP_ID || "app-id";
const APP_KEY = process.env.SOCKUDO_APP_KEY || "app-key";
const APP_SECRET = process.env.SOCKUDO_APP_SECRET || "app-secret";
const CHANNEL = process.env.SOCKUDO_CHANNEL || "chat:annotations";
const EVENT_NAME = "demo.message";
const RECEIPT_TYPE = "receipts:flag.v1";

const messages = new Map();

async function main() {
  const ws = await connectAndSubscribe(CHANNEL);

  ws.onmessage = (event) => {
    const message = parseFrame(event.data);
    if (!message) return;

    const payload = decodeData(message.data);
    if (payload?.action === "message.summary") {
      mergeSummary(payload.serial, payload.annotations?.summary || {});
      renderReceipts(payload.serial);
      return;
    }

    const serial = message.extras?.headers?.sockudo_message_serial;
    if (message.event === EVENT_NAME && serial) {
      messages.set(serial, {
        serial,
        text: payload?.text || String(message.data || ""),
        annotations: { summary: {} },
      });
      console.log(`message ${serial}: ${messages.get(serial).text}`);
    }
  };

  const messageSerial = await publishSeedMessageAndWait();
  for (const clientId of ["alice", "bob", "charlie"]) {
    await publishAnnotation(messageSerial, {
      type: RECEIPT_TYPE,
      client_id: clientId,
    });
  }

  setTimeout(() => ws.close(), 1500);
}

async function connectAndSubscribe(channel) {
  const WebSocketImpl = getWebSocket();
  const ws = new WebSocketImpl(
    `${WS_URL}/app/${encodeURIComponent(APP_KEY)}?protocol=2&client=sockudo-examples&version=1.0.0`,
  );

  await onceOpen(ws);
  await waitForConnection(ws);
  ws.send(JSON.stringify({ event: "sockudo:subscribe", data: { channel } }));
  await waitForEvent(ws, "sockudo_internal:subscription_succeeded");
  return ws;
}

async function publishSeedMessageAndWait() {
  const serialPromise = new Promise((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("Timed out waiting for the seed message serial")),
      5000,
    );
    const interval = setInterval(() => {
      for (const serial of messages.keys()) {
        clearInterval(interval);
        clearTimeout(timeout);
        resolve(serial);
        return;
      }
    }, 25);
  });

  await apiRequest("POST", `/apps/${APP_ID}/events`, {
    name: EVENT_NAME,
    channel: CHANNEL,
    data: JSON.stringify({ text: "Read receipt demo" }),
  });

  return serialPromise;
}

async function publishAnnotation(messageSerial, annotation) {
  await apiRequest(
    "POST",
    `/apps/${APP_ID}/channels/${encodeURIComponent(CHANNEL)}/messages/${encodeURIComponent(
      messageSerial,
    )}/annotations`,
    annotation,
  );
}

function mergeSummary(messageSerial, summary) {
  const message = messages.get(messageSerial) || {
    serial: messageSerial,
    annotations: { summary: {} },
  };
  message.annotations.summary = {
    ...message.annotations.summary,
    ...summary,
  };
  messages.set(messageSerial, message);
}

function renderReceipts(messageSerial) {
  const summary = messages.get(messageSerial)?.annotations.summary[RECEIPT_TYPE];
  if (!summary) return;

  const clipped = summary.clipped ? " (client list clipped)" : "";
  console.log(
    `read receipts for ${messageSerial}: ${summary.total} read by ${(summary.clientIds || []).join(
      ", ",
    )}${clipped}`,
  );
}

async function apiRequest(method, path, body) {
  const bodyText = body ? JSON.stringify(body) : "";
  const query = signRequest(method, path, bodyText);
  const response = await fetch(`${HTTP_URL}${path}?${query}`, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: bodyText || undefined,
  });
  if (!response.ok) {
    throw new Error(`${method} ${path} failed: ${response.status} ${await response.text()}`);
  }
  return response.json();
}

function signRequest(method, path, bodyText) {
  const params = {
    auth_key: APP_KEY,
    auth_timestamp: Math.floor(Date.now() / 1000).toString(),
    auth_version: "1.0",
  };
  if (bodyText) {
    params.body_md5 = crypto.createHash("md5").update(bodyText).digest("hex");
  }
  const queryForSig = Object.keys(params)
    .sort()
    .map((key) => `${key}=${params[key]}`)
    .join("&");
  const signature = crypto
    .createHmac("sha256", APP_SECRET)
    .update(`${method}\n${path}\n${queryForSig}`)
    .digest("hex");
  return new URLSearchParams({ ...params, auth_signature: signature }).toString();
}

function getWebSocket() {
  if (globalThis.WebSocket) return globalThis.WebSocket;
  try {
    return require("ws");
  } catch {
    throw new Error("This example needs Node 22+ or `npm install ws`.");
  }
}

function onceOpen(ws) {
  return new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });
}

function waitForConnection(ws) {
  return waitForEvent(ws, "sockudo:connection_established");
}

function waitForEvent(ws, eventName) {
  return new Promise((resolve, reject) => {
    const previous = ws.onmessage;
    const timeout = setTimeout(() => reject(new Error(`Timed out waiting for ${eventName}`)), 5000);
    ws.onmessage = (event) => {
      previous?.(event);
      const message = parseFrame(event.data);
      if (message?.event === eventName) {
        clearTimeout(timeout);
        ws.onmessage = previous;
        resolve(message);
      }
    };
  });
}

function parseFrame(data) {
  try {
    return JSON.parse(typeof data === "string" ? data : data.toString());
  } catch {
    return null;
  }
}

function decodeData(data) {
  if (typeof data !== "string") return data;
  try {
    return JSON.parse(data);
  } catch {
    return data;
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
