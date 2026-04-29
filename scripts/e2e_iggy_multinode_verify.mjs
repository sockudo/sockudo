import crypto from "node:crypto";
import process from "node:process";

const APP_ID = process.env.APP_ID ?? "demo-app";
const APP_KEY = process.env.APP_KEY ?? "demo-key";
const APP_SECRET = process.env.APP_SECRET ?? "demo-secret";
const EVENT_NAME = process.env.EVENT_NAME ?? "integration-event";
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS ?? "10000");

const CASES = [
  {
    label: "node2-to-node1",
    wsUrl: process.env.WS_URL_1 ?? `ws://127.0.0.1:6001/app/${APP_KEY}?protocol=2`,
    httpBase: process.env.HTTP_BASE_2 ?? "http://127.0.0.1:6002",
  },
  {
    label: "node3-to-node2",
    wsUrl: process.env.WS_URL_2 ?? `ws://127.0.0.1:6002/app/${APP_KEY}?protocol=2`,
    httpBase: process.env.HTTP_BASE_3 ?? "http://127.0.0.1:6003",
  },
  {
    label: "node1-to-node3",
    wsUrl: process.env.WS_URL_3 ?? `ws://127.0.0.1:6003/app/${APP_KEY}?protocol=2`,
    httpBase: process.env.HTTP_BASE_1 ?? "http://127.0.0.1:6001",
  },
];

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function signRequest(method, path, body) {
  const auth_timestamp = Math.floor(Date.now() / 1000).toString();
  const params = {
    auth_key: APP_KEY,
    auth_timestamp,
    auth_version: "1.0",
  };

  if (body && body.length > 0) {
    params.body_md5 = crypto.createHash("md5").update(body).digest("hex");
  }

  const queryForSig = Object.keys(params)
    .sort()
    .map((key) => `${key}=${params[key]}`)
    .join("&");

  const stringToSign = `${method}\n${path}\n${queryForSig}`;
  const auth_signature = crypto
    .createHmac("sha256", APP_SECRET)
    .update(stringToSign)
    .digest("hex");

  return new URLSearchParams({ ...params, auth_signature });
}

async function publishEvent({ httpBase, channel, marker }) {
  const path = `/apps/${APP_ID}/events`;
  const body = JSON.stringify({
    channel,
    name: EVENT_NAME,
    data: JSON.stringify({ marker, sent_at: Date.now() }),
  });
  const query = signRequest("POST", path, body);

  const response = await fetch(`${httpBase}${path}?${query.toString()}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });

  if (!response.ok) {
    throw new Error(`publish ${marker} failed (${response.status}): ${await response.text()}`);
  }
}

async function waitFor(events, predicate, label, timeoutMs = TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const event = events.find(predicate);
    if (event) {
      return event;
    }
    await wait(25);
  }

  throw new Error(
    `timed out waiting for ${label}; events=${JSON.stringify(
      events.map((event) => ({
        event: event.event,
        channel: event.channel,
        data: event.data,
      })),
    )}`,
  );
}

async function runCase({ label, wsUrl, httpBase, channel }) {
  const events = [];
  const ws = new WebSocket(wsUrl);

  ws.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    events.push(payload);
    if (payload.event === "sockudo:connection_established") {
      ws.send(JSON.stringify({ event: "sockudo:subscribe", data: { channel } }));
    }
  };

  ws.onerror = (event) => {
    events.push({ event: "ws:error", data: String(event?.message ?? "unknown") });
  };

  try {
    await waitFor(
      events,
      (event) => event.event === "sockudo_internal:subscription_succeeded",
      `${label} subscription`,
    );

    await publishEvent({ httpBase, channel, marker: label });

    const received = await waitFor(
      events,
      (event) => {
        if (event.event !== EVENT_NAME || event.channel !== channel) {
          return false;
        }
        const data = JSON.parse(event.data);
        return data.marker === label;
      },
      `${label} cross-node event`,
    );

    return { label, ok: true, event: received.event, channel: received.channel };
  } finally {
    ws.close();
  }
}

async function main() {
  const channel = `public-iggy-e2e-${Date.now()}`;
  const results = [];

  for (const testCase of CASES) {
    results.push(await runCase({ ...testCase, channel }));
    await wait(100);
  }

  console.log(JSON.stringify({ channel, results }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
