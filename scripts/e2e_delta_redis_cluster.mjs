import crypto from "node:crypto";
import process from "node:process";

// Usage:
//   docker compose -f docker-compose.redis-cluster.yml up --build -d
//   node scripts/e2e_delta_redis_cluster.mjs
//
// Optional overrides:
//   HTTP_BASES=http://127.0.0.1:6002,http://127.0.0.1:6003,http://127.0.0.1:6004
//   WS_URLS=ws://127.0.0.1:6002,ws://127.0.0.1:6003,ws://127.0.0.1:6004
const APP_ID = process.env.APP_ID ?? "test-app";
const APP_KEY = process.env.APP_KEY ?? "test-key";
const APP_SECRET = process.env.APP_SECRET ?? "test-secret";
const HTTP_BASES = (process.env.HTTP_BASES ?? "http://127.0.0.1:6002,http://127.0.0.1:6003,http://127.0.0.1:6004")
  .split(",")
  .map((url) => url.trim())
  .filter(Boolean);
const WS_URLS = (process.env.WS_URLS ?? "ws://127.0.0.1:6002,ws://127.0.0.1:6003,ws://127.0.0.1:6004")
  .split(",")
  .map((url) => `${url.trim().replace(/\/$/, "")}/app/${APP_KEY}?protocol=2`)
  .filter(Boolean);

const MARKET_CHANNEL = process.env.MARKET_CHANNEL ?? `ticker:BTC`;
const ETH_CHANNEL = process.env.ETH_CHANNEL ?? `ticker:ETH`;
const PRESENCE_CHANNEL = process.env.PRESENCE_CHANNEL ?? `presence-delta-cluster-${Date.now()}`;

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function hmac(input) {
  return crypto.createHmac("sha256", APP_SECRET).update(input).digest("hex");
}

function signRequest(method, path, body = "", extraParams = {}) {
  const params = {
    ...extraParams,
    auth_key: APP_KEY,
    auth_timestamp: Math.floor(Date.now() / 1000).toString(),
    auth_version: "1.0",
  };

  if (body.length > 0) {
    params.body_md5 = crypto.createHash("md5").update(body).digest("hex");
  }

  const queryForSig = Object.keys(params)
    .sort()
    .map((key) => `${key}=${params[key]}`)
    .join("&");
  params.auth_signature = hmac(`${method}\n${path}\n${queryForSig}`);
  return new URLSearchParams(params);
}

async function signedFetch(base, method, path, { bodyObject, query = {} } = {}) {
  const body = bodyObject ? JSON.stringify(bodyObject) : "";
  const params = signRequest(method, path, body, query);
  const response = await fetch(`${base}${path}?${params.toString()}`, {
    method,
    headers: body ? { "content-type": "application/json" } : undefined,
    body: body || undefined,
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${method} ${base}${path} failed (${response.status}): ${text}`);
  }
  return text ? JSON.parse(text) : null;
}

async function publishEvent(base, { channel, name, payload, tags }) {
  return signedFetch(base, "POST", `/apps/${APP_ID}/events`, {
    bodyObject: {
      channel,
      name,
      data: JSON.stringify(payload),
      ...(tags ? { tags } : {}),
    },
  });
}

async function readHistory(base, channel) {
  return signedFetch(base, "GET", `/apps/${APP_ID}/channels/${channel}/history`, {
    query: { limit: "20", direction: "newest_first" },
  });
}

async function readPresenceHistory(base, channel) {
  return signedFetch(base, "GET", `/apps/${APP_ID}/channels/${channel}/presence/history`, {
    query: { limit: "20", direction: "newest_first" },
  });
}

function parseData(value) {
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function normalizedEventName(event) {
  return String(event ?? "")
    .replace(/^pusher_internal:/, "")
    .replace(/^sockudo_internal:/, "")
    .replace(/^pusher:/, "")
    .replace(/^sockudo:/, "");
}

function isInternal(event) {
  const normalized = normalizedEventName(event);
  return (
    normalized === "connection_established" ||
    normalized === "subscription_succeeded" ||
    normalized === "delta_compression_enabled" ||
    normalized === "delta_cache_sync" ||
    normalized === "member_added" ||
    normalized === "member_removed"
  );
}

function isDeltaEvent(message) {
  const data = parseData(message.data);
  return normalizedEventName(message.event) === "delta" || Boolean(data?.__delta);
}

function eventSummary(message) {
  return {
    event: message.event,
    channel: message.channel,
    data: parseData(message.data),
  };
}

class TestClient {
  constructor(name, wsUrl, subscriptions) {
    this.name = name;
    this.wsUrl = wsUrl;
    this.subscriptions = subscriptions;
    this.events = [];
    this.socketId = null;
    this.ws = null;
  }

  async connect() {
    this.ws = new WebSocket(this.wsUrl);
    this.ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      this.events.push(payload);

      if (normalizedEventName(payload.event) === "connection_established") {
        const data = parseData(payload.data);
        this.socketId = data.socket_id;
        for (const subscription of this.subscriptions(this.socketId)) {
          this.ws.send(JSON.stringify({ event: "pusher:subscribe", data: subscription }));
        }
      }
    };
    this.ws.onerror = (event) => {
      this.events.push({ event: "ws:error", detail: String(event?.message ?? "unknown") });
    };

    await this.waitFor(
      () => this.subscribedChannels().length >= this.subscriptions("0.0").length,
      12000,
      "all subscriptions to succeed",
    );
  }

  subscribedChannels() {
    return this.events
      .filter((event) => normalizedEventName(event.event) === "subscription_succeeded")
      .map((event) => event.channel);
  }

  async waitFor(predicate, timeoutMs, label) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (predicate()) {
        return;
      }
      await wait(25);
    }
    throw new Error(
      `${this.name}: timed out waiting for ${label}; recent=${JSON.stringify(
        this.events.slice(-10).map(eventSummary),
      )}`,
    );
  }

  businessEvents() {
    return this.events.filter((event) => {
      if (!event.channel?.startsWith("ticker:")) {
        return false;
      }
      if (isInternal(event.event) || isDeltaEvent(event)) {
        return false;
      }
      return true;
    });
  }

  deltaEvents() {
    return this.events.filter(isDeltaEvent);
  }

  close() {
    try {
      this.ws?.close();
    } catch {}
  }
}

function publicSubscription(channel, { filter, delta }) {
  return {
    channel,
    ...(filter ? { filter } : {}),
    ...(delta !== undefined ? { delta } : {}),
  };
}

function presenceSubscription(socketId, channel, userId, userInfo) {
  const channelData = JSON.stringify({ user_id: userId, user_info: userInfo });
  return {
    channel,
    channel_data: channelData,
    auth: `${APP_KEY}:${hmac(`${socketId}:${channel}:${channelData}`)}`,
  };
}

async function waitForHealth() {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    const checks = await Promise.allSettled(
      HTTP_BASES.map((base) => fetch(`${base}/up`).then((response) => response.ok)),
    );
    if (checks.every((check) => check.status === "fulfilled" && check.value)) {
      return;
    }
    await wait(500);
  }
  throw new Error(`Sockudo nodes did not become healthy: ${HTTP_BASES.join(", ")}`);
}

async function main() {
  assert(typeof WebSocket !== "undefined", "This script needs Node.js with global WebSocket support");
  assert(HTTP_BASES.length >= 3 && WS_URLS.length >= 3, "Expected three HTTP_BASES and WS_URLS entries");

  await waitForHealth();

  const btcDeltaOnNode1 = new TestClient("btcDeltaOnNode1", WS_URLS[0], () => [
    publicSubscription(MARKET_CHANNEL, {
      filter: {
        events: ["price-update"],
        tags: { cmp: "eq", key: "symbol", val: "BTC" },
      },
      delta: { enabled: true, algorithm: "fossil" },
    }),
  ]);
  const btcWildcardDeltaOnNode2 = new TestClient("btcWildcardDeltaOnNode2", WS_URLS[1], () => [
    publicSubscription("ticker:*", {
      filter: {
        events: ["price-update"],
        tags: { cmp: "eq", key: "symbol", val: "BTC" },
      },
      delta: { enabled: true, algorithm: "fossil" },
    }),
  ]);
  const allPricesNoDeltaOnNode3 = new TestClient("allPricesNoDeltaOnNode3", WS_URLS[2], () => [
    publicSubscription("ticker:*", {
      filter: { events: ["price-update"] },
      delta: false,
    }),
  ]);
  const presenceAOnNode1 = new TestClient("presenceAOnNode1", WS_URLS[0], (socketId) => [
    presenceSubscription(socketId, PRESENCE_CHANNEL, "user-a", { node: "node1" }),
  ]);
  const presenceBOnNode2 = new TestClient("presenceBOnNode2", WS_URLS[1], (socketId) => [
    presenceSubscription(socketId, PRESENCE_CHANNEL, "user-b", { node: "node2" }),
  ]);

  const clients = [
    btcDeltaOnNode1,
    btcWildcardDeltaOnNode2,
    allPricesNoDeltaOnNode3,
    presenceAOnNode1,
    presenceBOnNode2,
  ];

  try {
    for (const client of clients) {
      await client.connect();
      await wait(100);
    }

    const filler = "cluster-delta-e2e-".repeat(80);
    const publisher = HTTP_BASES[2];

    for (let price = 50000; price <= 50050; price += 10) {
      await publishEvent(publisher, {
        channel: MARKET_CHANNEL,
        name: "price-update",
        payload: { item_id: "btc-1", symbol: "BTC", price, filler },
        tags: { symbol: "BTC", asset_class: "crypto" },
      });
      await wait(140);
    }

    await publishEvent(publisher, {
      channel: ETH_CHANNEL,
      name: "price-update",
      payload: { item_id: "eth-1", symbol: "ETH", price: 3000, filler },
      tags: { symbol: "ETH", asset_class: "crypto" },
    });
    await wait(140);

    await publishEvent(publisher, {
      channel: MARKET_CHANNEL,
      name: "trade-executed",
      payload: { item_id: "btc-1", symbol: "BTC", quantity: 2, filler },
      tags: { symbol: "BTC", asset_class: "crypto" },
    });

    await wait(1800);

    const btcNode1Business = btcDeltaOnNode1.businessEvents();
    const btcNode2Business = btcWildcardDeltaOnNode2.businessEvents();
    const allNode3Business = allPricesNoDeltaOnNode3.businessEvents();

    assert(btcDeltaOnNode1.deltaEvents().length > 0, "node1 BTC delta client did not receive any delta frames");
    assert(btcWildcardDeltaOnNode2.deltaEvents().length > 0, "node2 wildcard delta client did not receive any delta frames");
    assert(allPricesNoDeltaOnNode3.deltaEvents().length === 0, "delta-disabled node3 client received delta frames");
    assert(
      btcNode1Business.every((event) => event.channel === MARKET_CHANNEL && event.event === "price-update"),
      "node1 BTC client received an event outside the BTC price filter",
    );
    assert(
      btcNode2Business.every((event) => event.channel === MARKET_CHANNEL && event.event === "price-update"),
      "node2 wildcard BTC client received an event outside the tag filter",
    );
    assert(
      allNode3Business.some((event) => event.channel === MARKET_CHANNEL && event.event === "price-update") &&
        allNode3Business.some((event) => event.channel === ETH_CHANNEL && event.event === "price-update") &&
        allNode3Business.every((event) => event.event === "price-update"),
      "node3 non-delta wildcard client did not receive both BTC and ETH price updates only",
    );

    const history = await readHistory(publisher, MARKET_CHANNEL);
    assert(
      history.items?.some((item) => item.event_name === "price-update") &&
        history.items?.every((item) => item.event_name !== "pusher:delta"),
      "publisher-node message history did not retain full business events",
    );

    const presenceHistoryA = await readPresenceHistory(HTTP_BASES[0], PRESENCE_CHANNEL);
    const presenceHistoryB = await readPresenceHistory(HTTP_BASES[1], PRESENCE_CHANNEL);
    assert(
      presenceHistoryA.items?.some((item) => item.user_id === "user-a") &&
        presenceHistoryB.items?.some((item) => item.user_id === "user-b"),
      "presence history did not record local joins on both cluster nodes",
    );

    const summary = {
      ok: true,
      nodes: HTTP_BASES,
      marketChannel: MARKET_CHANNEL,
      presenceChannel: PRESENCE_CHANNEL,
      deltaFrames: {
        node1Exact: btcDeltaOnNode1.deltaEvents().length,
        node2Wildcard: btcWildcardDeltaOnNode2.deltaEvents().length,
        node3NoDelta: allPricesNoDeltaOnNode3.deltaEvents().length,
      },
      businessEvents: {
        node1Exact: btcNode1Business.map((event) => `${event.channel}:${event.event}`),
        node2Wildcard: btcNode2Business.map((event) => `${event.channel}:${event.event}`),
        node3Wildcard: allNode3Business.map((event) => `${event.channel}:${event.event}`),
      },
      historyItemsOnPublisher: history.items?.length ?? 0,
      presenceHistoryItems: {
        node1: presenceHistoryA.items?.length ?? 0,
        node2: presenceHistoryB.items?.length ?? 0,
      },
    };

    console.log(JSON.stringify(summary, null, 2));
  } finally {
    clients.forEach((client) => client.close());
  }
}

main().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
