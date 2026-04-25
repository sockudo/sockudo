import crypto from "node:crypto";
import process from "node:process";

const WS_URL = process.env.WS_URL ?? "ws://127.0.0.1:6001/app/app-key?protocol=2";
const HTTP_BASE = process.env.HTTP_BASE ?? "http://127.0.0.1:6001";
const APP_ID = process.env.APP_ID ?? "app-id";
const APP_KEY = process.env.APP_KEY ?? "app-key";
const APP_SECRET = process.env.APP_SECRET ?? "app-secret";
let finalSummary = null;

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

async function publishEvent({ channel, name, payload, tags }) {
  const path = `/apps/${APP_ID}/events`;
  const body = JSON.stringify({
    channel,
    name,
    data: JSON.stringify(payload),
    ...(tags ? { tags } : {}),
  });

  const query = signRequest("POST", path, body);
  const response = await fetch(`${HTTP_BASE}${path}?${query.toString()}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });

  if (!response.ok) {
    throw new Error(`Publish failed (${response.status}): ${await response.text()}`);
  }
}

class TestClient {
  constructor(name, subscribePayload) {
    this.name = name;
    this.subscribePayload = subscribePayload;
    this.events = [];
    this.ws = null;
  }

  async connect() {
    this.ws = new WebSocket(WS_URL);
    this.ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      this.events.push(payload);

      if (payload.event === "sockudo:connection_established") {
        this.ws.send(JSON.stringify(this.subscribePayload));
      }
    };
    this.ws.onerror = (event) => {
      this.events.push({ event: "ws:error", detail: String(event?.message ?? "unknown") });
    };

    await this.waitFor(
      (msg) => msg.event === "sockudo_internal:subscription_succeeded",
      10000,
      "subscription success",
    );
  }

  async waitFor(predicate, timeoutMs, label) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const existing = this.events.find(predicate);
      if (existing) {
        return existing;
      }
      await wait(25);
    }
    throw new Error(
      `${this.name}: timed out waiting for ${label}; recent events=${JSON.stringify(
        this.events.map((event) => ({ event: event.event, channel: event.channel })),
      )}`,
    );
  }

  businessEvents() {
    return this.events.filter(
      (event) =>
        event.channel?.startsWith("ticker:") &&
        !event.event?.startsWith("sockudo:") &&
        !event.event?.startsWith("sockudo_internal:"),
    );
  }

  deltaEvents() {
    return this.events.filter((event) => event.event === "sockudo:delta");
  }

  deltaEnabledEvents() {
    return this.events.filter(
      (event) => event.event === "sockudo:delta_compression_enabled",
    );
  }

  close() {
    try {
      this.ws?.close();
    } catch {}
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function main() {
  const exactCompoundNoDelta = new TestClient("exactCompoundNoDelta", {
    event: "sockudo:subscribe",
    data: {
      channel: "ticker:BTC",
      filter: {
        events: ["price-update"],
        tags: {
          cmp: "eq",
          key: "symbol",
          val: "BTC",
        },
      },
      delta: false,
    },
  });

  const exactTradeOnly = new TestClient("exactTradeOnly", {
    event: "sockudo:subscribe",
    data: {
      channel: "ticker:BTC",
      filter: {
        events: ["trade-executed"],
      },
      delta: false,
    },
  });

  const exactDeltaEventOnly = new TestClient("exactDeltaEventOnly", {
    event: "sockudo:subscribe",
    data: {
      channel: "ticker:BTC",
      filter: {
        events: ["price-update"],
      },
      delta: {
        enabled: true,
        algorithm: "fossil",
      },
    },
  });

  const wildcardCompoundDelta = new TestClient("wildcardCompoundDelta", {
    event: "sockudo:subscribe",
    data: {
      channel: "ticker:*",
      filter: {
        events: ["price-update"],
        tags: {
          cmp: "eq",
          key: "symbol",
          val: "BTC",
        },
      },
      delta: {
        enabled: true,
        algorithm: "fossil",
      },
    },
  });

  const wildcardEventOnlyNoDelta = new TestClient("wildcardEventOnlyNoDelta", {
    event: "sockudo:subscribe",
    data: {
      channel: "ticker:*",
      filter: {
        events: ["price-update"],
      },
      delta: false,
    },
  });

  const clients = [
    exactCompoundNoDelta,
    exactTradeOnly,
    exactDeltaEventOnly,
    wildcardCompoundDelta,
    wildcardEventOnlyNoDelta,
  ];
  try {
    for (const client of clients) {
      await client.connect();
      await wait(100);
    }

    await wait(300);

    const filler = "x".repeat(320);

    await publishEvent({
      channel: "ticker:BTC",
      name: "price-update",
      payload: { item_id: "btc-1", symbol: "BTC", price: 50000, filler },
      tags: { symbol: "BTC" },
    });
    await wait(120);

    await publishEvent({
      channel: "ticker:ETH",
      name: "price-update",
      payload: { item_id: "eth-1", symbol: "ETH", price: 3000, filler },
      tags: { symbol: "ETH" },
    });
    await wait(120);

    await publishEvent({
      channel: "ticker:BTC",
      name: "trade-executed",
      payload: { item_id: "btc-1", symbol: "BTC", quantity: 2, filler },
      tags: { symbol: "BTC" },
    });
    await wait(120);

    await publishEvent({
      channel: "ticker:BTC",
      name: "price-update",
      payload: { item_id: "btc-1", symbol: "BTC", price: 50010, filler },
      tags: { symbol: "BTC" },
    });
    await wait(120);

    await publishEvent({
      channel: "ticker:BTC",
      name: "price-update",
      payload: { item_id: "btc-1", symbol: "BTC", price: 50020, filler },
      tags: { symbol: "BTC" },
    });

    await wait(1500);

    const exactCompoundBusiness = exactCompoundNoDelta.businessEvents();
    const exactTradeBusiness = exactTradeOnly.businessEvents();
    const exactDeltaBusiness = exactDeltaEventOnly.businessEvents();
    const wildcardCompoundBusiness = wildcardCompoundDelta.businessEvents();
    const wildcardEventOnlyBusiness = wildcardEventOnlyNoDelta.businessEvents();

    finalSummary = {
      exactCompoundNoDelta: {
        totalEvents: exactCompoundNoDelta.events.length,
        businessEvents: exactCompoundBusiness.map((event) => `${event.channel}:${event.event}`),
        deltaEvents: exactCompoundNoDelta.deltaEvents().length,
        deltaEnabledEvents: exactCompoundNoDelta.deltaEnabledEvents().length,
      },
      exactTradeOnly: {
        totalEvents: exactTradeOnly.events.length,
        businessEvents: exactTradeBusiness.map((event) => `${event.channel}:${event.event}`),
        deltaEvents: exactTradeOnly.deltaEvents().length,
        deltaEnabledEvents: exactTradeOnly.deltaEnabledEvents().length,
      },
      exactDeltaEventOnly: {
        totalEvents: exactDeltaEventOnly.events.length,
        businessEvents: exactDeltaBusiness.map((event) => `${event.channel}:${event.event}`),
        deltaEvents: exactDeltaEventOnly.deltaEvents().length,
        deltaEnabledEvents: exactDeltaEventOnly.deltaEnabledEvents().length,
      },
      wildcardCompoundDelta: {
        totalEvents: wildcardCompoundDelta.events.length,
        businessEvents: wildcardCompoundBusiness.map((event) => `${event.channel}:${event.event}`),
        deltaEvents: wildcardCompoundDelta.deltaEvents().length,
        deltaEnabledEvents: wildcardCompoundDelta.deltaEnabledEvents().length,
      },
      wildcardEventOnlyNoDelta: {
        totalEvents: wildcardEventOnlyNoDelta.events.length,
        businessEvents: wildcardEventOnlyBusiness.map((event) => `${event.channel}:${event.event}`),
        deltaEvents: wildcardEventOnlyNoDelta.deltaEvents().length,
        deltaEnabledEvents: wildcardEventOnlyNoDelta.deltaEnabledEvents().length,
      },
    };

    assert(
      exactCompoundBusiness.length >= 3 &&
        exactCompoundBusiness.every(
          (event) => event.event === "price-update" && event.channel === "ticker:BTC",
        ),
      "exactCompoundNoDelta did not receive only BTC price-update business events",
    );
    assert(
      exactTradeBusiness.length === 1 &&
        exactTradeBusiness[0].event === "trade-executed" &&
        exactTradeBusiness[0].channel === "ticker:BTC",
      "exactTradeOnly did not receive exactly one trade-executed event",
    );
    assert(
      exactDeltaBusiness.every(
        (event) => event.event === "price-update" && event.channel === "ticker:BTC",
      ),
      "exactDeltaEventOnly received unexpected business event",
    );
    assert(
      wildcardCompoundBusiness.every(
        (event) => event.event === "price-update" && event.channel === "ticker:BTC",
      ),
      "wildcardCompoundDelta received non-BTC or non-price business event",
    );
    assert(
      wildcardEventOnlyBusiness.filter((event) => event.channel === "ticker:BTC").length >= 3 &&
        wildcardEventOnlyBusiness.filter((event) => event.channel === "ticker:ETH").length >= 1 &&
        wildcardEventOnlyBusiness.every((event) => event.event === "price-update"),
      "wildcardEventOnlyNoDelta did not receive both BTC and ETH price updates only",
    );

    assert(
      exactDeltaEventOnly.deltaEvents().length >= 1,
      "exactDeltaEventOnly did not receive any sockudo:delta",
    );
    assert(
      wildcardCompoundDelta.deltaEvents().length >= 1,
      "wildcardCompoundDelta did not receive any sockudo:delta",
    );
    assert(
      exactCompoundNoDelta.deltaEvents().length === 0 &&
        exactTradeOnly.deltaEvents().length === 0 &&
        wildcardEventOnlyNoDelta.deltaEvents().length === 0,
      "delta-disabled clients received sockudo:delta",
    );

    console.log(JSON.stringify({ ok: true, summary: finalSummary }, null, 2));
  } finally {
    clients.forEach((client) => client.close());
  }
}

main().catch((error) => {
  if (finalSummary) {
    console.error(JSON.stringify({ ok: false, summary: finalSummary }, null, 2));
  }
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
