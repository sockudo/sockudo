#!/usr/bin/env node
import crypto from "node:crypto";
import { createServer } from "node:http";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const stateDir = join(repoRoot, ".apns-e2e");

const config = {
  listenHost: process.env.APNS_BRIDGE_HOST ?? "0.0.0.0",
  listenPort: Number(process.env.APNS_BRIDGE_PORT ?? 8765),
  sockudoUrl: process.env.SOCKUDO_URL ?? "http://127.0.0.1:6001",
  appId: process.env.SOCKUDO_APP_ID ?? "app-id",
  key: process.env.SOCKUDO_APP_KEY ?? "app-key",
  secret: process.env.SOCKUDO_APP_SECRET ?? "app-secret",
  channel: process.env.APNS_CANARY_CHANNEL ?? `apns-canary-${Date.now()}`,
  maxPolls: Number(process.env.APNS_CANARY_MAX_POLLS ?? 30),
  pollMs: Number(process.env.APNS_CANARY_POLL_MS ?? 1000),
};

await mkdir(stateDir, { recursive: true });

const server = createServer(async (req, res) => {
  try {
    if (req.method === "GET" && req.url === "/health") {
      return sendJson(res, 200, { ok: true });
    }
    if (req.method !== "POST" || req.url !== "/apns-token") {
      return sendJson(res, 404, { ok: false, error: "not found" });
    }

    const body = await readJson(req);
    const token = String(body.token ?? "").trim();
    const bundleId = String(body.bundleId ?? "").trim();
    if (!/^[0-9a-fA-F]+$/.test(token) || token.length < 32) {
      return sendJson(res, 400, { ok: false, error: "invalid APNs token" });
    }

    const result = await runCanary({ token, bundleId });
    return sendJson(res, 202, { ok: true, ...result });
  } catch (error) {
    console.error(error);
    return sendJson(res, 500, { ok: false, error: error.message });
  }
});

server.listen(config.listenPort, config.listenHost, () => {
  console.log(`Sockudo APNs bridge listening on http://${config.listenHost}:${config.listenPort}`);
  console.log(`Sockudo API target: ${config.sockudoUrl}/apps/${config.appId}/push/*`);
});

async function runCanary({ token, bundleId }) {
  const prefix = `apns-e2e-${Date.now()}`;
  const deviceId = `${prefix}-device`;
  const clientId = `${prefix}-client`;

  await writeFile(join(stateDir, "latest-token.txt"), `${token}\n`);

  const activation = await signedJson("POST", "/push/deviceRegistrations", {
    appId: config.appId,
    id: deviceId,
    clientId,
    deviceSecret: `${prefix}-secret`,
    formFactor: "phone",
    platform: "ios",
    timezone: "UTC",
    locale: "en",
    push: {
      state: "ACTIVE",
      recipient: {
        transportType: "apns",
        deviceToken: token,
      },
    },
  }, {
    "x-sockudo-push-capability": "push-admin",
    "x-sockudo-rotate-device-identity-token": "true",
  });

  await signedJson("POST", "/push/channelSubscriptions", {
    appId: config.appId,
    channel: config.channel,
    deviceId,
    clientId,
    provider: "apns",
    tokenHash: activation.tokenHash,
    credentialVersion: 1,
  }, pushAdminHeaders());

  const publishId = `${prefix}-publish`;
  const accepted = await signedJson("POST", "/push/publish", {
    publishId,
    recipients: [{ type: "channel", channel: config.channel }],
    payload: {
      title: "Sockudo APNs canary",
      body: `Bundle ${bundleId || "unknown"} via Sockudo`,
      collapseKey: publishId,
      templateData: {
        bundleId,
        source: "tools/apns-e2e",
      },
    },
    providerOverrides: [],
    sync: false,
  }, pushAdminHeaders());

  let status = null;
  for (let poll = 0; poll < config.maxPolls; poll += 1) {
    status = await signedJson("GET", `/push/publish/${encodeURIComponent(publishId)}/status`, undefined, pushAdminHeaders());
    if (!["queued", "planning", "dispatching", "throttled"].includes(String(status.state))) {
      break;
    }
    await sleep(config.pollMs);
  }

  const summary = {
    bundleId,
    channel: config.channel,
    deviceId,
    publishId,
    accepted,
    status,
  };
  await writeFile(join(stateDir, "latest-summary.json"), `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ...summary, token: "[redacted]" }, null, 2));
  return {
    channel: config.channel,
    publishId,
    state: status?.state ?? null,
    counters: status?.counters ?? null,
  };
}

async function signedJson(method, pushPath, body, headers = {}) {
  const fullPath = `/apps/${config.appId}${pushPath}`;
  const bodyText = body === undefined ? undefined : JSON.stringify(stripUndefined(body));
  const query = {
    auth_key: config.key,
    auth_timestamp: `${Math.floor(Date.now() / 1_000)}`,
    auth_version: "1.0",
  };
  if (bodyText !== undefined) {
    query.body_md5 = crypto.createHash("md5").update(bodyText).digest("hex");
  }
  const canonicalQuery = Object.keys(query)
    .map((key) => [key.toLowerCase(), query[key]])
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  const stringToSign = `${method}\n${fullPath}\n${canonicalQuery}`;
  query.auth_signature = crypto
    .createHmac("sha256", config.secret)
    .update(stringToSign)
    .digest("hex");

  const url = new URL(`${config.sockudoUrl}${fullPath}`);
  for (const [key, value] of Object.entries(query)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      ...headers,
    },
    body: bodyText,
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Sockudo HTTP ${response.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

function pushAdminHeaders() {
  return { "x-sockudo-push-capability": "push-admin" };
}

function stripUndefined(value) {
  if (Array.isArray(value)) {
    return value.map(stripUndefined);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, entry]) => entry !== undefined)
        .map(([key, entry]) => [key, stripUndefined(entry)]),
    );
  }
  return value;
}

function readJson(req) {
  return new Promise((resolveRead, rejectRead) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("error", rejectRead);
    req.on("end", () => {
      try {
        const text = Buffer.concat(chunks).toString("utf8");
        resolveRead(text ? JSON.parse(text) : {});
      } catch (error) {
        rejectRead(error);
      }
    });
  });
}

function sendJson(res, status, body) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

function sleep(ms) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}
