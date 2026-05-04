import { createECDH, createHash, createHmac } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "../..");
const stateDir = join(repoRoot, ".webpush-e2e");
const port = Number(process.env.PORT || 5179);
const sockudoUrl = process.env.SOCKUDO_URL || "http://127.0.0.1:6001";
const sockudoAppId = process.env.SOCKUDO_APP_ID || "app-id";
const sockudoKey = process.env.SOCKUDO_KEY || "app-key";
const sockudoSecret = process.env.SOCKUDO_SECRET || "app-secret";

const vapidPrivateKey =
  process.env.VAPID_PRIVATE_KEY || base64url(Buffer.alloc(32, 1));
const vapidPublicKey = derivePublicKey(vapidPrivateKey);
const vapidContact =
  process.env.VAPID_CONTACT || "mailto:sockudo-webpush-e2e@example.com";

await mkdir(stateDir, { recursive: true });

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host}`);
    if (req.method === "GET" && url.pathname === "/") {
      return sendFile(res, "index.html", "text/html; charset=utf-8");
    }
    if (req.method === "GET" && url.pathname === "/sw.js") {
      return sendFile(res, "sw.js", "text/javascript; charset=utf-8");
    }
    if (req.method === "GET" && url.pathname === "/vapid-public-key") {
      return sendJson(res, { publicKey: vapidPublicKey });
    }
    if (req.method === "POST" && url.pathname === "/subscription") {
      const subscription = await readJson(req);
      await writeFile(
        join(stateDir, "subscription.json"),
        JSON.stringify(subscription, null, 2),
      );
      const result = await sendViaSockudo(subscription);
      return sendJson(res, result, result.ok ? 200 : 502);
    }
    if (req.method === "POST" && url.pathname === "/send") {
      const subscription = JSON.parse(
        await readFile(join(stateDir, "subscription.json"), "utf8"),
      );
      const result = await sendViaSockudo(subscription);
      return sendJson(res, result, result.ok ? 200 : 502);
    }
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  } catch (error) {
    sendJson(
      res,
      { ok: false, error: error instanceof Error ? error.message : String(error) },
      500,
    );
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log(`Sockudo Web Push E2E app: http://127.0.0.1:${port}/`);
  console.log(`Sockudo HTTP API: ${sockudoUrl}/apps/${sockudoAppId}/push/*`);
  console.log(`VAPID public key: ${vapidPublicKey}`);
});

async function sendFile(res, filename, contentType) {
  const body = await readFile(join(here, "static", filename));
  res.writeHead(200, {
    "cache-control": "no-store",
    "content-type": contentType,
  });
  res.end(body);
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function sendJson(res, data, status = 200) {
  res.writeHead(status, {
    "cache-control": "no-store",
    "content-type": "application/json; charset=utf-8",
  });
  res.end(JSON.stringify(data, null, 2));
}

async function sendViaSockudo(subscription) {
  const now = Date.now();
  const publishId = `webpush-e2e-${now}`;
  const payload = {
    title: "Sockudo Web Push E2E",
    body: `Delivered at ${new Date(now).toISOString()}`,
    collapseKey: `sockudo-webpush-e2e-${now}`,
    templateData: {
      source: "sockudo-webpush-e2e",
      sentAtMs: now,
    },
  };

  const credential = await sockudoRequest("POST", "/push/credentials/webpush", {
    credentialId: "webpush-e2e",
    publicKey: vapidPublicKey,
    privateKey: vapidPrivateKey,
  });
  const device = await sockudoRequest("POST", "/push/deviceRegistrations", {
    appId: sockudoAppId,
    id: "browser",
    clientId: "webpush-e2e-browser",
    formFactor: "desktop",
    platform: "browser",
    metadata: {
      source: "tools/webpush-e2e",
      userAgent: "browser",
    },
    deviceSecret: "admin-registration-placeholder",
    timezone: "UTC",
    locale: "en",
    lastActiveAtMs: now,
    push: {
      recipient: subscriptionToRecipient(subscription),
      state: "ACTIVE",
      failureCount: 0,
      errorReason: null,
    },
    pushRatePolicy: null,
  });
  const publish = await sockudoRequest("POST", "/push/publish", {
    publishId,
    recipients: [{ type: "device", device_id: "browser" }],
    payload,
    providerOverrides: [],
    sync: false,
  });
  const statusBeforeDelivery = await sockudoRequest(
    "GET",
    `/push/publish/${encodeURIComponent(publishId)}/status`,
  );
  return {
    ok: publish.status === "accepted",
    sockudo: {
      credential,
      device,
      publish,
      statusBeforeDelivery,
    },
  };
}

function subscriptionToRecipient(subscription) {
  if (!subscription?.endpoint || !subscription?.keys?.p256dh || !subscription?.keys?.auth) {
    throw new Error("Browser subscription is missing endpoint, p256dh, or auth");
  }
  return {
    transportType: "web",
    endpoint: subscription.endpoint,
    p256dh: subscription.keys.p256dh,
    auth: subscription.keys.auth,
  };
}

async function sockudoRequest(method, pushPath, body) {
  const path = `/apps/${sockudoAppId}${pushPath}`;
  const bodyText = body === undefined ? null : JSON.stringify(body);
  const query = {
    auth_key: sockudoKey,
    auth_timestamp: String(Math.floor(Date.now() / 1000)),
    auth_version: "1.0",
  };
  if (method === "POST" && bodyText !== null) {
    query.body_md5 = createHash("md5").update(bodyText).digest("hex");
  }
  query.auth_signature = signSockudoRequest(method, path, query);

  const url = new URL(path, sockudoUrl);
  for (const [key, value] of Object.entries(query)) {
    url.searchParams.set(key, value);
  }

  const response = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      "x-sockudo-push-capability": "push-admin",
    },
    body: bodyText,
  });
  const responseText = await response.text();
  const responseBody = parseMaybeJson(responseText) ?? responseText;
  if (!response.ok) {
    throw new Error(
      `Sockudo ${method} ${pushPath} returned ${response.status}: ${JSON.stringify(
        responseBody,
      )}`,
    );
  }
  return responseBody;
}

function signSockudoRequest(method, path, query) {
  const signingQuery = Object.entries(query)
    .filter(([key]) => key !== "auth_signature")
    .map(([key, value]) => [key.toLowerCase(), value])
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
  const stringToSign = `${method.toUpperCase()}\n${path}\n${signingQuery}`;
  return createHmac("sha256", sockudoSecret).update(stringToSign).digest("hex");
}

function parseMaybeJson(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function derivePublicKey(privateKey) {
  const ecdh = createECDH("prime256v1");
  ecdh.setPrivateKey(base64urlDecode(privateKey));
  return base64url(ecdh.getPublicKey(undefined, "uncompressed"));
}

function base64url(buffer) {
  return Buffer.from(buffer)
    .toString("base64")
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
}

function base64urlDecode(value) {
  const padded = value.replaceAll("-", "+").replaceAll("_", "/");
  return Buffer.from(padded + "=".repeat((4 - (padded.length % 4)) % 4), "base64");
}
