import { createECDH, randomBytes } from "node:crypto";
import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "../..");
const stateDir = join(repoRoot, ".webpush-e2e");
const require = createRequire(import.meta.url);
const Sockudo = require(join(
  repoRoot,
  "server-sdks/sockudo-http-node/dist/sockudo.js",
));
const port = Number(process.env.PORT || 5179);
const sockudoUrl = process.env.SOCKUDO_URL || "http://127.0.0.1:6001";
const sockudoAppId = process.env.SOCKUDO_APP_ID || "app-id";
const sockudoKey = process.env.SOCKUDO_KEY || "app-key";
const sockudoSecret = process.env.SOCKUDO_SECRET || "app-secret";

const vapidPrivateKey =
  process.env.VAPID_PRIVATE_KEY || base64url(randomBytes(32));
const vapidPublicKey = derivePublicKey(vapidPrivateKey);
const vapidContact =
  process.env.VAPID_CONTACT || "mailto:sockudo-webpush-e2e@example.com";
const sdkBundlePath = join(
  repoRoot,
  "client-sdks/sockudo-js/dist/web/sockudo.mjs",
);
const webpushChannel = process.env.WEBPUSH_E2E_CHANNEL || "webpush-e2e";
const sockudoApi = createSockudoApi(sockudoUrl);

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
    if (req.method === "GET" && url.pathname === "/sdk/sockudo.mjs") {
      return sendPath(res, sdkBundlePath, "text/javascript; charset=utf-8");
    }
    if (req.method === "GET" && url.pathname === "/sdk-info") {
      return sendJson(res, {
        ok: true,
        clientSdk: "@sockudo/client",
        serverSdk: "sockudo-http-node",
        channel: webpushChannel,
        sockudoUrl,
        appId: sockudoAppId,
        vapidPublicKey,
      });
    }
    if (req.method === "GET" && url.pathname === "/vapid-public-key") {
      return sendJson(res, { publicKey: vapidPublicKey });
    }
    if (url.pathname.startsWith("/sdk-push/")) {
      return handleSdkPush(req, res, url);
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
  console.log(`Harness SDK proxy: http://127.0.0.1:${port}/sdk-push/*`);
  console.log(`VAPID public key: ${vapidPublicKey}`);
});

async function sendFile(res, filename, contentType) {
  return sendPath(res, join(here, "static", filename), contentType);
}

async function sendPath(res, path, contentType) {
  const body = await readFile(path);
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

async function handleSdkPush(req, res, url) {
  try {
    if (req.method === "POST" && url.pathname === "/sdk-push/deviceRegistrations") {
      const device = await readJson(req);
      const result = await activateDevice(device);
      return sendJson(res, result);
    }
    if (req.method === "POST" && url.pathname === "/sdk-push/channelSubscriptions") {
      const subscription = await readJson(req);
      const result = await sockudoApi.upsertChannelPushSubscription({
        ...subscription,
        appId: sockudoAppId,
      });
      return sendJson(res, result);
    }
    if (req.method === "POST" && url.pathname === "/sdk-push/publish") {
      const publishRequest = await readJson(req);
      const result = await sockudoApi.publishPush(publishRequest);
      return sendJson(res, result);
    }

    const publishStatusMatch = url.pathname.match(
      /^\/sdk-push\/publish\/([^/]+)\/status$/,
    );
    if (req.method === "GET" && publishStatusMatch) {
      const result = await sockudoApi.getPublishStatus(
        decodeURIComponent(publishStatusMatch[1]),
      );
      return sendJson(res, result);
    }

    return sendJson(res, { ok: false, error: "sdk push route not found" }, 404);
  } catch (error) {
    return sendJson(res, sdkError(error), error.statusCode || error.status || 500);
  }
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

  const credential = await ensureWebPushCredential();
  const device = await activateDevice({
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
  const publish = await sockudoApi.publishPush({
    publishId,
    recipients: [{ type: "device", deviceId: "browser" }],
    payload,
    providerOverrides: [],
    sync: false,
  });
  const statusBeforeDelivery = await sockudoApi.getPublishStatus(publishId);
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

async function ensureWebPushCredential() {
  return sockudoApi.putPushCredential("webpush", {
    appId: sockudoAppId,
    credentialId: "webpush-e2e",
    publicKey: vapidPublicKey,
    privateKey: vapidPrivateKey,
  });
}

async function activateDevice(device) {
  await ensureWebPushCredential();
  return sockudoApi.activateDevice(
    {
      ...device,
      appId: sockudoAppId,
    },
    { rotateDeviceIdentityToken: true },
  );
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

function parseMaybeJson(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function createSockudoApi(baseUrl) {
  const parsed = new URL(baseUrl);
  return new Sockudo({
    appId: sockudoAppId,
    key: sockudoKey,
    secret: sockudoSecret,
    host: parsed.hostname,
    port: parsed.port || undefined,
    scheme: parsed.protocol.replace(/:$/, ""),
    useTLS: parsed.protocol === "https:",
  });
}

function sdkError(error) {
  const body = typeof error?.body === "string" ? parseMaybeJson(error.body) : null;
  return {
    ok: false,
    error: error instanceof Error ? error.message : String(error),
    status: error?.statusCode || error?.status,
    body: body ?? error?.body,
  };
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
