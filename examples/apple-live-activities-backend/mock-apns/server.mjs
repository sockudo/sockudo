// Mock APNs for Sockudo Live Activity end-to-end tests.
//
// Serves Apple's three Live Activity surfaces over HTTP/2 + TLS on PORT:
//   POST /3/device/{token}                 direct ActivityKit start/update/end
//   POST /4/broadcasts/apps/{bundleId}     iOS 18 broadcast update/end
//   POST|GET|DELETE /1/apps/{bundleId}/channels, GET /1/apps/{bundleId}/all-channels
// and an unauthenticated plain-HTTP inspection surface on INSPECT_PORT:
//   GET /_mock/health, GET|DELETE /_mock/requests, GET /_mock/channels
//
// Provider JWTs are verified with the ES256 public key at APNS_MOCK_PUBLIC_KEY.
// Device-token prefixes trigger Apple's documented failure contract:
//   bad0…  -> 400 BadDeviceToken            4a0e… -> 410 Unregistered
//   429e…  -> 429 TooManyRequests once      5005… -> 500 InternalServerError once
import http from "node:http";
import http2 from "node:http2";
import fs from "node:fs";
import crypto from "node:crypto";

const PORT = Number(process.env.PORT || 8443);
const INSPECT_PORT = Number(process.env.INSPECT_PORT || 8444);
const BUNDLE_ID = process.env.APNS_MOCK_BUNDLE_ID || "com.sockudo.rides";
const TEAM_ID = process.env.APNS_MOCK_TEAM_ID || "MOCKTEAM01";
const KEY_ID = process.env.APNS_MOCK_KEY_ID || "MOCKKEY001";
const LIVE_ACTIVITY_TOPIC = `${BUNDLE_ID}.push-type.liveactivity`;
const MAX_DEVICE_BYTES = 4096;
const MAX_BROADCAST_BYTES = 5120;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const publicKey = process.env.APNS_MOCK_PUBLIC_KEY
  ? crypto.createPublicKey(fs.readFileSync(process.env.APNS_MOCK_PUBLIC_KEY))
  : null;

const requests = [];
const channels = new Map(); // channelId -> { policy: 0|1, createdAt }
const onceFailures = new Set(); // tokens that already consumed their one-shot failure
let seq = 0;

class ApnsError extends Error {
  constructor(status, reason, extraHeaders = {}) {
    super(reason);
    this.status = status;
    this.reason = reason;
    this.extraHeaders = extraHeaders;
  }
}

function b64urlDecode(value) {
  return Buffer.from(value.replace(/-/g, "+").replace(/_/g, "/"), "base64");
}

function verifyProviderToken(headers) {
  const raw = headers.authorization;
  if (!raw || !/^bearer /i.test(raw)) throw new ApnsError(403, "MissingProviderToken");
  const token = raw.slice(7).trim();
  const parts = token.split(".");
  if (parts.length !== 3) throw new ApnsError(403, "InvalidProviderToken");
  let header;
  let claims;
  try {
    header = JSON.parse(b64urlDecode(parts[0]).toString("utf8"));
    claims = JSON.parse(b64urlDecode(parts[1]).toString("utf8"));
  } catch {
    throw new ApnsError(403, "InvalidProviderToken");
  }
  if (header.alg !== "ES256" || header.kid !== KEY_ID) throw new ApnsError(403, "InvalidProviderToken");
  if (claims.iss !== TEAM_ID) throw new ApnsError(403, "InvalidProviderToken");
  const ageSecs = Math.floor(Date.now() / 1000) - Number(claims.iat || 0);
  if (!(ageSecs >= -60 && ageSecs <= 3600)) throw new ApnsError(403, "ExpiredProviderToken");
  if (publicKey) {
    const ok = crypto.verify(
      "sha256",
      Buffer.from(`${parts[0]}.${parts[1]}`),
      { key: publicKey, dsaEncoding: "ieee-p1363" },
      b64urlDecode(parts[2]),
    );
    if (!ok) throw new ApnsError(403, "InvalidProviderToken");
  }
  return { kid: header.kid, iss: claims.iss, iat: claims.iat };
}

function parseJson(body, emptyReason = "PayloadEmpty") {
  if (!body.length) throw new ApnsError(400, emptyReason);
  try {
    return JSON.parse(body.toString("utf8"));
  } catch {
    throw new ApnsError(400, "BadPayload");
  }
}

function validateLiveActivityAps(json, { broadcast }) {
  const aps = json?.aps;
  if (!aps || typeof aps !== "object") throw new ApnsError(400, "BadPayload");
  if (!Number.isInteger(aps.timestamp) || aps.timestamp <= 0) throw new ApnsError(400, "BadPayload");
  if (!["start", "update", "end"].includes(aps.event)) throw new ApnsError(400, "BadPayload");
  if (broadcast && aps.event === "start") throw new ApnsError(400, "BadPayload");
  const state = aps["content-state"];
  if (!state || typeof state !== "object" || Array.isArray(state)) throw new ApnsError(400, "BadPayload");
  if (aps.event === "start") {
    if (typeof aps["attributes-type"] !== "string" || !aps["attributes-type"]) throw new ApnsError(400, "BadPayload");
    if (!aps.attributes || typeof aps.attributes !== "object") throw new ApnsError(400, "BadPayload");
    if (aps["input-push-token"] !== undefined && aps["input-push-channel"] !== undefined) {
      throw new ApnsError(400, "BadPayload");
    }
  }
  for (const key of ["stale-date", "dismissal-date"]) {
    if (aps[key] !== undefined && !Number.isInteger(aps[key])) throw new ApnsError(400, "BadPayload");
  }
  if (aps["relevance-score"] !== undefined && typeof aps["relevance-score"] !== "number") {
    throw new ApnsError(400, "BadPayload");
  }
}

function handleDevice(token, headers, body) {
  verifyProviderToken(headers);
  if (!/^[0-9a-f]{16,}$/i.test(token)) throw new ApnsError(400, "BadDeviceToken");
  if (!UUID.test(headers["apns-id"] || "")) throw new ApnsError(400, "BadMessageId");
  const pushType = headers["apns-push-type"];
  if (!pushType) throw new ApnsError(400, "MissingPushType");
  if (pushType === "liveactivity" && headers["apns-topic"] !== LIVE_ACTIVITY_TOPIC) {
    throw new ApnsError(400, "TopicDisallowed");
  }
  if (pushType !== "liveactivity" && headers["apns-topic"] !== BUNDLE_ID) throw new ApnsError(400, "BadTopic");
  if (!["5", "10"].includes(headers["apns-priority"])) throw new ApnsError(400, "BadPriority");
  if (body.length > MAX_DEVICE_BYTES) throw new ApnsError(413, "PayloadTooLarge");
  const json = parseJson(body);
  if (pushType === "liveactivity") validateLiveActivityAps(json, { broadcast: false });

  const lower = token.toLowerCase();
  if (lower.startsWith("bad0")) throw new ApnsError(400, "BadDeviceToken");
  if (lower.startsWith("4a0e")) throw new ApnsError(410, "Unregistered");
  if (lower.startsWith("429e") && !onceFailures.has(lower)) {
    onceFailures.add(lower);
    throw new ApnsError(429, "TooManyRequests", { "retry-after": "1" });
  }
  if (lower.startsWith("5005") && !onceFailures.has(lower)) {
    onceFailures.add(lower);
    throw new ApnsError(500, "InternalServerError");
  }
  return {
    status: 200,
    headers: { "apns-id": headers["apns-id"], "apns-unique-id": crypto.randomUUID() },
    body: "",
  };
}

function handleBroadcast(bundleId, headers, body) {
  verifyProviderToken(headers);
  if (bundleId !== BUNDLE_ID) throw new ApnsError(400, "BadTopic");
  if (!UUID.test(headers["apns-request-id"] || "")) throw new ApnsError(400, "BadRequestId");
  if (headers["apns-push-type"] !== "liveactivity") throw new ApnsError(400, "InvalidPushType");
  if (!["1", "5", "10"].includes(headers["apns-priority"])) throw new ApnsError(400, "BadPriority");
  const channelId = headers["apns-channel-id"];
  if (!channelId) throw new ApnsError(400, "MissingChannelId");
  const channel = channels.get(channelId);
  if (!channel) throw new ApnsError(400, "BadChannelId");
  if (!/^\d+$/.test(headers["apns-expiration"] || "")) throw new ApnsError(400, "BadExpirationDate");
  const expiration = Number(headers["apns-expiration"]);
  const now = Math.floor(Date.now() / 1000);
  if (channel.policy === 0 && expiration !== 0) throw new ApnsError(400, "BadExpirationDate");
  if (channel.policy === 1 && (expiration <= now || expiration > now + 8 * 3600)) {
    throw new ApnsError(400, "BadExpirationDate");
  }
  if (body.length > MAX_BROADCAST_BYTES) throw new ApnsError(413, "PayloadTooLarge");
  validateLiveActivityAps(parseJson(body), { broadcast: true });
  return {
    status: 200,
    headers: { "apns-request-id": headers["apns-request-id"], "apns-unique-id": crypto.randomUUID() },
    body: "",
  };
}

function handleManagement(bundleId, resource, method, headers, body) {
  verifyProviderToken(headers);
  if (bundleId !== BUNDLE_ID) throw new ApnsError(400, "BadTopic");
  const requestId = headers["apns-request-id"];
  if (!UUID.test(requestId || "")) throw new ApnsError(400, "BadRequestId");
  const echo = { "apns-request-id": requestId };
  if (resource === "all-channels" && method === "GET") {
    return { status: 200, headers: echo, body: JSON.stringify({ channels: [...channels.keys()] }) };
  }
  if (resource !== "channels") throw new ApnsError(404, "NotFound");
  if (method === "POST") {
    const json = parseJson(body);
    if (json["push-type"] !== "LiveActivity") throw new ApnsError(400, "BadPushType");
    if (![0, 1].includes(json["message-storage-policy"])) throw new ApnsError(400, "BadMessageStoragePolicy");
    if (channels.size >= 10_000) throw new ApnsError(400, "TooManyChannels");
    const channelId = crypto.randomBytes(32).toString("base64");
    channels.set(channelId, { policy: json["message-storage-policy"], createdAt: Date.now() });
    return { status: 201, headers: { ...echo, "apns-channel-id": channelId }, body: "" };
  }
  const channelId = headers["apns-channel-id"];
  if (!channelId) throw new ApnsError(400, "MissingChannelId");
  const channel = channels.get(channelId);
  if (!channel) throw new ApnsError(400, "BadChannelId");
  if (method === "GET") {
    return {
      status: 200,
      headers: { ...echo, "apns-channel-id": channelId },
      body: JSON.stringify({ "push-type": "LiveActivity", "message-storage-policy": channel.policy }),
    };
  }
  if (method === "DELETE") {
    channels.delete(channelId);
    return { status: 204, headers: echo, body: "" };
  }
  throw new ApnsError(405, "MethodNotAllowed");
}

function route(method, path, headers, body) {
  let match;
  if (method === "POST" && (match = path.match(/^\/3\/device\/([^/]+)$/))) {
    return handleDevice(match[1], headers, body);
  }
  if (method === "POST" && (match = path.match(/^\/4\/broadcasts\/apps\/([^/]+)$/))) {
    return handleBroadcast(match[1], headers, body);
  }
  if ((match = path.match(/^\/1\/apps\/([^/]+)\/(channels|all-channels)$/))) {
    return handleManagement(match[1], match[2], method, headers, body);
  }
  throw new ApnsError(404, "NotFound");
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

function redactHeaders(headers) {
  const out = {};
  for (const [name, value] of Object.entries(headers)) {
    if (name.startsWith(":")) continue;
    out[name] = name === "authorization" ? "bearer [redacted]" : value;
  }
  return out;
}

async function onApnsRequest(req, res) {
  const body = await readBody(req);
  const headers = req.headers;
  const path = req.url.split("?")[0];
  let response;
  let authorization = null;
  try {
    try {
      authorization = verifyProviderToken(headers);
    } catch (error) {
      authorization = { error: error.reason };
    }
    response = route(req.method, path, headers, body);
  } catch (error) {
    if (!(error instanceof ApnsError)) {
      console.error("mock failure", error);
      response = { status: 500, headers: {}, body: JSON.stringify({ reason: "InternalServerError" }) };
    } else {
      response = {
        status: error.status,
        headers: { ...error.extraHeaders, "apns-id": headers["apns-id"], "apns-request-id": headers["apns-request-id"] },
        body: JSON.stringify({ reason: error.reason }),
      };
    }
  }
  let parsedBody = null;
  if (body.length) {
    try {
      parsedBody = JSON.parse(body.toString("utf8"));
    } catch {
      parsedBody = body.toString("utf8");
    }
  }
  requests.push({
    seq: ++seq,
    at: new Date().toISOString(),
    httpVersion: req.httpVersion,
    method: req.method,
    path,
    headers: redactHeaders(headers),
    authorization,
    bodyBytes: body.length,
    body: parsedBody,
    status: response.status,
    responseHeaders: response.headers,
    responseBody: response.body ? JSON.parse(response.body) : null,
  });
  const responseHeaders = { "content-type": "application/json" };
  for (const [name, value] of Object.entries(response.headers)) {
    if (value !== undefined) responseHeaders[name] = value;
  }
  res.writeHead(response.status, responseHeaders);
  res.end(response.body);
}

const tls = {
  key: fs.readFileSync(process.env.TLS_KEY),
  cert: fs.readFileSync(process.env.TLS_CERT),
  allowHTTP1: true,
};
const apns = http2.createSecureServer(tls);
apns.on("request", (req, res) => {
  onApnsRequest(req, res).catch((error) => {
    console.error("request handler failed", error);
    res.writeHead(500);
    res.end();
  });
});
apns.listen(PORT, () => console.log(`mock apns listening on https://0.0.0.0:${PORT} for ${BUNDLE_ID}`));

const inspect = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  const send = (status, payload) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(payload));
  };
  if (url.pathname === "/_mock/health") return send(200, { ok: true, bundleId: BUNDLE_ID, channels: channels.size });
  if (url.pathname === "/_mock/requests" && req.method === "GET") {
    const since = Number(url.searchParams.get("since") || 0);
    return send(200, { requests: requests.filter((entry) => entry.seq > since) });
  }
  if (url.pathname === "/_mock/requests" && req.method === "DELETE") {
    requests.length = 0;
    onceFailures.clear();
    return send(200, { ok: true });
  }
  if (url.pathname === "/_mock/channels") {
    return send(200, { channels: [...channels.entries()].map(([id, meta]) => ({ id, ...meta })) });
  }
  send(404, { error: "not found" });
});
inspect.listen(INSPECT_PORT, () => console.log(`mock inspection listening on http://0.0.0.0:${INSPECT_PORT}`));
