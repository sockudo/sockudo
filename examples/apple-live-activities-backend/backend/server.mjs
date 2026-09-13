// Ride-tracking backend example.
//
// An iOS app uploads its ActivityKit tokens here (never to Sockudo directly), and this
// backend turns ride lifecycle events into Live Activity pushes through the Sockudo
// Node server SDK. Broadcast channels cover audiences that share content, such as
// everyone following the same event.
import http from "node:http";
import Sockudo from "sockudo";

const PORT = Number(process.env.BACKEND_PORT || 8787);
const sockudo = new Sockudo({
  appId: process.env.SOCKUDO_APP_ID || "rides-app",
  key: process.env.SOCKUDO_KEY || "rides-key",
  secret: process.env.SOCKUDO_SECRET || "rides-secret",
  host: process.env.SOCKUDO_HOST || "127.0.0.1",
  port: process.env.SOCKUDO_PORT || "6001",
  useTLS: false,
});

// Token registry. Production systems persist these and replace them on every rotation.
const pushToStartTokens = new Map(); // userId -> token
const activityTokens = new Map(); // rideId -> current update token
const rides = new Map(); // rideId -> { userId, sequence }
const lastPublished = new Map(); // rideId -> { publishId, event, contentState } (mirrored by the Simulator app)

const nowSecs = () => Math.floor(Date.now() / 1000);

async function publishRide(rideId, request) {
  const accepted = await sockudo.publishPush(request);
  lastPublished.set(rideId, {
    publishId: accepted.publishId,
    event: request.liveActivity.event,
    contentState: request.liveActivity.contentState,
  });
  return accepted;
}

function liveActivityRecipient(activityToken) {
  return { type: "recipient", recipient: { transportType: "apnsLiveActivity", activityToken } };
}

function broadcastRecipient(channelId, storagePolicy) {
  return {
    type: "recipient",
    recipient: { transportType: "apnsLiveActivityBroadcast", channelId, storagePolicy },
  };
}

async function startRide(rideId, { userId, contentState, channelId }) {
  const token = pushToStartTokens.get(userId);
  if (!token) throw httpError(409, `no push-to-start token for user ${userId}`);
  rides.set(rideId, { userId, sequence: 1 });
  return publishRide(rideId, {
    publishId: `ride-${rideId}-start`,
    recipients: [liveActivityRecipient(token)],
    payload: {},
    liveActivity: {
      event: "start",
      timestamp: nowSecs(),
      attributesType: "RideAttributes",
      attributes: { rideID: rideId },
      contentState,
      alert: { title: "Driver assigned", body: `ETA ${contentState.etaMinutes} min` },
      priority: "immediate",
      ...(channelId ? { inputPushChannel: channelId } : { inputPushToken: true }),
    },
  });
}

async function updateRide(rideId, { contentState, staleInSecs = 120, relevanceScore, priority }) {
  const ride = rides.get(rideId);
  const token = activityTokens.get(rideId);
  if (!ride || !token) throw httpError(409, `ride ${rideId} has no activity token`);
  ride.sequence += 1;
  return publishRide(rideId, {
    publishId: `ride-${rideId}-update-${ride.sequence}`,
    recipients: [liveActivityRecipient(token)],
    payload: {},
    liveActivity: {
      event: "update",
      timestamp: nowSecs(),
      contentState,
      staleDate: nowSecs() + staleInSecs,
      ...(relevanceScore !== undefined ? { relevanceScore } : {}),
      priority: priority || "conservePower",
    },
  });
}

async function endRide(rideId, { contentState, dismissalInSecs = 300 }) {
  const ride = rides.get(rideId);
  const token = activityTokens.get(rideId);
  if (!ride || !token) throw httpError(409, `ride ${rideId} has no activity token`);
  ride.sequence += 1;
  return publishRide(rideId, {
    publishId: `ride-${rideId}-end`,
    recipients: [liveActivityRecipient(token)],
    payload: {},
    liveActivity: {
      event: "end",
      timestamp: nowSecs(),
      contentState,
      dismissalDate: nowSecs() + dismissalInSecs,
      priority: "immediate",
    },
  });
}

async function broadcast(channelId, { storagePolicy, event, contentState, priority, expiresInSecs, publishId }) {
  const expiresAtMs = storagePolicy === "mostRecent" ? Date.now() + (expiresInSecs ?? 1800) * 1000 : undefined;
  return sockudo.publishPush({
    publishId,
    recipients: [broadcastRecipient(channelId, storagePolicy)],
    payload: {},
    ...(expiresAtMs ? { expiresAtMs } : {}),
    liveActivity: {
      event,
      timestamp: nowSecs(),
      contentState,
      priority: priority || "conservePower",
    },
  });
}

function httpError(status, message) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(httpError(400, "invalid JSON body"));
      }
    });
    req.on("error", reject);
  });
}

const routes = [
  ["POST", /^\/devices\/push-to-start$/, async (_, body) => {
    pushToStartTokens.set(body.userId, body.token);
    return { ok: true };
  }],
  ["POST", /^\/rides\/([^/]+)\/token$/, async ([rideId], body) => {
    activityTokens.set(rideId, body.token);
    // Activities started on-device (Activity.request) are registered on first token upload.
    if (!rides.has(rideId)) rides.set(rideId, { userId: body.userId || null, sequence: 1 });
    return { ok: true };
  }],
  // sockudo_flutter `ApnsLiveActivityTokenUpdate.toJson()` uploads: {kind, token, activityId?} plus
  // the app's own userId / rideId context.
  ["POST", /^\/liveActivities\/tokens$/, async (_, body) => {
    if (body.kind === "pushToStart") {
      pushToStartTokens.set(body.userId, body.token);
    } else if (body.kind === "update") {
      const rideId = body.rideId || body.activityId;
      activityTokens.set(rideId, body.token);
      if (!rides.has(rideId)) rides.set(rideId, { userId: body.userId || null, sequence: 1 });
    } else {
      throw httpError(400, `unknown token kind ${body.kind}`);
    }
    return { ok: true };
  }],
  ["GET", /^\/devices\/push-to-start\/([^/]+)$/, async ([userId]) => ({ token: pushToStartTokens.get(userId) || null })],
  ["GET", /^\/rides\/([^/]+)\/token$/, async ([rideId]) => ({ token: activityTokens.get(rideId) || null })],
  ["GET", /^\/rides\/([^/]+)\/latest$/, async ([rideId]) => lastPublished.get(rideId) || {}],
  ["POST", /^\/rides\/([^/]+)\/start$/, ([rideId], body) => startRide(rideId, body)],
  ["POST", /^\/rides\/([^/]+)\/update$/, ([rideId], body) => updateRide(rideId, body)],
  ["POST", /^\/rides\/([^/]+)\/end$/, ([rideId], body) => endRide(rideId, body)],
  ["POST", /^\/broadcasts\/channels$/, (_, body) => sockudo.createApnsLiveActivityChannel(body.storagePolicy || "noStorage")],
  ["GET", /^\/broadcasts\/channels$/, () => sockudo.listApnsLiveActivityChannels()],
  ["GET", /^\/broadcasts\/channels\/([^/]+)$/, ([id]) => sockudo.getApnsLiveActivityChannel(decodeURIComponent(id))],
  ["DELETE", /^\/broadcasts\/channels\/([^/]+)$/, async ([id]) => {
    await sockudo.deleteApnsLiveActivityChannel(decodeURIComponent(id));
    return { ok: true };
  }],
  ["POST", /^\/broadcasts\/channels\/([^/]+)\/publish$/, ([id], body) => broadcast(decodeURIComponent(id), body)],
  ["GET", /^\/publishes\/([^/]+)$/, ([publishId]) => sockudo.getPublishStatus(decodeURIComponent(publishId))],
  // Push proxy surface for mobile SDK helpers (sockudo_flutter `PushRegistrationOptions.endpoint`).
  // A real backend authenticates the caller and restricts publishes to that user's activities;
  // this example forwards the validated request with the server credentials.
  ["POST", /^\/push\/publish$/, async (_, body) => {
    const accepted = await sockudo.publishPush(body);
    // Record the payload for rides whose activity token we know (Simulator mirroring).
    const token = body.recipients?.[0]?.recipient?.activityToken;
    const rideId = token && [...activityTokens.entries()].find(([, t]) => t === token)?.[0];
    if (rideId && body.liveActivity) {
      lastPublished.set(rideId, { publishId: accepted.publishId, event: body.liveActivity.event, contentState: body.liveActivity.contentState });
    }
    return accepted;
  }],
  ["GET", /^\/push\/publish\/([^/]+)\/status$/, ([publishId]) => sockudo.getPublishStatus(decodeURIComponent(publishId))],
  ["POST", /^\/push\/liveActivities\/channels$/, (_, body) => sockudo.createApnsLiveActivityChannel(body.storagePolicy || "noStorage")],
  ["DELETE", /^\/push\/liveActivities\/channels\/([^/]+)$/, async ([id]) => {
    await sockudo.deleteApnsLiveActivityChannel(decodeURIComponent(id));
    return { ok: true };
  }],
];

const server = http.createServer(async (req, res) => {
  const path = req.url.split("?")[0];
  const send = (status, payload) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(payload));
  };
  try {
    for (const [method, pattern, handler] of routes) {
      const match = method === req.method && path.match(pattern);
      if (!match) continue;
      const body = req.method === "GET" || req.method === "DELETE" ? {} : await readJson(req);
      return send(200, await handler(match.slice(1), body));
    }
    send(404, { error: "not found" });
  } catch (error) {
    // The SDK throws RequestError with status/body for Sockudo API rejections.
    const status = error.status || 502;
    let body = error.body;
    if (typeof body === "string") {
      try {
        body = JSON.parse(body);
      } catch {
        // keep raw string
      }
    }
    send(status, { error: error.message, status, body });
  }
});

server.listen(PORT, () => console.log(`rides backend listening on http://127.0.0.1:${PORT}`));
