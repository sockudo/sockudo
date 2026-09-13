// End-to-end scenario: plays the iOS app and the ride dispatcher against the backend, then
// verifies every request Sockudo made to the mock APNs. Exits non-zero on any failed check.
import Sockudo from "sockudo";

const BACKEND = process.env.BACKEND_URL || "http://127.0.0.1:8787";
const MOCK = process.env.MOCK_INSPECT_URL || "http://127.0.0.1:8444";
const BUNDLE_ID = process.env.APNS_MOCK_BUNDLE_ID || "com.sockudo.rides";
const LIVE_ACTIVITY_TOPIC = `${BUNDLE_ID}.push-type.liveactivity`;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// Direct SDK client for contract probes that a real backend would never send.
const sockudo = new Sockudo({
  appId: process.env.SOCKUDO_APP_ID || "rides-app",
  key: process.env.SOCKUDO_KEY || "rides-key",
  secret: process.env.SOCKUDO_SECRET || "rides-secret",
  host: process.env.SOCKUDO_HOST || "127.0.0.1",
  port: process.env.SOCKUDO_PORT || "6001",
  useTLS: false,
});

const results = [];
function check(name, condition, detail) {
  results.push({ name, ok: Boolean(condition), detail });
  const marker = condition ? "PASS" : "FAIL";
  console.log(`${marker}  ${name}${condition ? "" : `  -> ${JSON.stringify(detail)}`}`);
}

const hexToken = (prefix) =>
  (prefix + "0123456789abcdef".repeat(4)).slice(0, 64);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const nowSecs = () => Math.floor(Date.now() / 1000);

async function api(method, path, body) {
  const response = await fetch(`${BACKEND}${path}`, {
    method,
    headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const json = await response.json();
  if (!response.ok) throw Object.assign(new Error(`${method} ${path} -> ${response.status}`), { status: response.status, body: json });
  return json;
}

async function mockRequests(since = 0) {
  const response = await fetch(`${MOCK}/_mock/requests?since=${since}`);
  return (await response.json()).requests;
}
async function resetMock() {
  await fetch(`${MOCK}/_mock/requests`, { method: "DELETE" });
}

async function waitForStatus(publishId, predicate, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  let status;
  while (Date.now() < deadline) {
    try {
      status = await api("GET", `/publishes/${encodeURIComponent(publishId)}`);
    } catch (error) {
      if (error.status !== 429) throw error; // API rate limit: back off and retry
      await sleep(1000);
      continue;
    }
    if (predicate(status)) return status;
    await sleep(400);
  }
  return status;
}
const settled = (expected = 1) => (status) => {
  const c = status.counters || {};
  return (c.succeeded || 0) + (c.failed || 0) + (c.expired || 0) + (c.deadLettered || 0) >= expected;
};
const retryScheduled = (status) => (status.counters?.retryScheduled || 0) >= 1;

function findRequest(list, predicate) {
  return list.find(predicate);
}

async function expectRejected(name, promise, expectedStatus = 400) {
  try {
    await promise;
    check(name, false, "request unexpectedly succeeded");
  } catch (error) {
    let body = error.body;
    if (typeof body === "string") {
      try {
        body = JSON.parse(body);
      } catch {
        // raw
      }
    }
    check(name, error.status === expectedStatus, { status: error.status, body });
    return body;
  }
}

async function main() {
  await resetMock();
  const userId = "user-7";
  const rideId = "ride-184";
  const pushToStartToken = hexToken("a11ce");
  const activityToken = hexToken("ac71");

  // --- Broadcast channel lifecycle -------------------------------------------------------
  console.log("\n# Broadcast channel management");
  const channel = await api("POST", "/broadcasts/channels", { storagePolicy: "mostRecent" });
  check("create channel returns id + policy", channel.channelId && channel.storagePolicy === "mostRecent", channel);
  const fetched = await api("GET", `/broadcasts/channels/${encodeURIComponent(channel.channelId)}`);
  check("get channel reflects APNs policy", fetched.channelId === channel.channelId && fetched.storagePolicy === "mostRecent", fetched);
  const noStorage = await api("POST", "/broadcasts/channels", { storagePolicy: "noStorage" });
  check("create noStorage channel", noStorage.storagePolicy === "noStorage", noStorage);
  const listed = await api("GET", "/broadcasts/channels");
  check("list channels contains both", listed.channels.includes(channel.channelId) && listed.channels.includes(noStorage.channelId), listed);
  let seen = await mockRequests();
  const createReq = seen.find((r) => r.method === "POST" && r.path === `/1/apps/${BUNDLE_ID}/channels`);
  check("channel create wire: HTTP/2, JWT verified, LiveActivity push-type, policy 1",
    createReq && createReq.httpVersion === "2.0" && createReq.authorization?.kid === "MOCKKEY001"
      && createReq.body?.["push-type"] === "LiveActivity" && createReq.body?.["message-storage-policy"] === 1
      && UUID.test(createReq.headers["apns-request-id"]), createReq);
  const getReq = seen.find((r) => r.method === "GET" && r.path.endsWith("/channels"));
  check("channel get wire uses apns-channel-id header", getReq && getReq.headers["apns-channel-id"] === channel.channelId, getReq);
  check("channel list wire hits all-channels", seen.some((r) => r.method === "GET" && r.path.endsWith("/all-channels")), seen.map((r) => r.path));

  // --- Direct ActivityKit lifecycle ------------------------------------------------------
  console.log("\n# Direct token lifecycle (start -> update -> end)");
  await resetMock();
  await api("POST", "/devices/push-to-start", { userId, token: pushToStartToken });
  const start = await api("POST", `/rides/${rideId}/start`, {
    userId,
    contentState: { status: "driverAssigned", etaMinutes: 4 },
    channelId: channel.channelId,
  });
  check("start accepted (202-style admission)", start.publishId === `ride-${rideId}-start` && start.expectedRecipients === 1, start);
  let status = await waitForStatus(start.publishId, settled());
  check("start delivered", status.counters?.succeeded === 1 && status.counters?.failed === 0, status);
  seen = await mockRequests();
  const startReq = seen.find((r) => r.path === `/3/device/${pushToStartToken}`);
  check("start wire headers: topic/push-type/priority 10/uuid apns-id",
    startReq && startReq.headers["apns-topic"] === LIVE_ACTIVITY_TOPIC && startReq.headers["apns-push-type"] === "liveactivity"
      && startReq.headers["apns-priority"] === "10" && UUID.test(startReq.headers["apns-id"]) && startReq.httpVersion === "2.0", startReq?.headers);
  const startAps = startReq?.body?.aps;
  check("start wire aps: event/timestamp/content-state/attributes/alert/input-push-channel",
    startAps && startAps.event === "start" && Number.isInteger(startAps.timestamp)
      && startAps["content-state"]?.etaMinutes === 4 && startAps["attributes-type"] === "RideAttributes"
      && startAps.attributes?.rideID === rideId && startAps.alert?.title === "Driver assigned"
      && startAps["input-push-channel"] === channel.channelId && startAps["input-push-token"] === undefined, startAps);

  // The app receives the activity's update token and uploads it.
  await api("POST", `/rides/${rideId}/token`, { token: activityToken });
  const update = await api("POST", `/rides/${rideId}/update`, {
    contentState: { status: "arriving", etaMinutes: 1 },
    staleInSecs: 120,
    relevanceScore: 0.9,
  });
  status = await waitForStatus(update.publishId, settled());
  check("update delivered", status.counters?.succeeded === 1, status);
  seen = await mockRequests();
  const updateReq = seen.find((r) => r.path === `/3/device/${activityToken}` && r.body?.aps?.event === "update");
  check("update wire: priority 5, stale-date, relevance-score",
    updateReq && updateReq.headers["apns-priority"] === "5" && updateReq.body.aps["stale-date"] > nowSecs()
      && updateReq.body.aps["relevance-score"] === 0.9 && updateReq.body.aps["content-state"].status === "arriving", updateReq?.body);

  // Idempotent re-publish of the same publishId must not produce a second APNs request.
  const before = (await mockRequests()).length;
  const replay = await sockudo.publishPush({
    publishId: update.publishId,
    recipients: [{ type: "recipient", recipient: { transportType: "apnsLiveActivity", activityToken } }],
    payload: {},
    liveActivity: { event: "update", timestamp: nowSecs(), contentState: { status: "arriving", etaMinutes: 1 } },
  });
  await sleep(1500);
  const after = (await mockRequests()).length;
  check("re-publishing same publishId is idempotent (no second APNs request)", replay.publishId === update.publishId && after === before, { before, after, replay });

  const end = await api("POST", `/rides/${rideId}/end`, { contentState: { status: "completed", etaMinutes: 0 }, dismissalInSecs: 300 });
  status = await waitForStatus(end.publishId, settled());
  check("end delivered", status.counters?.succeeded === 1, status);
  seen = await mockRequests();
  const endReq = seen.find((r) => r.body?.aps?.event === "end");
  check("end wire: dismissal-date + priority 10", endReq && endReq.body.aps["dismissal-date"] > nowSecs() && endReq.headers["apns-priority"] === "10", endReq?.body);
  check("apns-id is unique per publish", new Set([startReq, updateReq, endReq].map((r) => r?.headers["apns-id"])).size === 3);

  // --- Broadcast delivery ------------------------------------------------------------------
  console.log("\n# Broadcast delivery");
  await resetMock();
  const expiresInSecs = 1800;
  const bcast = await api("POST", `/broadcasts/channels/${encodeURIComponent(channel.channelId)}/publish`, {
    publishId: "match-90-score-3-1",
    storagePolicy: "mostRecent",
    event: "update",
    contentState: { home: 3, away: 1 },
    priority: "lowPower",
    expiresInSecs,
  });
  check("broadcast accepted with expectedRecipients 1", bcast.expectedRecipients === 1, bcast);
  status = await waitForStatus(bcast.publishId, settled());
  check("broadcast delivered", status.counters?.succeeded === 1, status);
  seen = await mockRequests();
  const bReq = seen.find((r) => r.path === `/4/broadcasts/apps/${BUNDLE_ID}`);
  const expiration = Number(bReq?.headers["apns-expiration"]);
  check("broadcast wire: channel id, uuid request id, priority 1, expiration ~ expiresAtMs",
    bReq && bReq.headers["apns-channel-id"] === channel.channelId && UUID.test(bReq.headers["apns-request-id"])
      && bReq.headers["apns-priority"] === "1" && bReq.headers["apns-push-type"] === "liveactivity"
      && Math.abs(expiration - (nowSecs() + expiresInSecs)) < 30 && bReq.body.aps["content-state"].home === 3, bReq?.headers);

  const bNo = await api("POST", `/broadcasts/channels/${encodeURIComponent(noStorage.channelId)}/publish`, {
    publishId: "match-90-final",
    storagePolicy: "noStorage",
    event: "end",
    contentState: { home: 3, away: 1, final: true },
    priority: "conservePower",
  });
  status = await waitForStatus(bNo.publishId, settled());
  check("noStorage broadcast end delivered", status.counters?.succeeded === 1, status);
  seen = await mockRequests();
  const bNoReq = seen.find((r) => r.headers["apns-channel-id"] === noStorage.channelId);
  check("noStorage broadcast sends apns-expiration 0 and priority 5", bNoReq && bNoReq.headers["apns-expiration"] === "0" && bNoReq.headers["apns-priority"] === "5", bNoReq?.headers);

  // --- Admission-time validation (rejected before APNs) --------------------------------
  console.log("\n# Admission validation (must be rejected with 400, never reach APNs)");
  await resetMock();
  const direct = (activity) => sockudo.publishPush({
    recipients: [{ type: "recipient", recipient: { transportType: "apnsLiveActivity", activityToken } }],
    payload: {},
    liveActivity: activity,
  });
  await expectRejected("lowPower rejected for direct token", direct({ event: "update", timestamp: nowSecs(), contentState: {}, priority: "lowPower" }));
  await expectRejected("inputPushToken + inputPushChannel rejected", direct({
    event: "start", timestamp: nowSecs(), contentState: {}, attributesType: "RideAttributes", attributes: {}, inputPushToken: true, inputPushChannel: channel.channelId,
  }));
  await expectRejected("start without attributesType rejected", direct({ event: "start", timestamp: nowSecs(), contentState: {} }));
  await expectRejected("timestamp 0 rejected", direct({ event: "update", timestamp: 0, contentState: {} }));
  await expectRejected("non-object contentState rejected", direct({ event: "update", timestamp: nowSecs(), contentState: [1, 2] }));
  await expectRejected("liveActivity recipient without liveActivity body rejected", sockudo.publishPush({
    recipients: [{ type: "recipient", recipient: { transportType: "apnsLiveActivity", activityToken } }],
    payload: { title: "plain alert" },
  }));
  await expectRejected("broadcast start rejected", sockudo.publishPush({
    recipients: [{ type: "recipient", recipient: { transportType: "apnsLiveActivityBroadcast", channelId: channel.channelId, storagePolicy: "mostRecent" } }],
    payload: {},
    liveActivity: { event: "start", timestamp: nowSecs(), contentState: {}, attributesType: "X", attributes: {} },
  }));
  await expectRejected("channel create with bogus policy rejected", sockudo.createApnsLiveActivityChannel("forever"));
  check("no APNs traffic from rejected publishes", (await mockRequests()).length === 0);

  // --- Provider failure handling ---------------------------------------------------------
  console.log("\n# Provider failure contract");
  await resetMock();
  const publishTo = (token, publishId, extra = {}) => sockudo.publishPush({
    publishId,
    recipients: [{ type: "recipient", recipient: { transportType: "apnsLiveActivity", activityToken: token } }],
    payload: {},
    liveActivity: { event: "update", timestamp: nowSecs(), contentState: { status: "x" }, ...extra },
  });
  const badToken = hexToken("bad0");
  await publishTo(badToken, "fail-bad-token");
  status = await waitForStatus("fail-bad-token", settled());
  check("400 BadDeviceToken is terminal (failed=1, no retry)", status.counters?.failed === 1 && !(status.counters?.retryScheduled > 0), status);

  const goneToken = hexToken("4a0e");
  await publishTo(goneToken, "fail-unregistered");
  status = await waitForStatus("fail-unregistered", settled());
  check("410 Unregistered is terminal", status.counters?.failed === 1, status);

  const throttled = hexToken("429e");
  await publishTo(throttled, "retry-429");
  status = await waitForStatus("retry-429", (s) => s.counters?.succeeded === 1, 20_000);
  seen = await mockRequests();
  const throttledHits = seen.filter((r) => r.path === `/3/device/${throttled}`);
  check("429 with Retry-After is retried and then succeeds", status.counters?.succeeded === 1 && status.counters?.retryScheduled >= 1 && throttledHits.length === 2,
    { counters: status.counters, statuses: throttledHits.map((r) => r.status) });
  check("retry reuses the same deterministic apns-id", throttledHits.length === 2 && throttledHits[0].headers["apns-id"] === throttledHits[1].headers["apns-id"],
    throttledHits.map((r) => r.headers["apns-id"]));

  const flaky = hexToken("5005");
  await publishTo(flaky, "retry-500");
  status = await waitForStatus("retry-500", retryScheduled);
  const holdMs = (status.retryAfterMs || 0) - Date.now();
  check("500 schedules a retry held for Apple's 15-minute delay", status.counters?.retryScheduled >= 1 && holdMs > 14 * 60_000 && holdMs <= 15 * 60_000 + 5_000,
    { counters: status.counters, retryAfterMs: status.retryAfterMs, holdMs });

  const oversize = hexToken("0ff5");
  await publishTo(oversize, "fail-oversize", { contentState: { blob: "x".repeat(4200) } });
  status = await waitForStatus("fail-oversize", settled());
  seen = await mockRequests();
  check("oversized direct payload fails before reaching APNs", status.counters?.failed === 1 && !seen.some((r) => r.path === `/3/device/${oversize}`), status);

  const deletedChannel = await api("POST", "/broadcasts/channels", { storagePolicy: "noStorage" });
  await api("DELETE", `/broadcasts/channels/${encodeURIComponent(deletedChannel.channelId)}`);
  await sockudo.publishPush({
    publishId: "fail-deleted-channel",
    recipients: [{ type: "recipient", recipient: { transportType: "apnsLiveActivityBroadcast", channelId: deletedChannel.channelId, storagePolicy: "noStorage" } }],
    payload: {},
    liveActivity: { event: "update", timestamp: nowSecs(), contentState: { a: 1 } },
  });
  status = await waitForStatus("fail-deleted-channel", settled());
  check("broadcast to deleted channel (400 BadChannelId) is terminal", status.counters?.failed === 1 && !(status.counters?.retryScheduled > 0), status);

  // --- Cleanup -----------------------------------------------------------------------------
  console.log("\n# Cleanup");
  await resetMock();
  await api("DELETE", `/broadcasts/channels/${encodeURIComponent(channel.channelId)}`);
  await api("DELETE", `/broadcasts/channels/${encodeURIComponent(noStorage.channelId)}`);
  const remaining = await api("GET", "/broadcasts/channels");
  seen = await mockRequests();
  check("delete channels hits APNs DELETE and inventory is empty", seen.filter((r) => r.method === "DELETE").length === 2 && remaining.channels.length === 0, { remaining, seen: seen.map((r) => [r.method, r.path, r.status]) });
  const missing = await expectRejected("get deleted channel surfaces provider error", api("GET", `/broadcasts/channels/${encodeURIComponent(channel.channelId)}`), 400);
  console.log("   provider error body:", JSON.stringify(missing));

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} checks passed`);
  if (failed.length) process.exit(1);
}

main().catch((error) => {
  console.error("scenario crashed:", error);
  process.exit(2);
});
