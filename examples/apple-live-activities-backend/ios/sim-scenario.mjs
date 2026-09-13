// Drives the Live Activity shown in the iOS Simulator through the backend.
//
// Default (mock APNs): tap "Start locally" in the app first. The app uploads the activity's
// real ActivityKit update token; this script publishes an update and an end for it through
// Sockudo and verifies the mock accepted both. The Simulator cannot receive Live Activity
// pushes from a mock: `xcrun simctl push` only emulates application notifications and never
// reaches liveactivitiesd.
//
// MODE=sandbox (real Apple sandbox APNs credentials in Sockudo): also exercises push-to-start,
// waits for the new activity's token, and the Simulator screen updates for real.
const BACKEND = process.env.BACKEND_URL || "http://127.0.0.1:8787";
const MODE = process.env.MODE || "mock";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(method, path, body) {
  const response = await fetch(`${BACKEND}${path}`, {
    method, headers: { "content-type": "application/json" }, body: body && JSON.stringify(body),
  });
  const json = await response.json();
  if (!response.ok) throw new Error(`${method} ${path} -> ${response.status} ${JSON.stringify(json)}`);
  return json;
}
async function waitFor(label, fn, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value) { console.log(`✓ ${label}`); return value; }
    await sleep(500);
  }
  throw new Error(`timed out waiting for ${label}`);
}
const delivered = (publishId) => async () => {
  const s = await api("GET", `/publishes/${publishId}`);
  if (s.counters?.failed) throw new Error(`${publishId} failed: ${JSON.stringify(s)}`);
  return s.counters?.succeeded === 1 ? s : null;
};

let rideId = process.env.RIDE_ID || "local-1";
if (MODE === "sandbox") {
  rideId = `sim-${Date.now().toString(36)}`;
  await waitFor("app uploaded a push-to-start token", async () => (await api("GET", "/devices/push-to-start/sim-user")).token);
  const start = await api("POST", `/rides/${rideId}/start`, { userId: "sim-user", contentState: { status: "driverAssigned", etaMinutes: 6 } });
  await waitFor("start accepted by APNs", delivered(start.publishId));
}
console.log(`ride ${rideId}: waiting for the app to upload its activity token (tap "Start locally" in mock mode)`);
await waitFor("activity update token uploaded", async () => (await api("GET", `/rides/${rideId}/token`)).token);
const update = await api("POST", `/rides/${rideId}/update`, { contentState: { status: "arriving", etaMinutes: 2 }, relevanceScore: 0.9 });
await waitFor("update accepted by APNs", delivered(update.publishId));
await sleep(3000);
const end = await api("POST", `/rides/${rideId}/end`, { contentState: { status: "completed", etaMinutes: 0 }, dismissalInSecs: 600 });
await waitFor("end accepted by APNs", delivered(end.publishId));
console.log(MODE === "sandbox" ? "done; the Live Activity on the Simulator should now read completed" : "done; with real sandbox credentials (MODE=sandbox) the Simulator screen would update");
