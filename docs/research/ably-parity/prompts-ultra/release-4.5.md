# Release 4.5 Ultra Prompt Pack — Push Notification Platform

Research binding context for every prompt in this pack:
- `docs/research/ably-parity/ably-push-notifications-research.md`
- `docs/research/ably-parity/sockudo-ably-parity-release-plan.md`

---

## Prompt 4.5-01: Architecture and release contract
```text
Implement Sockudo release 4.5: Push Notification Platform.

Before writing any code, produce the release contract and commit it to the repo.

Parity target:
- Ably Push delivers to devices via FCM and APNs, and to browsers via Web Push.
- Ably supports two activation models (direct and server-assisted) and two delivery models
  (direct publish and channel-based publish).
- Sockudo parity is not "send JSON to FCM". It includes the full activation lifecycle,
  device registry, channel subscription model, payload transformation layer,
  admin APIs, per-device state tracking, and operator inspection surface.

Provider scope:
- FCM (Firebase Cloud Messaging) via service account JSON
- APNs (Apple Push Notification service) via .p12 or PEM credentials
- Web Push via VAPID keys

Delivery models:
- Direct publish: targets by deviceId, clientId, or recipient attributes (transportType + deviceToken)
- Channel-based publish: devices/browsers subscribed to a channel receive push when that channel
  publishes a message with `extras.push` payload

Activation models:
- Direct activation: client registers with the push platform and registers with Sockudo directly
- Server-assisted activation: client sends token to app server; app server registers with Sockudo
  via Push Admin API (lower-trust client path)

Device/browser lifecycle states:
- ACTIVE: registered and receiving
- FAILING: delivery failures detected
- FAILED: permanently failed; operator must inspect and clean

Admin API surfaces:
- Device registrations: get, list (filterable by clientId, deviceId, state), save, remove, removeWhere
- Channel subscriptions: list, listChannels, save, remove, removeWhere

Release blockers:
- End-to-end delivery must be verified for at least one provider (FCM or APNs) before shipping.
- Admin registry APIs must be complete and returning correct device state.
- Channel push must work independently of whether any realtime subscriber is present.
- Per-device failure state must be tracked and observable.
- Payload transformation layer must be implemented (not pass-through JSON).

Non-goals:
- No dashboard UI in this release.
```

---

## Prompt 4.5-02: Domain model and invariants
```text
Design and implement the push domain model for release 4.5.

DeviceDetails entity:
- `id`: unique device identifier (stable across activations if same physical device)
- `clientId`: optional — the identified Sockudo user associated with this device
- `formFactor`: string enum (phone, tablet, desktop, tv, car, watch, embedded, other)
- `platform`: string enum (android, ios, browser)
- `metadata`: arbitrary JSON for operator use
- `deviceSecret`: opaque credential used to authenticate device-to-server updates
- `push.recipient`: provider-specific address data
  - for FCM: `{ transportType: "gcm", registrationToken: "<fcm_token>" }`
  - for APNs: `{ transportType: "apns", deviceToken: "<apns_hex_token>" }`
  - for Web Push: `{ transportType: "web", endpoint: "<push_endpoint>", p256dh: "<key>", auth: "<auth>" }`
- `push.state`: one of ACTIVE, FAILING, FAILED
- `push.errorReason`: last known error message if FAILING or FAILED

Channel subscription entity:
- `channel`: channel name
- `deviceId`: device this subscription belongs to
- `clientId`: optional, for client-level subscriptions

Invariants:
- A device may only have one active registration per (platform, push.recipient.transportType) combination.
  Re-registering with the same details is idempotent.
- Deleting a non-existent device is treated as success (not 404).
- A device subscription to a channel is separate from an ordinary realtime channel subscription.
  A device can receive channel push without having an active WebSocket connection.
- Changing a device token (e.g. FCM token rotation) requires updating the registration, not creating a new one.
- Per-device state transitions: ACTIVE → FAILING (on first delivery failure) → FAILED (after N failures or explicit operator action).
  Delivery success from FAILING state resets to ACTIVE.

Provider credential entity (per app):
- `fcm_credentials`: serialized service account JSON (stored encrypted)
- `apns_credentials`: certificate/key material (.p12 or PEM, stored encrypted)
- `vapid_keys`: VAPID public/private key pair for Web Push (generated per app on first use)
```

---

## Prompt 4.5-03: Storage schema, migrations, and backfill
```text
Design and implement storage for registrations, subscriptions, provider credentials, and failure state.

Device registrations table:
- `device_id` PK
- `app_id` FK
- `client_id` nullable
- `platform` string
- `form_factor` string
- `metadata` JSON
- `device_secret` string (hashed)
- `recipient_transport_type` string
- `recipient_data` JSON (endpoint, token, keys — provider-specific)
- `push_state` enum
- `push_error` nullable string
- `created_at`, `updated_at`

Indexes:
- `(app_id, client_id)` — for list-by-client queries
- `(app_id, push_state)` — for admin monitoring queries
- `(app_id, recipient_transport_type)` — for provider-targeted batch operations

Channel push subscriptions table:
- `app_id`
- `channel`
- `device_id` nullable
- `client_id` nullable
- (at least one of device_id or client_id must be set)
- `created_at`

Indexes:
- `(app_id, channel)` — for listing subscriptions for a channel
- `(app_id, device_id)` — for listing channels a device is subscribed to
- `(app_id, client_id)` — for listing channels a client is subscribed to

Provider credentials table (per app):
- `app_id` PK
- `fcm_credentials` encrypted blob nullable
- `apns_cert_type` enum (p12, pem) nullable
- `apns_cert_data` encrypted blob nullable
- `apns_key_data` encrypted blob nullable
- `vapid_public_key` string nullable
- `vapid_private_key` encrypted string nullable
- `updated_at`

Migration strategy:
- All push tables are new; no existing tables change.
- Add migrations to `ops/migrations/` for MySQL and PostgreSQL.
- Memory backend: in-process maps.
- ScyllaDB/DynamoDB/SurrealDB: add equivalent schema.

Rollback: drop all push tables. No impact on message history or channel state.

Credential storage security:
- Provider credentials (FCM service account, APNs keys, VAPID private key) must never be stored in plaintext.
- Use per-app encryption keyed to an operator-supplied secret.
- Document the key management requirement in operator guidance.
```

---

## Prompt 4.5-04: Protocol, API, and payload contract
```text
Implement the HTTP API for push administration and publishing.

Provider credential management:
POST /apps/{app_id}/push/credentials/fcm  (body: { serviceAccountJson })
POST /apps/{app_id}/push/credentials/apns (body: { certType, certData, keyData })
POST /apps/{app_id}/push/credentials/webpush (generates VAPID pair if not present)
GET  /apps/{app_id}/push/credentials      (returns provider status without secret material)

Device registration API:
POST   /apps/{app_id}/push/deviceRegistrations       save/upsert a device
GET    /apps/{app_id}/push/deviceRegistrations/{id}   get a device
GET    /apps/{app_id}/push/deviceRegistrations        list (query: clientId?, deviceId?, state?)
DELETE /apps/{app_id}/push/deviceRegistrations/{id}   remove a device
DELETE /apps/{app_id}/push/deviceRegistrations        removeWhere (query: clientId?, deviceId?)

Channel subscription API:
POST   /apps/{app_id}/push/channelSubscriptions                         save subscription
GET    /apps/{app_id}/push/channelSubscriptions                         list (query: channel?, clientId?, deviceId?)
GET    /apps/{app_id}/push/channelSubscriptions/channels                listChannels (query: clientId?, deviceId?)
DELETE /apps/{app_id}/push/channelSubscriptions                         removeWhere

Direct publish API:
POST /apps/{app_id}/push/publish
Body:
{
  "recipient": {
    "deviceId": "dev-123"
    // OR "clientId": "user-456"
    // OR { "transportType": "apns", "deviceToken": "..." }
  },
  "notification": {
    "title": "Hello",
    "body": "World",
    "icon": null,
    "sound": null,
    "collapseKey": null
  },
  "data": { "key": "value" },
  "apns": { ... },   // provider-specific overrides
  "fcm": { ... },
  "web": { ... }
}

Batch publish API:
POST /apps/{app_id}/push/batch/publish
Body: array of publish payloads (same shape as single publish)
Returns: array of per-item results (success/failure)

Channel push (via message extras):
- When a message is published to a channel with `extras.push` set, and the channel has push enabled,
  Sockudo looks up all push-subscribed devices/browsers for that channel and dispatches notifications.
- The push payload in `extras.push` follows the same notification/data/override structure.

Generic-to-provider payload mapping:
  FCM:
    notification.title → notification.title
    notification.body  → notification.body
    notification.icon  → notification.icon
    notification.sound → notification.sound
    notification.collapseKey → android.collapseKey
    data → data (root level)

  APNs:
    notification.title → aps.alert.title
    notification.body  → aps.alert.body
    notification.sound → aps.sound
    data → payload root level
    apns-headers: support apns-push-type and apns-priority for background notifications

  Web Push:
    notification.title → notification.title
    notification.body  → notification.body
    notification.icon  → notification.icon
    data → notification.data

Provider overrides take precedence over mapped generic fields.
```

---

## Prompt 4.5-05: Auth, capabilities, and security
```text
Implement authorization for push administration and self-service device registration.

Capability model:

`push-admin`:
  - Full access to all device registration and channel subscription admin APIs.
  - Required for: save/remove/list any device, save/remove/list any channel subscription, direct publish.
  - Server-to-server operations use this.

`push-subscribe`:
  - Restricted self-service: a client may manage only their own device and their own subscriptions.
  - The device's clientId must match the authenticated caller's clientId.
  - A client with push-subscribe cannot see or modify other clients' registrations.

Authentication for device-to-server update operations:
- After direct activation, the device holds a `deviceIdentityToken` issued by Sockudo.
- Subsequent device updates (token rotation, state update) authenticate via:
  Option A: Ably-style: Sockudo token embedding the deviceId in claims
  Option B: deviceIdentityToken in a request header (simpler, implement this first)
- Document both paths; implement at least Option B.

Channel push capability:
- Publishing a message with `extras.push` requires publish capability on the channel.
- The push dispatch is then server-side; no additional push capability needed on the publisher.

Security requirements:
- Provider credentials (FCM service account JSON, APNs keys, VAPID private key) must never
  appear in API responses. Credential GET endpoints return status metadata only.
- Device secrets must be stored hashed, not plaintext.
- `removeWhere` with no query parameters must be rejected (would delete all devices for app —
  require at least one filter to prevent accidental wipeout).
- Direct publish by recipient attributes (raw transportType + deviceToken) requires push-admin,
  not push-subscribe, to prevent clients from targeting arbitrary device tokens.
```

---

## Prompt 4.5-06: Server and runtime implementation
```text
Implement the push runtime end to end.

Add `crates/sockudo-push/` as a new crate (or add to sockudo-server/src/) with these subsystems:

1. Credential manager
   - Load and decrypt provider credentials per app_id.
   - Cache decrypted credentials in memory with TTL to avoid per-request decryption.
   - Invalidate cache on credential update.

2. Device registry service
   - CRUD for DeviceDetails.
   - State machine: ACTIVE → FAILING → FAILED with configurable threshold.
   - On successful delivery: if state was FAILING, reset to ACTIVE.
   - On delivery failure: increment failure counter; transition state accordingly.

3. Activation service
   - Direct activation: validate and store DeviceDetails, return deviceIdentityToken.
   - Server-assisted: same as admin save, no deviceIdentityToken issued.
   - Re-activation (same deviceId): update recipient data, reset state to ACTIVE.

4. Channel subscription service
   - subscribeDevice / subscribeClient / unsubscribeDevice / unsubscribeClient.
   - listSubscriptions for a channel.
   - listChannels for a device/client.

5. Payload transformer
   - Accepts generic notification+data+overrides.
   - Returns provider-specific payload (FCM JSON body, APNs JSON body, Web Push payload).
   - Override precedence: provider-specific override > generic field mapping.
   - Supports APNs headers (apns-push-type, apns-priority) for background notifications.

6. Provider dispatchers
   - FCM dispatcher: HTTP v1 API using service account JWT authentication.
   - APNs dispatcher: HTTP/2 APNs provider API using certificate authentication.
   - Web Push dispatcher: RFC 8030 / RFC 8291 / RFC 8292 (VAPID-authenticated Web Push).

7. Direct publish handler
   - Resolve recipient to one or more push targets from the device registry.
   - Dispatch to appropriate provider dispatcher per target.
   - Collect results and return per-target success/failure.

8. Channel push handler (hook into message publish path in `crates/sockudo-server/src/http_handler.rs`):
   - On message publish: if `extras.push` is set and push is enabled for the channel,
     look up all channel push subscriptions, resolve devices, dispatch.
   - This must be async and must not block the message publish response.

9. Push failure logger
   - On provider dispatch failure: log structured error with channel, deviceId, provider, error code.
   - Update device state in registry.
   - Emit to the push meta-channel `[meta]log:push` as a V2 server event.
```

---

## Prompt 4.5-07: Cluster and distributed correctness
```text
Harden release 4.5 for clustered operation.

State consistency requirements:
- Device registry and channel subscription state must be stored in the shared backing store,
  not in per-node memory. All nodes must read/write the same registry state.
- Provider credential cache is per-node; cache invalidation must propagate across nodes
  when credentials are updated (use the existing cluster pub/sub for invalidation signals).

Duplicate dispatch prevention:
- When a channel message triggers push, only one node should dispatch to each device.
  Implement a distributed lock (or use the message ID for idempotency) to prevent
  multiple nodes from dispatching the same channel push event to the same device.
- Idempotency key: `(message_id, device_id)` — use `CacheManager::set_if_not_exists()`
  (already used for V2 message idempotency in `crates/sockudo-core/src/cache.rs`).

Token rotation race:
- FCM and APNs may return a new token in the push response (canonical token).
- The dispatching node must update the device registry with the new token.
- If two nodes dispatch concurrently and receive different canonical tokens, the last writer wins.
  Document this behavior; it is acceptable because the token is ultimately canonical anyway.

Failure state convergence:
- If a node dispatches and records a failure, other nodes must see the updated state on next access.
  Do not cache failure state locally beyond the TTL of the device registry cache.

Registry consistency:
- `removeWhere` operations must be atomic with respect to concurrent subscribe operations.
  Use a transaction or compare-and-swap at the backing store level.
```

---

## Prompt 4.5-08: Client and SDK behavior
```text
Implement push notification support across all existing Sockudo SDKs.

Client SDKs (under client-sdks/):
  - sockudo-js       (TypeScript — browser, Node.js, React, React Native, NativeScript, Vue)
  - sockudo-kotlin   (Kotlin — Android)
  - sockudo-swift    (Swift — iOS/macOS)
  - sockudo-flutter  (Dart — Android + iOS cross-platform)
  - sockudo-dotnet   (.NET — MAUI/Android + iOS + Windows)

Server SDKs (under server-sdks/):
  - sockudo-http-python (Python)
  - sockudo-http-node   (TypeScript/Node.js)
  - sockudo-http-go     (Go)
  - sockudo-http-java   (Java)
  - sockudo-http-php    (PHP)
  - sockudo-http-ruby   (Ruby)
  - sockudo-http-rust   (Rust)
  - sockudo-http-dotnet (.NET)
  - sockudo-http-swift  (Swift server-side)

==== CLIENT SDKs ====

--- sockudo-js ---

Browser target (Web Push):
- Add `push` module to `client-sdks/sockudo-js/src/core/`.
- `push.activate()`:
  1. Ensure service worker is registered and ready.
  2. Fetch VAPID public key from Sockudo (GET /apps/{app_id}/push/credentials).
  3. Call `pushManager.subscribe({ userVisibleOnly: true, applicationServerKey })`.
  4. POST registration to Sockudo; store returned deviceIdentityToken.
  5. Idempotent: if already activated with the same endpoint, return existing registration.
- `push.deactivate()`: call `pushManager.unsubscribe()` and DELETE Sockudo registration.
- `push.subscribeToChannel(channel)` / `push.unsubscribeFromChannel(channel)`.
- Service worker integration: document `push` event handler and `notificationclick` handler;
  provide a copyable service worker snippet in the docs and examples.
- Re-activation: if subscription is lost (cleared browser data), re-running activate() is safe.

React Native target (`client-sdks/sockudo-js/react-native`):
- Add push module that accepts a platform token (FCM for Android, APNs for iOS).
- `push.activate({ platform, token })` — POST to Sockudo, store deviceIdentityToken.
- `push.deactivate()` — DELETE Sockudo registration.
- `push.subscribeToChannel(channel)` / `push.unsubscribeFromChannel(channel)`.
- Token rotation: expose `push.updateToken(newToken)` for use in FCM/APNs token refresh callbacks.

Node.js target (`client-sdks/sockudo-js/node.js`):
- No push activation (Node is a server, not a push target).
- Push admin is handled by the server SDK (sockudo-http-node).

--- sockudo-kotlin ---

Location: `client-sdks/sockudo-kotlin/lib/`

FCM integration:
- Add `SockudoPush` class.
- `SockudoPush.activate(context, sockudoClient)`:
  1. Call `FirebaseMessaging.getInstance().getToken()`.
  2. POST to Sockudo device registration endpoint.
  3. Persist deviceIdentityToken in SharedPreferences.
- `SockudoPush.deactivate(context, sockudoClient)`:
  Call DELETE on Sockudo registration. Clear stored token.
- Extend `FirebaseMessagingService.onNewToken()`: update Sockudo registration using
  stored deviceIdentityToken when FCM issues a new token.
- Server-assisted activation variant: `SockudoPush.getTokenForServer()` returns the FCM
  token without registering with Sockudo directly — app server handles registration.
- `SockudoPush.subscribeToChannel(channel)` / `unsubscribeFromChannel(channel)`.
- Document notification payload handling in `FirebaseMessagingService.onMessageReceived()`.

--- sockudo-swift ---

Location: `client-sdks/sockudo-swift/`

APNs integration:
- Add `SockudoPush` class.
- `SockudoPush.activate(client:)`:
  1. Request permission via `UNUserNotificationCenter.requestAuthorization()`.
  2. Call `UIApplication.shared.registerForRemoteNotifications()`.
  3. Receive token in `application(_:didRegisterForRemoteNotificationsWithDeviceToken:)`.
  4. POST hex-encoded token to Sockudo; store deviceIdentityToken in Keychain.
- `SockudoPush.updateToken(_ deviceToken: Data)`: PUT updated token on refresh.
- `SockudoPush.deactivate()`: DELETE registration, clear Keychain entry.
- `SockudoPush.subscribeToChannel(_ channel: String)` / `unsubscribeFromChannel`.
- Background notification requirements (document prominently):
  - payload: `{ aps: { content-available: 1 } }`, headers: `apns-push-type: background`, `apns-priority: 5`.
  - No `alert`, `badge`, or `sound` in background payloads — iOS silently drops them otherwise.

--- sockudo-flutter ---

Location: `client-sdks/sockudo-flutter/lib/`

Cross-platform (Android + iOS via firebase_messaging plugin):
- Add dependency on `firebase_messaging` in `pubspec.yaml`.
- Add `SockudoPush` class.
- `SockudoPush.activate(client)`:
  1. Request permission via `FirebaseMessaging.instance.requestPermission()`.
  2. Get token via `FirebaseMessaging.instance.getToken()`.
  3. POST to Sockudo; store deviceIdentityToken in secure storage.
- Handle token refresh: `FirebaseMessaging.instance.onTokenRefresh.listen()` → PUT updated token.
- `SockudoPush.deactivate()`: DELETE registration, clear stored token.
- `SockudoPush.subscribeToChannel(channel)` / `unsubscribeFromChannel(channel)`.
- Foreground message handling: `FirebaseMessaging.onMessage.listen()` — document how to show
  local notifications for foreground messages.
- Background/terminated handling: `FirebaseMessaging.onBackgroundMessage()` top-level handler.
- iOS-specific: background notification entitlement and APNs headers still apply
  (firebase_messaging handles APNs under the hood on iOS).

--- sockudo-dotnet ---

Location: `client-sdks/sockudo-dotnet/src/`

MAUI cross-platform (Android + iOS + Windows):
- Add `SockudoPush` class.
- On Android (FCM via Firebase MAUI plugin):
  - Get FCM token, POST to Sockudo, store deviceIdentityToken in SecureStorage.
  - Handle token refresh and update Sockudo registration.
- On iOS (APNs via MAUI):
  - Call `UIApplication.RegisterForRemoteNotifications()`.
  - Receive token in `RegisteredForRemoteNotifications` delegate.
  - POST to Sockudo; store deviceIdentityToken.
  - Background notification: same APNs header requirements as iOS.
- On Windows (WNS):
  - Get WNS channel URI via `PushNotificationChannelManager.CreatePushNotificationChannelForApplicationAsync()`.
  - POST as `platform: windows`, `recipient.transportType: wns`, `recipient.channelUri: <uri>`.
  - Extend the server-side WNS dispatcher accordingly (add to Prompt 4.5-06 dispatcher list).
- `SockudoPush.deactivate()`: DELETE registration.
- `SockudoPush.subscribeToChannel(channel)` / `unsubscribeFromChannel(channel)`.

--- sockudo-http-python ---

Location: `server-sdks/sockudo-http-python/`
Python is not a native push target (no platform push APIs).
Provide a push helper module for use in server-assisted flows:
- `sockudo.push.register_device(device_details)` — wraps the HTTP registration POST.
- `sockudo.push.subscribe_to_channel(device_id, channel)`.
- This is not a client activation — it is the server-side registration helper for
  Python app servers doing server-assisted activation on behalf of mobile clients.

==== SERVER SDKs ====

All 8 server SDKs must expose the same push admin interface.
Naming follows each language's conventions but the functionality is identical.

Interface to implement in each SDK:

push.admin:
  saveDevice(deviceDetails)            — upsert a device registration
  getDevice(deviceId)                  — get one device
  listDevices(filters)                 — list devices by clientId / deviceId / state
  removeDevice(deviceId)               — delete a registration
  removeWhereDevices(filters)          — bulk delete (at least one filter required)
  saveChannelSubscription(sub)         — subscribe device or client to channel
  listChannelSubscriptions(filters)    — list subscriptions for channel / device / client
  listChannels(filters)                — list channels for a device or client
  removeChannelSubscription(filters)   — unsubscribe

push:
  publish(recipient, payload)          — direct publish (by deviceId, clientId, or attributes)
  batchPublish(items[])                — batch direct publish; returns per-item results

Payload shape (same across all SDKs):
  notification: { title, body, icon?, sound?, collapseKey? }
  data: {}
  apns: {}     (APNs provider overrides)
  fcm: {}      (FCM provider overrides)
  web: {}      (Web Push provider overrides)

Per-SDK implementation notes:
- sockudo-http-node   (TypeScript): add `push` namespace to the existing Sockudo client class
- sockudo-http-go     (Go): add `Push` field on the client struct with typed methods
- sockudo-http-java   (Java): add `PushAdmin` and `Push` classes, fluent builder for payloads
- sockudo-http-php    (PHP): add `push()` method returning a push fluent interface
- sockudo-http-ruby   (Ruby): add `push` module with admin and publish methods
- sockudo-http-rust   (Rust): add `push` module with async methods returning Result<>
- sockudo-http-dotnet (.NET): add `IPushAdmin` interface and `Push` methods on the client
- sockudo-http-swift  (Swift): add `PushAdmin` actor and `publish()` async method

All server SDK implementations must:
- Use the existing HTTP client/request infrastructure already in each SDK.
- Return provider-agnostic errors with the device state and reason where available.
- Document the `removeWhereDevices` filter requirement (no empty filter allowed).

==== CHANNEL SUBSCRIPTIONS (all SDKs) ====

Client SDKs (with push-subscribe capability):
- `push.subscribeToChannel(channel)` — subscribe own device
- `push.unsubscribeFromChannel(channel)` — unsubscribe own device

Server SDKs (with push-admin capability):
- `push.admin.saveChannelSubscription({ channel, deviceId?, clientId? })`
- `push.admin.removeChannelSubscription({ channel, deviceId?, clientId? })`
```

---

## Prompt 4.5-09: Framework integration and release engineering
```text
Prepare release 4.5 for operator and SDK adoption.

Provider setup guides (add to docs):
- FCM: create Firebase project → create service account → download JSON → POST to Sockudo credentials API.
- APNs: create Apple push certificate → export .p12 → (optional: convert to PEM via openssl) → POST to Sockudo.
  Document the openssl command exactly. Note that APNs auth keys (.p8) are not yet supported — .p12 or PEM only.
- Web Push: Sockudo generates VAPID keys on first POST to /push/credentials/webpush; document the flow.
- WNS (Windows): operator obtains WNS client secret from Azure portal; POST to Sockudo credentials API.

Environment variables (add to `.env.example` and config reference):
- `PUSH_FCM_ENABLED=true`
- `PUSH_APNS_ENABLED=true`
- `PUSH_WEBPUSH_ENABLED=true`
- `PUSH_WNS_ENABLED=true`
- `PUSH_CREDENTIAL_ENCRYPTION_KEY=<operator-supplied secret>`
- `PUSH_FAILURE_THRESHOLD=3` — consecutive failures before device transitions to FAILED

Feature flag (Cargo server):
- Add `push` feature to `sockudo-server/Cargo.toml`.
- All push runtime code is gated behind `#[cfg(feature = "push")]`.
- Build without push: `cargo build -p sockudo --no-default-features --features "v2"`.

SDK package versioning:
- All client and server SDKs bump to a new minor version for this release.
- The push API surface is additive — existing channel/auth/connection APIs are unchanged.
- SDK changelogs must list the new `push` module and its methods explicitly.

SDK release checklist — client SDKs (client-sdks/):
- sockudo-js: push module added to browser, React, and React Native targets; Node target excluded.
- sockudo-kotlin: SockudoPush class added; onNewToken handler documented and tested.
- sockudo-swift: SockudoPush class added; Keychain storage used for deviceIdentityToken.
- sockudo-flutter: SockudoPush class added; both Android (FCM) and iOS (APNs) paths tested.
- sockudo-dotnet: SockudoPush MAUI class added; Android, iOS, and Windows paths tested.
- sockudo-http-python: push registration helper added (server-assisted only).

SDK release checklist — server SDKs (server-sdks/):
- sockudo-http-node: push.admin and push.publish methods added to client class.
- sockudo-http-go: Push field added to client struct with all admin methods.
- sockudo-http-java: PushAdmin and Push classes added with fluent payload builder.
- sockudo-http-php: push() fluent interface added.
- sockudo-http-ruby: push module with admin and publish methods added.
- sockudo-http-rust: push module with async methods added.
- sockudo-http-dotnet: IPushAdmin interface and Push methods added.
- sockudo-http-swift: PushAdmin actor and publish() async method added.

Release engineering checklist (server):
- End-to-end delivery verified for FCM (mock provider in CI; real device for release sign-off).
- VAPID key generation and Web Push dispatch work in CI.
- Channel push dispatch does not block the message publish response.
- Duplicate dispatch prevention tested under concurrent nodes.
- Credential API never returns secret material.
- `removeWhereDevices` with no filter returns 400.
- Per-device state transitions (ACTIVE/FAILING/FAILED) tested.
- WNS dispatcher integrated (or explicitly deferred with a tracked issue).

Release note draft input:
- New feature: Push Notification Platform (FCM, APNs, Web Push, WNS).
- New server Cargo feature: `--features push`.
- New capabilities: push-admin, push-subscribe.
- New client SDK modules: sockudo-js (browser + React Native), sockudo-kotlin, sockudo-swift,
  sockudo-flutter, sockudo-dotnet — all expose SockudoPush activation helpers.
- New server SDK methods: push.admin.* and push.publish() across all 8 server SDKs.
- Breaking: none — all additions are new namespaces/modules.
```

---

## Prompt 4.5-10: Test matrix and automation
```text
Build and run the full test matrix for release 4.5.

Unit tests:
- Payload transformer: for each provider, assert that generic fields map correctly.
- Payload transformer: assert that provider overrides take precedence over mapped fields.
- Payload transformer: APNs background notification produces correct apns-push-type header.
- Device state machine: ACTIVE → FAILING after first failure, → FAILED after threshold, → ACTIVE on success.

Integration tests (using mock provider dispatchers in test mode):
- Direct activation (direct): POST registration → assert device stored in ACTIVE state.
- Direct activation (re-register same deviceId): assert idempotent, no error.
- Server-assisted activation: app server POST → assert device stored, no deviceIdentityToken returned.
- Direct publish by deviceId: assert dispatch called with correct payload.
- Direct publish by clientId: assert all devices associated with clientId are dispatched.
- Direct publish by recipient attributes (transportType + deviceToken): assert correct targeting.
- Batch publish: assert per-item results returned, partial failure handled.
- Channel push: publish message with extras.push, assert all channel push subscribers dispatched.
- Channel push: assert non-subscribed devices are NOT dispatched.
- Push failure: mock provider returns failure → assert device state transitions to FAILING.
- Push failure threshold: N consecutive failures → assert device state transitions to FAILED.
- Delivery recovery: mock success after FAILING → assert device resets to ACTIVE.
- `removeWhere` with no filter: assert 400 rejection.
- Credential GET: assert secret material not returned.
- push-subscribe client subscribes own device (success).
- push-subscribe client tries to subscribe another device (403).
- Token rotation: mock provider returns canonical token → assert registry updated.

Auth boundary tests:
- push-admin can list all devices.
- push-subscribe can only list own devices.
- publish with no push capability: channel push still works (push is server-initiated).

Fix all failures before completing this prompt.
```

---

## Prompt 4.5-11: Observability, docs, and operator guidance
```text
Add metrics, logs, docs, and operator guidance for release 4.5.

Metrics (add to `crates/sockudo-metrics/src/prometheus.rs`):
- `sockudo_push_dispatched_total{provider, status}` — counter per dispatch attempt (status: success/failure)
- `sockudo_push_dispatch_duration_seconds{provider}` — histogram for provider round-trip latency
- `sockudo_push_devices_total{app, state}` — gauge per device state (ACTIVE, FAILING, FAILED)
- `sockudo_push_channel_publish_total{channel}` — counter per channel push trigger
- `sockudo_push_duplicate_suppressed_total` — counter for idempotency suppressions

Logs:
- INFO on successful dispatch (deviceId, provider, message title)
- WARN on first delivery failure (deviceId, provider, error code, failure count)
- ERROR on device transition to FAILED (deviceId, provider, consecutive failure count)
- ERROR on credential load failure (provider, app_id)
- WARN on credential cache invalidation (provider, app_id)

Push meta-channel:
- Emit push delivery failures and state transitions to `[meta]log:push` as V2 server events.
- Event shape: `{ deviceId, provider, error, state, timestamp }`.
- Subscribing to `[meta]log:push` requires push-admin capability.

Docs:
- Add `docs/content/2.server/23.push-notifications.md` covering:
  - provider configuration (FCM, APNs, Web Push) with step-by-step setup
  - activation models (direct and server-assisted)
  - direct publish, batch publish, and channel push
  - payload structure and provider override format
  - APNs background notification headers
  - admin API reference
  - device state model and failure tracking
  - `[meta]log:push` meta-channel
  - `push` feature flag
  - environment variables
  - troubleshooting: FAILING/FAILED devices, credential errors, duplicate suppression

- Update `docs/content/5.reference/3.http-api.md` with all push endpoints.
```

---

## Prompt 4.5-12: Final verification and release audit
```text
Run the final release audit for 4.5.

Verification checklist:

Delivery:
- End-to-end delivery verified for FCM (real or mock) and Web Push (VAPID flow).
- APNs credential load and certificate auth tested in integration (real device not required for CI).
- Channel push dispatches do not block the message publish response.

Registry:
- Device state transitions (ACTIVE/FAILING/FAILED) work correctly.
- `removeWhere` with no filter is rejected.
- Credential GET never returns secret material.

Auth:
- push-subscribe cannot access other clients' devices.
- push-admin has full access.
- Channel push requires no extra capability on the publisher.

Cluster:
- Duplicate dispatch is suppressed by idempotency key under concurrent nodes.
- Credential cache invalidation propagates to all nodes.

Observability:
- Dispatch metrics fire for success and failure cases.
- Device FAILED transitions log an error.
- `[meta]log:push` delivers push failure events.

Docs:
- All provider setup guides are accurate.
- API reference matches implementation.
- Failure state and troubleshooting guidance is present.

Deliver:
1. Any findings and exact evidence.
2. Go/no-go decision with justification.
3. If no-go: enumerated blockers with owner and fix scope.
4. Release note draft for 4.5.
5. Readiness statement for proceeding to 4.6 (AI Transport).
```
