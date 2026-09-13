# Apple Live Activities backend example

A ride-tracking backend that drives ActivityKit Live Activities through Sockudo, plus a
mock APNs that lets the whole path run locally without Apple credentials or a device:

```
iOS app (simulated) ──tokens──▶ backend (Node, @sockudo server SDK)
                                   │ signed push-admin API
                                   ▼
                                Sockudo (push-apns, monolith workers)
                                   │ HTTP/2 + TLS, ES256 provider JWT
                                   ▼
                        mock APNs (Docker): /3/device, /4/broadcasts, /1/apps/…/channels
```

The mock enforces Apple's contract: verified provider JWTs, `apns-topic` of
`<bundle>.push-type.liveactivity`, hyphenated `aps` keys, priority rules (1 is broadcast-only),
4096/5120-byte payload limits, `apns-expiration` semantics per storage policy, and the
documented error reasons (`BadDeviceToken`, `Unregistered`, `BadChannelId`, `TooManyRequests`,
`InternalServerError`). Device-token prefixes select failures: `bad0…` → 400, `4a0e…` → 410,
`429e…` → one 429 with `Retry-After`, `5005…` → one 500.

## Run

Requires Docker, Node 24+, OpenSSL, and a Rust toolchain.

```bash
examples/apple-live-activities-backend/run.sh
```

`run.sh` generates a private CA and provider key (`certs/`, git-ignored), starts the mock,
builds and starts Sockudo with `push.apns.ca_certificate_path` pointing at that CA, starts the
backend on `:8787`, and runs `backend/scenario.mjs`, which plays the iOS app and the dispatcher
and then verifies every request the mock received. Set `KEEP_RUNNING=1` to leave the mock up.

Useful endpoints while it runs:

- `http://127.0.0.1:8444/_mock/requests` — every APNs request with redacted auth, parsed body,
  and the mock's response
- `http://127.0.0.1:8444/_mock/channels` — broadcast channel inventory
- `http://127.0.0.1:9601/metrics` — Sockudo push metrics, including
  `sockudo_push_apns_live_activity_requests_total`

## What the scenario covers

- broadcast channel create / get / list / delete through the signed push-admin API
- push-to-start with `inputPushChannel`, update with `staleDate` and `relevanceScore`, end with
  `dismissalDate`, and idempotent re-publish of the same `publishId`
- `mostRecent` broadcast with `expiresAtMs` → `apns-expiration`, `noStorage` → `apns-expiration: 0`,
  `lowPower` priority 1 on broadcast only
- admission-time rejections that never reach APNs (`lowPower` on a direct token, both input push
  fields, missing attributes, bad timestamps, broadcast `start`, unknown storage policy)
- provider failure contract: terminal 400/410, 429 retried with the same `apns-id`, 5xx held for
  Apple's 15-minute payload delay without pausing other deliveries, oversized payloads rejected
  before dispatch, publishes to deleted channels failing terminally

## iOS Simulator app

`ios/RidesLiveActivity.xcodeproj` is a SwiftUI app plus a WidgetKit Live Activity extension that
depends on the local `client-sdks/sockudo-swift` package. On launch it observes the ActivityKit
push-to-start token and every activity's update token with `SockudoLiveActivityTokens`, uploads
them to the backend, and mirrors activity state and lifecycle in a log. "Start locally" requests an
activity on the device; the app then uploads that activity's real update token.

```bash
xcodebuild -project ios/RidesLiveActivity.xcodeproj -scheme RidesApp \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' build
# install/launch the built RidesApp.app with `xcrun simctl install` / `launch`, tap "Start locally"
node ios/sim-scenario.mjs
```

With the mock APNs the scenario proves the token path end to end: real 128-byte ActivityKit tokens
travel app → backend → Sockudo → APNs request with the right topic, headers, and `aps` payload. The
Simulator cannot *receive* those pushes from a mock: `xcrun simctl push` only emulates application
notifications and never reaches `liveactivitiesd`. To see the Live Activity update on screen, point
Sockudo at Apple's sandbox hosts with a real `.p8` key (`PUSH_APNS_TEAM_ID`, `PUSH_APNS_KEY_ID`,
`PUSH_APNS_PRIVATE_KEY_PATH`, the three `*.sandbox.push.apple.com` endpoints, and a bundle ID your
team owns) and run `MODE=sandbox node ios/sim-scenario.mjs`; Simulators on Apple silicon hold a real
sandbox APNs connection and the tokens they issue are valid there.

## Flutter app

`flutter_app/` is the same ride demo built with Flutter and `sockudo_flutter` (path dependency).
ActivityKit is reached through a small platform-channel bridge in
`flutter_app/ios/Runner/LiveActivityBridge.swift`, as the SDK README recommends; the Dart side
turns every token rotation into `ApnsLiveActivityTokenUpdate.pushToStart(...)` /
`.activity(...)` uploads and publishes updates for its own activity with
`SockudoPushRegistration.publishLiveActivity(...)` through the backend's `/push` proxy. The Live
Activity widget extension is added to the generated Runner project by
`flutter_app/ios/add_widget_target.rb` (run once with CocoaPods' Ruby:
`GEM_HOME=$(brew --prefix cocoapods)/libexec ruby add_widget_target.rb`).

Build it with Xcode rather than `flutter build ios --simulator`: the Flutter tool disables code
signing for Simulator builds, which drops the `aps-environment` entitlement ActivityKit needs
before it issues push tokens.

```bash
cd flutter_app && flutter pub get
xcodebuild -workspace ios/Runner.xcworkspace -scheme Runner -configuration Debug \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' build
```

The backend accepts the SDK's token payload on `POST /liveActivities/tokens` and exposes the proxy
routes `POST /push/publish`, `GET /push/publish/{id}/status`, and
`POST|DELETE /push/liveActivities/channels[/{id}]`. The `sockudo_flutter` test suite has a matching
live test (`SOCKUDO_LIVE_TESTS=1`, optional `SOCKUDO_PUSH_PROXY_URL` and `SOCKUDO_APNS_MOCK_URL`).

## Backend API

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/devices/push-to-start` | store a user's push-to-start token |
| POST | `/rides/{id}/token` | store the activity's current update token |
| POST | `/liveActivities/tokens` | `sockudo_flutter` token update (`kind`, `token`, `activityId`, plus `userId`/`rideId`) |
| POST/GET/DELETE | `/push/...` | proxy surface for mobile SDK push helpers |
| POST | `/rides/{id}/start` | push-to-start (`contentState`, optional `channelId`) |
| POST | `/rides/{id}/update` | update (`contentState`, `staleInSecs`, `relevanceScore`, `priority`) |
| POST | `/rides/{id}/end` | end (`contentState`, `dismissalInSecs`) |
| POST | `/broadcasts/channels` | create a channel (`storagePolicy`) |
| GET/DELETE | `/broadcasts/channels[/{id}]` | inspect or delete channels |
| POST | `/broadcasts/channels/{id}/publish` | broadcast update/end |
| GET | `/devices/push-to-start/{userId}`, `/rides/{id}/token` | read stored tokens |
| GET | `/publishes/{publishId}` | Sockudo publish status |
