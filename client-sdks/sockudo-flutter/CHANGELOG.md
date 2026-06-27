## Unreleased

* Added Protocol V2 capability-token options with initial `token` query auth, proactive 80% lifetime refresh for JWTs when `authCallback` is available, `authCallback` refresh on `sockudo:token_expired` code `40142`, and `sockudo:auth_success` event passthrough. Opaque tokens and static-only tokens remain reactive/manual.
* Added `PresenceChannel.update(data)` plus inbound `sockudo_internal:presence_update` member-state handling and `sockudo:presence_update` emission.
* Added `ChannelHistoryParams.untilAttach`, `SockudoChannel.attachSerial`, local `appendRollupWindow` validation/query support, and proxy-backed versioned message create/append/update/delete helpers returning typed `MessageAck`s with a 10s default timeout.
* Harden realtime decoding for forward-compatible Protocol V2 frames by preserving raw extras (including `extras.ai`), retaining unsafe serials as strings, and ignoring future presence/member shapes without corrupting membership state.

## 2.0.0

* Release the Sockudo Flutter client as part of the coordinated SDK 2.0.0 release train.
* Align the package version and runtime client version reported during WebSocket handshakes.

## 1.0.0

* Stable release with full Sockudo V1 and V2 protocol support.

## 0.1.0

* Initial public Dart and Flutter package layout.
* Added filter builders and validation helpers.
* Added auth request and response models.
* Added a WebSocket-backed Sockudo client with public, private, presence, and encrypted channel types.
* Added CI validation and `pub.dev` publish workflows.
