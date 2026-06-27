# Changelog

## Unreleased

- Added Protocol V2 capability-token options, initial WebSocket `token` query support, typed
  `sockudo:auth_success` / `sockudo:token_expired` state, and provider-backed `sockudo:auth`
  refresh attempts. Provider-backed JWTs with `exp` now schedule proactive refresh at 80% of token
  lifetime; opaque tokens and static-only tokens remain reactive only.
- Added Protocol V2 `append_rollup_window` validation/query support for the locked server windows
  `0`, `20`, `40`, `100`, and `500`.
- Added presence channel `update(data:)`, `sockudo_internal:presence_update` member updates, and
  `sockudo:presence_update` channel emission.
- Added `attachSerial`, `ChannelHistoryParams.untilAttach`, channel-level history/latest/version
  helpers, and proxy-backed versioned message create/update/append/delete helpers with typed
  acknowledgements and 10 second proxy timeouts.
- Hardened realtime decoding for forward compatibility: full-width serials are preserved, unknown
  extras tiers such as `extras.ai` remain available through raw extras passthrough, and unknown
  internal channel events no longer mutate presence/member state.

## 0.1.0

- Initial public release of the official Sockudo Swift client.
- Added public, private, presence, and encrypted channel support.
- Added channel auth, user sign-in, watchlist events, and client events.
- Added filter-aware subscriptions and Fossil/Xdelta3 delta decoding.
- Added Swift Package Manager packaging and GitHub Actions CI.
