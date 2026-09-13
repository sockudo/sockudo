# Changelog

## Unreleased

- Added typed Apple Live Activity token updates, direct and broadcast recipients, validated
  ActivityKit payloads, and `PublishLiveActivityAsync` for authenticated push proxies.

## 2.2.0 - 2026-08-17

- Added bounded automatic reconnection, configurable retry limits and gaps, and an explicit
  reconnecting state.
- Corrected Protocol V2 capability-token refresh, expiration, and revocation handling.
- Hardened Ably-compatible connection behavior.

## 2.1.0 - 2026-06-27

- Added Protocol V2 capability-token auth, presence updates, history/recovery helpers, mutable
  message operations, and forward-compatible frame decoding.
