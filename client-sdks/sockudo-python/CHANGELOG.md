# Changelog

## Unreleased

- Fixed MessagePack extras: the server sends them as a positional array, which was ignored, so
  headers, idempotency keys, echo and AI metadata were lost. JSON `idempotencyKey` is accepted too.
- Fixed `PresenceChannel.update()`: it sent `{"user_info": ...}`, which the server rejects; it now
  sends `{"channel", "data"}` and requires Protocol V2.
- Fixed user sign-in: events on the `#server-to-user-{id}` channel were never delivered because the
  channel was not registered with the client.

## 2.2.0 - 2026-08-17

- Added bounded automatic reconnection, configurable retry limits and gaps, and an explicit
  reconnecting state.
- Corrected Protocol V2 capability-token refresh, expiration, revocation, and reconnect behavior.
- Hardened Ably-compatible auth, presence, and connection handling.

## 2.1.0 - 2026-06-27

- Added Protocol V2 capability-token auth, presence updates, history/recovery helpers, mutable
  message operations, and forward-compatible frame decoding.
