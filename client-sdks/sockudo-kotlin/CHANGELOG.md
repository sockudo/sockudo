# Changelog

## Unreleased

- Added Pusher-style connection/error listeners (`SockudoConnectionEventListener`, `SockudoError`) while preserving the raw Pusher-compatible `state_change` event.
- Added typed channel event and auth listeners (`SockudoChannelEventListener.onEvent`, `onSubscriptionSucceeded`, `onAuthenticationFailure`, `onError`) while preserving the raw `bind`/`on` event API.

## 2.2.0 - 2026-08-17

- Added `RECONNECTING`, bounded reconnect attempts, configurable retry gaps, and quadratic backoff
  with immediate retry for retryable/TLS-only close codes.
- Corrected Protocol V2 capability-token refresh, expiration, revocation, and reconnect behavior.
- Lowered the JVM target from 23 to 21.

## 2.1.0 - 2026-06-27

- Added Protocol V2 client platform primitives: capability-token URL auth with provider refresh, presence member updates, attach-serial exposure with `until_attach` channel history params, proxy-backed mutable-message write helpers, and `appendRollupWindow` negotiation.
- Added proactive 80%-lifetime refresh scheduling for JWT tokens returned by `ClientAuthTokenProvider`; opaque and static tokens remain reactive-only and rely on `sockudo:token_expired`.
- Hardened Protocol V2 realtime decoding for forward compatibility: integer string serials now parse without floating-point truncation, `extras.ai` and unknown extras are retained, and unknown internal presence frames no longer mutate member state.
- Added forward-compat fixture replay coverage for realtime frames and serial boundary tests beyond 32-bit and JavaScript safe-integer limits.
- Capability-token auth now fails closed outside Protocol V2, fetches provider tokens before reconnect, refreshes only code `40142`, and never resends static or revoked tokens.

## 0.1.0

- Initial public release of the official Sockudo Kotlin client.
- Added public, private, presence, and encrypted channel support.
- Added channel auth, user sign-in, watchlist events, and client events.
- Added filter-aware subscriptions and Fossil/Xdelta3 delta decoding.
- Added CI, Maven publishing workflow, and live integration tests.
