# Changelog

## Unreleased

- Added Protocol V2 client platform primitives: capability-token URL auth with provider refresh, presence member updates, attach-serial exposure with `until_attach` channel history params, proxy-backed mutable-message write helpers, and `appendRollupWindow` negotiation.
- Added proactive 80%-lifetime refresh scheduling for JWT tokens returned by `ClientAuthTokenProvider`; opaque and static tokens remain reactive-only and rely on `sockudo:token_expired`.
- Hardened Protocol V2 realtime decoding for forward compatibility: integer string serials now parse without floating-point truncation, `extras.ai` and unknown extras are retained, and unknown internal presence frames no longer mutate member state.
- Added forward-compat fixture replay coverage for realtime frames and serial boundary tests beyond 32-bit and JavaScript safe-integer limits.

## 0.1.0

- Initial public release of the official Sockudo Kotlin client.
- Added public, private, presence, and encrypted channel support.
- Added channel auth, user sign-in, watchlist events, and client events.
- Added filter-aware subscriptions and Fossil/Xdelta3 delta decoding.
- Added CI, Maven publishing workflow, and live integration tests.
