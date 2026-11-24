# Changelog

## Unreleased

### Added

* public TypeScript declarations for push proxy APIs and typed Apple Live Activity direct and
  broadcast publishing
* `retryJitterMs` option that waits a random delay before immediate protocol retries (such as the
  `4200` server-shutdown close), so a draining node's clients do not reconnect in the same instant

### Changed

* published TypeScript declarations now name the client `Sockudo`, `SockudoWithEncryption`, and
  `SockudoEvent`, matching the runtime; the deep-path-only `PusherEvent` alias is removed
* the Vite and Vitest configs resolve their paths through `import.meta.dirname` instead of
  `__dirname`, which Vite's upcoming native config loader does not support

### Fixed

* honour `delta_compression_enabled` with `enabled: false`; `data.enabled || true` always reported
  delta compression as enabled
* track delta bases per event and conflation key, as the server does; a single base per channel
  applied one event's delta to another event's base whenever events interleaved, failing the
  checksum and forcing a resync
* retry a connection attempt that fails before its 15 s timeout after a short jittered backoff
  (0.5-1 s, growing to 2.5-5 s) instead of waiting out the full timeout, so clients come back
  within seconds of an outage ending (sockudo/sockudo#532)
* stop retrying when a connection attempt is aborted, even if its transport reports a failure
  afterwards
* resolve the documented `@sockudo/client/with-encryption`, `@sockudo/client/worker`, and
  `@sockudo/client/worker/with-encryption` subpaths, plus the `node.js` shim, which still imported
  the pre-rename `pusher-*` bundles and failed with `ERR_MODULE_NOT_FOUND` in Node and bundlers
  (sockudo/sockudo#569)
* declare `channel.sockudo`, `user.sockudo`, and the `sockudo` constructor and factory parameters
  that the runtime exposes, instead of a `pusher` field that was always `undefined`
* emit `pusher:signin_error` (`sockudo:signin_error` under Protocol V2) when user authentication
  fails, mirroring the existing `subscription_error` contract; ported from pusher-js v8.6.0
* run the signin cleanup even when a `signin_error` listener throws, so `user.signinDonePromise`
  settles and presence authorization does not wait for a disconnect

### Security

* clear the development dependency advisories: `oxfmt` 0.80.0 drops the vulnerable `tinypool`,
  `vitest` 4.1.12 fixes `@vitest/mocker` path traversal and mock injection, `vite` 8.3.8 fixes the
  `launch-editor` and dev-server issues, and `vue` 3.5.46 fixes the `@vue/server-renderer` XSS.
  `undici` and `source-map-js` now resolve to patched releases through `jsdom` 31 and the existing
  parent ranges
* remove the `overrides` block; every previously pinned transitive resolves to a patched version
  through its parent's own range, so the forced versions are no longer needed
* move `protobufjs` to 7.6.6 and `@msgpack/msgpack` to 3.1.3

## [2.2.0] - 2026-08-17

### Added

* bounded automatic reconnection with configurable attempt and retry-gap limits plus an explicit
  `reconnecting` state
* Protocol V2 capability-token refresh through static tokens, auth URLs, and callbacks, including
  proactive JWT refresh and typed expiration/revocation errors
* native continuity and recovery metadata used by Sockudo 5 and the AI Transport SDK

### Fixed

* refresh provider tokens before reconnecting and never resend static, expired, or revoked tokens
* restore the React Native entrypoint and harden Ably-compatible auth, presence, and connection
  behavior
* update `protobufjs` to the patched 7.6.5 release

## [2.1.0] - 2026-06-27

### Fixed

* harden forward-compatible realtime decoding for unknown V2 frames, `extras.ai`
  passthrough, malformed presence member events, and u64 serial preservation
* add Protocol V2 capability-token URL auth through `token`, `authUrl`, and
  `authCallback`, including reconnect refresh, proactive JWT refresh,
  `sockudo:auth`, and typed expiry/revocation errors
* reject capability-token configuration under Protocol V1 and avoid resending
  static or revoked tokens
* synchronize checked-in declarations with the V2 event and capability-token APIs

## [1.1.0](https://github.com/sockudo/sockudo-js/compare/v1.0.0...v1.1.0) (2026-02-11)


### Features

* initialize @sockudo/client package with CI publish workflow ([680b3f5](https://github.com/sockudo/sockudo-js/commit/680b3f54d0452ce1879b6f005a8c61f57754be64))
