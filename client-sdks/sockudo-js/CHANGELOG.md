# Changelog

## Unreleased

### Added

* public TypeScript declarations for push proxy APIs and typed Apple Live Activity direct and
  broadcast publishing

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
