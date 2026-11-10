# Changelog

## Unreleased

- Use the stable `crypto_secretbox` 0.1 release. The `0.2.0-pre.0` pre-release pinned `aead`
  release candidates that cannot resolve alongside stable `crypto-common` 0.2, so the crate failed
  to build in any project that also used current RustCrypto crates (for example `sockudo-ws`).

## 2.2.0 - 2026-08-17

- Added `force_reconnect_user` for `POST /users/{userId}/force_reconnect`.
- Corrected webhook signature header names to match the server and fixed the README's public type
  and module names.
- Reduced avoidable clones in request construction.

## 2.1.0 - 2026-06-27

- Preserved future webhook event names, fields, and nested payload values.
