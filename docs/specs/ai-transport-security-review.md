# AI Transport Security Review

Date: 2026-06-03
Scope: AI Transport plus newly exposed versioned-message, history, presence, recovery, and push surfaces.

## Threat Model

Sockudo AI Transport is exposed to hostile multi-tenant internet clients. Attackers may be anonymous, V1 HMAC users, V2 HMAC users, capability-token users with narrow grants, expired or revoked token holders, wrong-app key holders, server-key holders, or push admins/subscribers. The protected assets are tenant isolation, verified `client_id`, V1 byte compatibility, channel history continuity, mutable-message ownership, push provider credentials, webhook secrets, and node memory/CPU.

## Landed Mitigations

| Area | Mitigation |
| --- | --- |
| Token identity | Capability-token sockets can no longer use legacy `signin` to replace identity or widen capabilities. |
| AI client events | WS `ai-input` and `ai-cancel` now require an authenticated V2 `client_id`; `*-client-id` headers are still checked against verified identity. |
| Rollup exhaustion | Append rollup state is capped per `(app, channel)` using `ai_transport.max_open_streaming_messages_per_channel`; overflow bypasses coalescing instead of allocating more state. |
| Secret logging | API request signatures, NATS credentials, raw WebSocket messages, local adapter sends, and webhook payloads are redacted or metadata-only in debug logs. |
| Debug redaction | `PusherMessage` debug output no longer includes payloads, extras, or idempotency-key values. |
| Crypto compare | SQL app-manager signature helpers now use constant-time `secure_compare`. |
| Push credentials | New push credential writes require `PUSH_CREDENTIAL_ENCRYPTION_KEY` and always store AES-GCM envelopes. Legacy local plaintext envelopes remain readable only for migration/rotation. |
| Limiter cleanup | Per-socket `channel_history` semaphores are removed on socket cleanup. |
| Presence update limiters | Presence update rate limiters are scoped to socket lifetime while retaining per `(app, channel, user)` counters inside the limiter. |
| Pending presence removals | Ungraceful disconnect grace periods use one bounded deadline worker and a per-channel pending-member index instead of one sleeper task per disconnect plus global scans. |
| AI admission counters | Open streaming-message admission uses the active-stream counter, and append-count admission uses a cache-backed per-message counter with rollback before persistence; absent counters bootstrap once from existing version-store state. |
| Orphan scanning | AI stream orphan cleanup uses paged cache cursor scans across the active-stream registry instead of repeatedly scanning a fixed prefix page. |
| AuthZ regression | A table-driven server regression enumerates all AI Transport operation families against every principal class with stable allow/deny codes. |
| Parser smoke | CI fuzz smoke covers AI header validation, generic wire-message deserialization, capability maps, history cursors, push payload mapping, and mutation requests; seed corpora are committed. |
| Supply chain | JWT signing now uses the `aws_lc_rs` backend, native WebPush VAPID JWT signing avoids the RustCrypto RSA path, SQLx is on the 0.9 line, and `deny.toml` gates the audited AI Transport release feature graph. |

## AuthZ Matrix Summary

| Operation family | Anonymous | V1 HMAC | V2 HMAC | Capability token | Expired/revoked token | Server key | Wrong-app key |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Subscribe public | Allow unless namespace denies | Allow | Allow | Requires `subscribe` if token scoped | Deny | N/A | Deny |
| Subscribe private/presence | Deny | Pusher auth required | Pusher auth or token state required | Requires `subscribe`; presence also `presence` | Deny | N/A | Deny |
| Publish client events | Deny on public/non-private | Existing V1 behavior | Requires client events + publish rules | Requires `publish` | Deny | N/A | Deny |
| Publish `ai-input`/`ai-cancel` | Deny | Deny | Requires V2, publish, authenticated `client_id` | Requires `publish`, authenticated token identity | Deny | Allow via HTTP signed publish | Deny |
| Publish `ai-output`/`ai-turn-*` | Deny | Deny | Deny from WS client | Deny from WS client | Deny | Allow via HTTP signed publish | Deny |
| Mutable append/update/delete | Deny | Server HTTP only | Socket-proxied HTTP requires V2 actor + mutation capability | Requires publish plus own/any mutation capability | Deny | Allow privileged HTTP | Deny |
| History WS `channel_history` | Deny | Deny | Requires V2 subscribed channel | Requires `history` | Deny | N/A | Deny |
| History HTTP/state/reset/purge | Deny | Signed app HTTP only | Signed app HTTP only | Not accepted as app auth | Deny | Allow signed app HTTP | Deny |
| Presence update | Deny | Deny | Requires V2 active member + `presence` capability gate when scoped | Requires active member + `presence` | Deny | N/A | Deny |
| Push admin | Deny | Signed app HTTP + push-admin | Signed app HTTP + push-admin | Not accepted as app auth | Deny | Allow signed app HTTP | Deny |
| Push subscribe | Deny without device token | Signed app HTTP or push-subscribe device flow | Signed app HTTP or push-subscribe device flow | Not accepted as app auth | Deny | Allow signed app HTTP | Deny |
| Token revocation admin | Deny | Signed app HTTP only | Signed app HTTP only | Not accepted as app auth | Deny | Allow signed app HTTP | Deny |

## Release Gate

No residual AI Transport security risk is accepted for release. The previously listed release
blockers are closed in code by the mitigations above. "Release blocker" here means a defect or
unverified security condition that must be fixed before the AI Transport surface can be shipped.

The committed `deny.toml` intentionally checks the AI Transport release graph:

- `sockudo/ai-transport`
- `sockudo/push`
- `sockudo/push-fcm`
- `sockudo/push-apns`
- `sockudo/push-webpush`
- `sockudo/mysql`
- `sockudo/postgres`

That graph excludes unrelated optional datastore and queue backends such as SurrealDB and Iggy.
Separate full-feature compilation still verifies those optional backends build.

## Verification Notes

Focused checks run during this pass:

- `cargo test -p sockudo-protocol pusher_message_debug_redacts_payload_extras_and_idempotency --lib`
- `cargo test -p sockudo --features push encrypted_secret_ --bins --tests`
- `cargo test -p sockudo-adapter token_authenticated_connections_cannot_sign_in_again --lib --features ai-transport,full`
- `cargo test -p sockudo-ai-transport active_stream_cap_bypasses_new_rollup_state --lib`
- `cargo test -p sockudo-core token::tests --lib`
- `cargo test -p sockudo-push debug_redacts_tokens_credentials_metadata_and_raw_payloads --lib`
- `cargo check -p sockudo --features ai-transport,push,push-fcm,push-apns,push-webpush`
- `cargo check -p sockudo-adapter --features ai-transport,full`
- `cargo check -p sockudo-app --features mysql,postgres`
- `cargo test -p sockudo-cache increment_by_serializes_concurrent_updates`

Full checks run during this pass:

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
- `cargo audit`
- `cargo deny check`
- `cargo check -p sockudo --features full`
- `cargo check --manifest-path fuzz/Cargo.toml`
- AI release graph absence checks for `rsa` and `paste` with `cargo tree -i`

`cargo audit` exits successfully. It still reports the repository's existing allowed
unmaintained-`paste` warning through the optional Iggy backend, which is outside the AI Transport
release graph above. The AI release graph does not include `paste` or `rsa`. `cargo deny check`
passes for that graph with advisories, bans, licenses, and sources all OK; cargo-deny still emits
metadata warnings because workspace crates use `license-file` rather than a manifest `license`
expression. Miri was not run in this pass.
