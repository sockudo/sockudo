# Release 4.3 Adoption And Release Engineering

## Scope

This artifact packages the downstream-adoption inputs for release 4.3:

- worked example pointers
- compatibility notes
- release note draft
- SDK and packaging follow-up markers
- release engineering checklist

Release 4.3 is Sockudo-native V2 behavior. It is not a Pusher-compatible extension.

## Downstream Summary

Release 4.3 adds mutable durable messages for Protocol V2:

- `message.create`
- `message.update`
- `message.delete`
- `message.append`

Client-visible rules:

- latest-view history stays at the original history position
- version history preserves all accepted versions
- `message.update` means replace local visible state with the full payload
- `message.delete` means the latest visible version is deleted, without physical erasure
- `message.append` means concatenate onto the current local string state
- if a client receives `message.append` without a seeded string base, it should fetch the latest visible state first

## Worked Examples

Current repo examples for release 4.3:

- [`examples/versioned_messages_v2.js`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/examples/versioned_messages_v2.js)
- mutable-message reducer guidance in:
  - [`client-sdks/sockudo-js/README.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-js/README.md)
  - `client-sdks/sockudo-swift/README.md`
  - `client-sdks/sockudo-kotlin/README.md`
  - `client-sdks/sockudo-flutter/README.md`
  - `client-sdks/sockudo-dotnet/README.md`

## Compatibility Notes

- Protocol V1 remains the Pusher-compatible path.
- Protocol V1 must never receive mutable-message action events, version metadata, or mutation-only envelopes.
- Protocol V2 is the only supported path for release 4.3 mutable messages.
- Existing immutable history remains readable and is not backfilled into mutable-message chains.
- Mutable-message HTTP endpoints and client behavior should be documented as Sockudo-native V2 features, not as Pusher-compatible behavior.

## Release Note Draft

### Release 4.3: Versioned Durable Messages

Sockudo 4.3 adds Sockudo-native mutable durable messages on Protocol V2.

This release introduces stable logical message identity, preserved version history, latest-visible history substitution, and realtime mutation events for:

- `sockudo:message.update`
- `sockudo:message.delete`
- `sockudo:message.append`

It also adds own-versus-any mutation authorization, version-history retrieval, and continuity-aware recovery for mutable-message streams.

Important notes:

- this is a Protocol V2 feature set
- Protocol V1 remains strictly Pusher-compatible and does not receive mutable-message mutation envelopes
- latest-view history and version-history are distinct by design
- existing immutable history is not backfilled into mutable-message chains

### Suggested Upgrade Framing

- Stay on Protocol V1 if you only need Pusher-compatible event delivery.
- Adopt Protocol V2 when you want Sockudo-native message mutation, version retrieval, or continuity-aware mutable history.
- Treat release 4.3 as a new product capability, not a transparent compatibility change.

## SDK And Packaging Follow-Up Markers

- JavaScript SDK:
  - shipped reducer helpers for mutable-message event interpretation
  - future follow-up: add signed REST helpers for latest-message and version-history retrieval
- Python SDK:
  - docs updated with client interpretation rules
  - future follow-up: add helper utilities for mutable-message reduction
- Swift SDK:
  - docs updated with client interpretation rules
  - future follow-up: add typed mutable-message models and reducers
- Kotlin SDK:
  - docs updated with client interpretation rules
  - future follow-up: add typed mutable-message models and reducers
- Flutter SDK:
  - docs updated with client interpretation rules
  - future follow-up: add typed mutable-message models and reducers
- .NET SDK:
  - docs updated with client interpretation rules
  - future follow-up: add typed mutable-message models and reducers
- Server SDKs:
  - future follow-up: add first-class release 4.3 REST helpers for latest-message and version-history endpoints

## Release Engineering Checklist

- Update top-level changelog with a 4.3 release summary.
- Keep compatibility docs explicit that mutable messages are Sockudo-native V2 behavior.
- Ship at least one worked example showing replace-versus-concatenate handling.
- Ensure the JS client package exports the mutable-message reducer helpers.
- Keep README-level guidance aligned across all in-repo client SDKs.
- Verify protocol, auth, runtime, and latest-history substitution tests pass.
- Verify cluster correctness tests compile and run, even if environment-gated integrations self-skip.
- Do not claim historical backfill support for pre-4.3 immutable history.
- Mark durable version-store backend work as follow-up if production packaging depends on it.

## Remaining Packaging Risk

Release 4.3 is technically usable with the current in-process version-store runtime and the documented client behavior, but durable backend version-store runtime remains a follow-up for fully restart-safe multi-node production packaging.
