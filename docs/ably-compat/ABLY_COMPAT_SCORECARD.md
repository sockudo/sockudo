# Ably AI Transport Compatibility Scorecard

Discovery date: 2026-07-03

Status: reduced compatibility implementation added for the Ably AI Transport SDK smoke path. When
built with Cargo feature `ably-compat`, Sockudo exposes an additive Ably Realtime/REST facade while
the native Sockudo/Pusher `/app/{appKey}` WebSocket route remains unchanged.

Sockudo should not claim broad Ably platform compatibility until the full Ably package tests and
Sockudo V1/V2 conformance tests pass against pinned target versions.

## Target Versions

| Surface | Target | Evidence |
| --- | --- | --- |
| Ably AI Transport package | `@ably/ai-transport@0.4.0` | Smoke harness in `tests/ably-compat`. |
| Ably Realtime JS peer | `ably@2.23.0` | Smoke harness in `tests/ably-compat`. |
| Sockudo server workspace | `4.7.0` | Root Cargo workspace package version. |

## Compatibility Summary

| Area | Status | Notes |
| --- | --- | --- |
| Pusher Protocol V1 compatibility | Supported | Ably compatibility is additive and must not alter V1 wire behavior. |
| Sockudo-native AI Transport | Supported | Uses Cargo feature `ai-transport` plus runtime `[ai_transport]`; it does not require `ably-compat`. |
| Ably facade feature gate | Supported | Root Ably WebSocket route, Ably REST routes, and Ably realtime egress tap are compiled only with `ably-compat`. |
| Ably Realtime JSON protocol | Supported for AI Transport only | Handles the JSON ProtocolMessage flows needed by the harness. |
| Ably REST time/token/history/message reads | Supported for AI Transport only | Provides the reduced surfaces used by stock Ably clients and AI Transport smoke tests. |
| Mutable output streaming | Supported for AI Transport only | Ably mutable message calls map to Sockudo versioned messages. |
| History and recovery | Supported for AI Transport only | Harness covers history projection and recovery of missed append fragments. |
| Channel modes/capabilities | Not yet implemented | Attach accepts requested SDK modes; full Ably mode enforcement is not claimed. |
| LiveObjects/object modes | Intentionally out of scope | Not part of the current compatibility target. |
| Ably Push | Intentionally out of scope | Sockudo native push is separate from Ably Push compatibility. |
| Binary MsgPack Ably protocol | Not yet implemented | Harness uses JSON protocol. |
| Full Ably platform API parity | Intentionally out of scope | The claim is limited to AI Transport compatibility. |

## Stable Commands

Start Sockudo with:

```bash
cargo run -p sockudo --features "v2,ai-transport,ably-compat,redis,postgres,push" -- \
  --config config/config.toml
```

Then run:

```bash
make ably-compat-test
make ably-ai-transport-test
make ably-ai-demo
```

## Compatibility Claim

Current status: **Ably AI Transport compatible through an opt-in reduced Ably Pub/Sub subset**.

Preferred public wording:

- **Ably AI Transport compatible**
- **Ably Pub/Sub subset for AI Transport**

Avoid "complete Ably support" or "full Ably compatibility" until the scorecard proves it.
