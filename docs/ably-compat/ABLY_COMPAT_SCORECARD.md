# Ably AI Transport Compatibility Scorecard

Discovery date: 2026-07-04

Status: reduced compatibility implementation added for the Ably AI Transport SDK smoke path. When
built with Cargo feature `ably-compat`, Sockudo exposes an additive Ably Realtime/REST facade while
the native Sockudo/Pusher `/app/{appKey}` WebSocket route remains unchanged.

Sockudo should not claim broad Ably platform compatibility until the full Ably package tests and
Sockudo V1/V2 conformance tests pass against pinned target versions.

## Target Versions

| Surface | Target | Evidence |
| --- | --- | --- |
| Ably AI Transport package | `@ably/ai-transport@0.4.0` | Smoke harness in `tests/ably-compat`. |
| Ably Realtime JS peer | `ably@2.23.0` | Smoke and protocol discovery harnesses in `tests/ably-compat`. |
| Sockudo server workspace | `4.7.0` | Root Cargo workspace package version. |

## Compatibility Summary

| Area | Status | Notes |
| --- | --- | --- |
| Pusher Protocol V1 compatibility | Supported | Ably compatibility is additive and must not alter V1 wire behavior. |
| Sockudo-native AI Transport | Supported | Uses Cargo feature `ai-transport` plus runtime `[ai_transport]`; it does not require `ably-compat`. |
| Ably facade feature gate | Supported | Root Ably WebSocket route, Ably REST routes, and Ably realtime egress tap are compiled only with `ably-compat`. |
| Ably Realtime JSON/MsgPack protocol | Supported for AI Transport only | Handles the ProtocolMessage flows needed by the harness in JSON and MsgPack WebSocket modes. |
| Ably REST time/token/history/message reads and publish | Supported for AI Transport only | Provides the reduced surfaces used by stock Ably clients and AI Transport smoke tests in the tested JSON/MsgPack lanes. |
| Mutable output streaming | Supported for AI Transport only | Ably mutable message calls map to Sockudo versioned messages. |
| History and recovery | Supported for AI Transport only | Harness covers history projection and recovery of missed append fragments. |
| Channel modes/capabilities | Not yet implemented | Attach accepts requested SDK modes; full Ably mode enforcement is not claimed. |
| LiveObjects/object modes | Intentionally out of scope | Not part of the current compatibility target. |
| Ably Push | Intentionally out of scope | Sockudo native push is separate from Ably Push compatibility. |
| Binary MsgPack Ably protocol | Supported for AI Transport only | Covered for Realtime Pub/Sub and REST publish/history/time discovery lanes. |
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
make ably-protocol-discovery
make ably-ai-transport-test
make ably-ai-demo
```

`make ably-protocol-discovery` is not a support claim by itself. It records the
current stock `ably` SDK behavior across required and optional lanes. Required
lanes protect the claimed AI Transport Pub/Sub subset; optional failures remain
scorecard gaps until intentionally implemented.

## Protocol Discovery Baseline

Last local run: 2026-07-04 against `ably@2.23.0`, Sockudo built with
`v2,ai-transport,ably-compat,redis,postgres,push`.

Command:

```bash
ABLY_PROTOCOL_RUN_ID=realtime-msgpack npm run protocol:discovery
```

| Lane | Status | Notes |
| --- | --- | --- |
| `node-realtime-json-pubsub` | Supported | Required baseline for the current compatibility claim. |
| `node-rest-json-time` | Supported | Stock Ably REST `time()` returned a timestamp. |
| `node-rest-json-publish-history` | Supported | Stock Ably REST publish writes through the real publish path and history returns the message. |
| `node-rest-msgpack-publish-history` | Supported | Stock Ably REST publish/history passes with `useBinaryProtocol: true`. |
| `node-realtime-json-presence` | Supported | Enter, get, update, and leave passed for one client. |
| `node-auth-json-request-token` | Supported | Stock Ably token request returned a token. |
| `node-realtime-msgpack-pubsub` | Supported | Realtime Pub/Sub passes with `format=msgpack` binary ProtocolMessages. |
| `node-rest-msgpack-time` | Supported | REST time works with the SDK's MsgPack option. |
| `browser-chromium-json-pubsub` | Not run | Browser runner needs an explicit Playwright/browser dependency and CI setup. |

## Compatibility Claim

Current status: **Ably AI Transport compatible through an opt-in reduced Ably Pub/Sub subset**.

Preferred public wording:

- **Ably AI Transport compatible**
- **Ably Pub/Sub subset for AI Transport**

Avoid "complete Ably support" or "full Ably compatibility" until the scorecard proves it.
