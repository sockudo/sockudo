# Ably AI Transport Compatibility Test Plan

This plan separates Sockudo-native AI Transport verification from the optional Ably compatibility
facade. Native AI Transport uses Cargo feature `ai-transport`; the Ably facade additionally requires
Cargo feature `ably-compat`.

## Required Local Service

```bash
cargo run -p sockudo --features "v2,ai-transport,ably-compat,redis,postgres,push" -- \
  --config config/config.toml
```

The default repository config uses app key `app-key`, app secret `app-secret`, port `6001`, and AI
channel prefix `private-ai-`.

## Stable Commands

```bash
make ably-compat-test
make ably-protocol-discovery
make ably-ai-transport-test
make ably-ai-demo
```

The `make` targets use `tests/ably-compat`, stock `ably@2.23.0`, and
`@ably/ai-transport@0.4.0`. The Ably facade is expected at the Sockudo root WebSocket/REST endpoint;
existing Sockudo/Pusher clients continue to use `/app/{appKey}` and `/apps/{appId}`.
`make ably-protocol-discovery` also installs Playwright Chromium and runs one stock browser bundle
Pub/Sub/history lane from an allowed localhost origin.

## Required Test Cases

| Case | Command |
| --- | --- |
| Ably Realtime connect, attach, publish, receive, history, detach | `make ably-compat-test` |
| Broader stock `ably` SDK discovery across JSON, REST publish/history, presence, token, token capability enforcement, MsgPack, and browser lanes | `make ably-protocol-discovery` |
| Ably mutable message create/append/update/delete/version reads | `make ably-compat-test` |
| Ably recovery/history replay during streamed output | `make ably-compat-test` |
| Stock `@ably/ai-transport` chat smoke | `make ably-ai-transport-test` |
| Headless chat and recovery demos | `make ably-ai-demo` |

## Claim Gate

Do not claim full Ably support from these tests. The passing claim is:

> Sockudo is Ably AI Transport compatible through an opt-in reduced Ably Pub/Sub subset.

Broader compatibility claims require upstream Ably SDK test evidence and updated scorecard entries.
