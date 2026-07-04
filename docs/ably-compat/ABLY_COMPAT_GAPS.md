# Ably Compatibility Gaps

This file records known gaps in Sockudo's opt-in `ably-compat` facade. It is intentionally scoped to
the reduced Ably Pub/Sub subset needed by AI Transport tests and demos.

## Not Claimed

| Gap | Status | Notes |
| --- | --- | --- |
| Full Ably platform API parity | Intentionally out of scope | Sockudo is not claiming to replace every Ably service or endpoint. |
| Ably Push compatibility | Intentionally out of scope | Sockudo native push remains separate. |
| LiveObjects/object modes | Intentionally out of scope | Not required for the current AI Transport target. |
| Binary MsgPack protocol | Not yet implemented | Current harness forces JSON protocol. |
| Full channel mode/capability enforcement | Not yet implemented | Sockudo app auth remains authoritative. |
| Full Ably presence-history parity | Not yet implemented | Realtime presence frames are supported for the reduced path. |
| Byte-for-byte Ably serial internals | Not yet implemented | Sockudo emits stable compatible serials for the tested history/recovery path. |

## Required Before Broader Claims

- Run the current unmodified Ably AI Transport SDK test suite, or a documented equivalent subset,
  against Sockudo.
- Keep the `ably-compat` feature disabled in native-only AI Transport builds.
- Keep Pusher V1 and Sockudo V2 conformance green with and without `ably-compat`.
- Expand docs only when supported by tests in `tests/ably-compat` or the upstream SDK suite.
