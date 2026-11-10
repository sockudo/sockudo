# Changelog

## 0.1.0 - Unreleased

- Initial release of the official async Rust realtime client (`sockudo-client`), built on
  `sockudo-ws` and `sonic-rs`.
- Protocol V2 by default with strict Protocol V1 (Pusher) compatibility; JSON, MessagePack and
  Protobuf wire formats verified against the server's `sockudo-protocol` codecs.
- Public, private, presence and encrypted channels; HTTP-endpoint or custom-handler channel
  authorization and user authentication; user sign-in, server-to-user events and watchlist.
- Capability tokens (static, provider, URL) with proactive and `40142` refresh and `40160`
  revocation handling.
- Tag/event/JMESPath filters, rewind, annotation subscriptions, Fossil and Xdelta3 delta
  compression with conflation keys and resync, continuity-aware recovery with persistable
  positions, and message deduplication.
- Proxy-backed channel history, presence history/snapshots, message versions, annotations and
  versioned-message mutations; client-side mutable-message reducer; push registration client.
- Reconnect with quadratic backoff, jitter, bounded attempts and Pusher close-code semantics.
- permessage-deflate WebSocket compression and opt-in connection timeline telemetry.
- Delta compression is rejected with binary wire formats, at build time and for per-subscription
  settings, matching the server's refusal.
- Null `extras.headers` values and string `extras.ai.codec` ids in every wire format.
