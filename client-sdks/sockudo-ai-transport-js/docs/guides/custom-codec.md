# Custom Codec

Use the core helpers when the Vercel `UIMessageCodec` is not the right projection.

1. Use `createEncoderCore` to publish creates, appends, terminal updates, and recovery updates.
2. Use `createDecoderCore` to fold `sockudo:message.create|append|update|delete` into domain events
   without JSON round-tripping opaque payloads.
3. Use `createLifecycleTracker` to track `streaming`, `complete`, and `cancelled` status.
4. Keep custom keys under `extras.ai.codec`; transport keys are reserved and validated by Sockudo.

The codec must preserve deterministic folding: two clients that see the same serial-ordered stream
must materialize the same message state.
