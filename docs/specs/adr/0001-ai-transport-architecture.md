# ADR 0001: AI Transport Architecture

Status: Accepted for S1+ implementation
Date: 2026-06-02

## Context

Sockudo is closing Ably AI Transport platform gaps while preserving strict Pusher V1 compatibility and existing Protocol V2 behavior. A code audit found that Sockudo already has most required primitives: versioned mutable messages, durable history, rewind, two-tier recovery, annotations, idempotency surfaces, and push. The missing work is conventions, validation, capability tokens, append rollup, untilAttach/client history, and presence update/timeout semantics.

## Decision

AI Transport will be implemented as server platform primitives over existing Sockudo subsystems, not as a parallel AI message stack.

1. AI lifecycle events are plain channel events: `ai-input`, `ai-output`, `ai-turn-start`, `ai-turn-end`, and `ai-cancel`.
2. Streaming content uses existing `sockudo:message.create|append|update|delete` mutable-message events and `VersionStore`.
3. AI metadata is represented by `extras.ai.transport` and `extras.ai.codec`; the server validates transport headers and bounds codec headers without interpreting codec semantics.
4. New behavior is behind Cargo feature `ai-transport` and runtime `[ai_transport] enabled`.
5. V1 output remains byte-compatible. Existing V2 frames remain additive-only.
6. Append rollup is an egress optimization only; persistence, version storage, recovery, webhooks, and push see every original append.
7. Capability tokens are additive V2 JWT auth and do not replace existing HMAC app-key/private-channel auth.
8. Client history extends existing history semantics with untilAttach guarantees instead of replacing rewind or signed app HTTP history.

## Consequences

The SDK can map Ably AI Transport semantics onto Sockudo without the server understanding turns, trees, or codecs. The server remains responsible for identity, validation, ordering, recovery, bounded queues, and durable replay.

This choice keeps the broadcast path conservative. The largest new hot-path component is append rollup, which must be benchmarked and proven to preserve the same reduced mutable-message state as the unrolled append stream.

## Rejected Alternatives

Rejected: Introduce a separate AI message store | It would duplicate `VersionStore`, weaken recovery/history consistency, and create another persistence path.

Rejected: Encode AI semantics in new reserved `sockudo:ai.*` events | Plain channel events match the AI Transport model and avoid expanding the reserved Sockudo protocol namespace unnecessarily.

Rejected: Put AI headers under `extras.headers` | Existing headers are flat scalar values; `extras.ai.transport` and `extras.ai.codec` provide clear tiering while preserving the current extras envelope.

Rejected: Roll up before persistence | It would break durable replay, recovery, append idempotency, and exact state reconstruction.

Rejected: Make capability tokens replace HMAC auth | Existing SDKs and app-key workflows depend on HMAC semantics; JWT capabilities are additive and V2-only.

## Verification

The accompanying spec `docs/specs/ai-transport-wire-protocol.md` records audited source references for existing behavior and a conformance checklist for later implementation prompts.

Confidence: high
Scope-risk: moderate
Directive: Do not implement AI Transport primitives beside versioned messages, durable history, recovery, annotations, or push; extend those subsystems.
