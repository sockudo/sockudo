# Release 4.6 Prompt Pack

## Prompt 4.6-A: Architecture and acceptance

```text
You are implementing Sockudo release 4.6: AI Transport Core.

Context:
- Repository: /Users/radudiaconu/Desktop/Code/Rust/sockudo
- Follow all AGENTS.md instructions.
- Target behavioral parity with Ably AI Transport core capabilities, not Ably API compatibility.
- Use:
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-ai-transport-research.md
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md
- Assume releases 4.3 and 4.4 exist. Reuse them rather than re-inventing mutation/history logic.

Before editing code, produce the architecture and acceptance package for release 4.6.

It must define:
- session-as-channel model
- turn identity and turn lifecycle
- server transport responsibilities
- client transport responsibilities
- authentication and capability model
- token streaming model
- reconnect/recovery model
- history hydration model
- optimistic message insertion and reconciliation model
- agent presence state model
- initial AI transport wire contract

Acceptance criteria must explicitly cover:
- session continuity across disconnects
- multi-device session attach
- turn-scoped cancellation
- append-rollup or equivalent streaming efficiency strategy
- long-gap history hydration
- optimistic message reconciliation
- authenticated user identity and cancel authorization

Do not proceed until the design explains exactly how AI turns are represented durably and how recovery avoids duplicate or missing streamed content.
```

## Prompt 4.6-B: Implementation

```text
Implement Sockudo release 4.6: AI Transport Core.

Ship all core capabilities:
- session channels for AI conversations
- durable turn model
- server transport for creating turns, streaming responses, ending turns, and receiving control signals
- client transport for subscribing before attach, decoding transport events, tracking active turns, and loading history
- turn-scoped cancellation with authorization hooks
- streaming patterns for full-response and incremental-token delivery
- reconnect/recovery for short disconnects
- history hydration for longer disconnects
- multi-device session continuity
- optimistic user message insertion and server reconciliation
- agent presence statuses such as thinking, streaming, idle, offline
- initial docs and examples

Implementation standards:
- keep AI Transport V2-native
- do not bind the transport core to a single AI framework
- isolate framework-specific concerns behind codec or adapter interfaces
- treat session durability and replay correctness as release blockers

Minimum tests:
- single-device happy path
- reconnect mid-stream and resume cleanly
- long disconnect and history hydration
- two devices in one session seeing the same turn lifecycle
- cancellation by authorized user
- cancellation rejected for unauthorized actor
- optimistic send reconciles without duplicates
- presence state transitions
- cluster-aware session continuity if the implementation touches horizontal paths

Required docs:
- product overview
- transport lifecycle
- auth setup
- session/turn semantics
- recovery behavior
- presence semantics

Do not stop at partial transport scaffolding. Complete the end-to-end release scope.
```

## Prompt 4.6-C: Verification and docs hardening

```text
Audit and harden Sockudo release 4.6: AI Transport Core.

Verify:
- the session is truly durable and not merely connection-bound
- turn-scoped cancellation is independent of session lifetime
- recovery is correct for short and long disconnects
- optimistic updates reconcile correctly
- multi-device state is consistent
- agent presence is accurate and does not drift
- docs explain the transport model clearly enough for SDK work in 4.8

If issues exist, fix them. Then provide:
- findings first if any
- exact verification evidence
- remaining risks
- readiness statement for advanced AI interaction features in release 4.7
```

## Prompt 4.6-D: Release readiness audit

```text
Perform a release-readiness audit for Sockudo release 4.6.

Question to answer:
- Is Sockudo's AI Transport substrate now strong enough to support branching, tool calls, edit/regenerate, and framework adapters without redesign?

Audit:
- session durability
- turn model stability
- cancellation model quality
- recovery integrity
- optimistic update correctness
- multi-device consistency
- auth and capability clarity

Deliver:
- go/no-go
- blocker list
- release note draft
- explicit statement of any architectural debts that would endanger 4.7 or 4.8
```
