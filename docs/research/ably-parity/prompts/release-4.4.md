# Release 4.4 Prompt Pack

## Prompt 4.4-A: Architecture and acceptance

```text
You are implementing Sockudo release 4.4: Annotation Engine And Summary Model.

Context:
- Repository: /Users/radudiaconu/Desktop/Code/Rust/sockudo
- Follow all AGENTS.md instructions.
- Behavioral target is parity with Ably message annotations and summaries, not API compatibility.
- Use:
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-message-annotations-research.md
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md
- Assume release 4.3 exists and is the mutation/history substrate.

Before editing code, produce the release 4.4 design and acceptance package.

It must cover:
- annotation storage model
- annotation identity model
- summary rollup model
- clipping strategy for oversized summaries
- subscription modes for raw annotation events versus rolled-up summaries
- authenticated identity requirements for each summarizer type
- performance and fanout implications
- cluster-safe rollup and delete behavior

Acceptance criteria must explicitly cover:
- `total`
- `flag`
- `distinct`
- `unique`
- `multiple`
- raw annotation publish/delete
- summary publish/update behavior
- clipped summaries
- named versus unnamed annotations
- counted annotations

Do not start implementation until the design explains exactly how summaries stay correct under concurrent annotation churn and cross-node delivery.
```

## Prompt 4.4-B: Implementation

```text
Implement Sockudo release 4.4: Annotation Engine And Summary Model.

Ship the complete release scope:
- annotation publish API
- annotation delete API
- raw annotation persistence
- summary rollup projection
- Sockudo-native annotation type format with namespace + summarizer + version semantics
- raw annotation subscription mode
- summary delivery mode
- clipping signaling for oversized participant lists
- examples and docs for read receipts, reactions, moderation flags, and scored annotations

Behavioral requirements:
- summaries are derived state from annotation events
- raw annotation payloads are available only to raw annotation subscribers
- summary payloads exclude raw annotation `data`
- identified-user requirements are enforced for the summarizers that need them
- delete operations remove the prior contribution from the effective summary
- `unique` semantics move a user between named buckets rather than duplicating them
- `multiple` semantics preserve per-client counts and unidentified totals

Tests must include:
- concurrent publish/delete churn for the same message
- two users competing on `unique`
- one user contributing to multiple `distinct` names
- multiple counts and `totalUnidentified` for `multiple`
- clipped summary output with correct totals
- recovery and history interaction with summary messages
- cluster replication and reconciliation

Docs must include:
- protocol reference additions
- channel mode and subscription guidance
- summary semantics and clipping behavior
- operator notes on persistence and cost impact
```

## Prompt 4.4-C: Verification and docs hardening

```text
Audit and harden Sockudo release 4.4: Annotation Engine And Summary Model.

Verify all of the following:
- summaries remain correct after reconnect and replay
- raw annotation subscriptions cannot accidentally replace normal message subscriptions
- clipping does not corrupt totals
- `unique` and `multiple` semantics are correct under concurrent updates
- cluster behavior converges to the same effective summary on every node
- docs accurately describe access patterns and identity requirements

If anything is incomplete or misleading, fix it before reporting completion.

Final report requirements:
- findings first if any
- then exact verification evidence
- then the final status of release 4.4 readiness
```

## Prompt 4.4-D: Release readiness audit

```text
Perform a release-readiness audit for Sockudo release 4.4.

Determine whether the annotation system is good enough to support:
- reactions
- read receipts
- moderation state
- future AI transcript metadata overlays

Audit for:
- data model stability
- subscription ergonomics
- summary correctness
- history/recovery integrity
- operational risk

Deliver:
- go/no-go decision
- any blockers
- release note draft
- explicit statement of whether 4.4 is strong enough to be reused by later AI Transport releases
```
