# Release 4.7 Production Prompt Pack

## Prompt 4.7-01: Architecture and release contract

```text
You are implementing Sockudo release 4.7: AI Interaction Control And Collaboration.

Read and follow all AGENTS.md instructions.

Primary context:
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-ai-transport-research.md
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md

Before coding, produce the release contract for:
- branching
- edit/regenerate
- interruption and barge-in
- concurrent turns
- double texting
- tool calling
- human-in-the-loop
- chain-of-thought handling
- push-trigger integration

The contract must define exact behavioral policies and release blockers.
```

## Prompt 4.7-02: Conversation tree and state model

```text
Design and implement the conversation tree model for release 4.7.

Scope:
- branch identity
- parent/child relationships
- sibling navigation model
- regenerate semantics
- edit semantics
- flatten/projection strategy for UI consumption
- persistence and replay

Requirements:
- prior history must remain durable
- branches must be first-class transcript structure
- model must support later public SDK exposure
```

## Prompt 4.7-03: Protocol and control-plane contract

```text
Implement the protocol/control-plane contract for release 4.7.

Scope:
- branch creation events
- edit/regenerate events
- interruption signals
- concurrent-turn identifiers
- tool call and tool result events
- approval/reject events
- reasoning stream classification

Rules:
- make each control action durable where needed
- preserve replayability
- avoid ambiguous event semantics
```

## Prompt 4.7-04: Auth, policy, and safety rules

```text
Implement the release 4.7 auth and policy model.

Scope:
- who may edit
- who may regenerate
- who may interrupt
- who may resolve tool approvals
- who may access reasoning streams
- push-trigger authorization for background events

Requirements:
- sensitive reasoning output must support policy gating
- approval actions must be attributable and auditable
```

## Prompt 4.7-05: Runtime implementation for branching and interruption

```text
Implement the runtime for:
- branching
- edit/regenerate
- interruption
- barge-in
- concurrent turns
- double-texting policy

Behavior must be deterministic, replayable, and documented.

Do not stop at partial branch metadata. Complete the real runtime semantics.
```

## Prompt 4.7-06: Runtime implementation for tools and human-in-the-loop

```text
Implement the runtime for:
- tool call persistence
- tool result persistence
- pending approval states
- approval/reject resolution
- recovery of pending tool workflows after reconnect
- cross-device visibility of pending HITL state

This release is not complete until tool/HITL flows are durable and recoverable.
```

## Prompt 4.7-07: Test matrix and automation

```text
Build the full verification matrix for release 4.7.

Coverage must include:
- edit creates new branch
- regenerate creates new branch
- sibling branch navigation correctness
- interruption behavior
- concurrent turns
- double-texting policy outcomes
- tool call pending state survives reconnect
- approval on another device resolves state correctly
- reasoning stream policy enforcement
- background push-trigger events where configured
```

## Prompt 4.7-08: Observability and operator surfaces

```text
Add observability for release 4.7.

Scope:
- metrics for branch creation and concurrent turns
- metrics for interruptions and cancellations
- metrics for tool call pending/approved/rejected states
- logs for branch mutations and policy decisions
- diagnostics for stuck approval flows
- diagnostics for inconsistent branch state
```

## Prompt 4.7-09: Docs, examples, and product guidance

```text
Complete the release 4.7 docs set.

Required docs:
- conversation branching
- edit/regenerate
- interruption and barge-in
- concurrent turns
- double texting behavior
- tool calling
- human-in-the-loop
- reasoning stream policy guidance
- examples showing advanced AI interaction flows
```

## Prompt 4.7-10: Final verification and release audit

```text
Run the final release 4.7 verification and audit.

Prove:
- the transcript tree is coherent
- interruption and concurrency semantics are stable
- tools and HITL flows are durable
- reasoning policy controls exist
- docs are complete

Deliver:
- findings first if any
- verification evidence
- go/no-go
- blockers if no-go
- release note draft
- explicit readiness statement for public SDK/framework work in 4.8
```
