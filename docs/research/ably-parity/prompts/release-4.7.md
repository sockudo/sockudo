# Release 4.7 Prompt Pack

## Prompt 4.7-A: Architecture and acceptance

```text
You are implementing Sockudo release 4.7: AI Interaction Control And Collaboration.

Context:
- Repository: /Users/radudiaconu/Desktop/Code/Rust/sockudo
- Follow all AGENTS.md instructions.
- Use:
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-ai-transport-research.md
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md
- Assume release 4.6 AI Transport Core is in place.

Before coding, design the release 4.7 interaction model.

It must cover:
- transcript branching model
- edit and regenerate semantics
- interruption and barge-in policies
- concurrent turns
- double texting behavior
- tool call persistence model
- human-in-the-loop approval state machine
- reasoning/chain-of-thought stream classification and policy model
- integration point to push notifications for background completion or approval requests

Acceptance criteria must explicitly define:
- how branches are created, identified, navigated, and retained
- how interrupted turns are marked and terminated
- whether double texting queues, forks, or runs concurrently and under which policy
- how tool calls and tool results persist across reconnect and device switch
- how approval requests are resumed after reconnect
- how reasoning streams are protected from accidental exposure where policy forbids it

Do not start implementation until the transcript tree and interruption policy are explicit and testable.
```

## Prompt 4.7-B: Implementation

```text
Implement Sockudo release 4.7: AI Interaction Control And Collaboration.

Ship the full scope:
- transcript branching
- edit-and-regenerate flows
- interruption and barge-in
- double texting policy support
- concurrent turns
- durable tool calling and tool result messages
- human-in-the-loop approval/reject flows
- reasoning/chain-of-thought message class or stream support
- optional push-trigger hooks for offline re-engagement when approvals or long-running completions occur
- docs and examples for each behavior

Implementation standards:
- branches must be durable transcript structure, not UI-only state
- tool calls must survive reconnect and multi-device handoff
- interruption must be turn-aware and deterministic
- concurrent turns must not corrupt history or branch order
- reasoning streams must support policy gating

Minimum tests:
- edit then regenerate creates a new branch without losing prior history
- interruption cancels or redirects exactly per policy
- two overlapping user prompts behave correctly under the selected double-texting policy
- concurrent turns remain independently cancellable
- tool call pending state survives reconnect
- approval on one device resolves the pending tool state on another device
- background completion can emit a push-trigger event if configured

Required docs:
- transcript tree and branch semantics
- tool calling contract
- HITL flow
- interruption policies
- reasoning stream policy guidance
```

## Prompt 4.7-C: Verification and docs hardening

```text
Audit and harden Sockudo release 4.7.

Verify:
- branch navigation remains coherent under replay and reconnect
- edit/regenerate never mutates prior history destructively
- interruption behavior is consistent and documented
- concurrent turns do not leak state into one another
- tool calling and HITL flows are durable and recoverable
- reasoning streams are correctly classified and gated
- push-trigger hooks do not create duplicate notifications or duplicate state transitions

Find defects, fix them, then report with:
- findings first if any
- exact evidence
- remaining risks
- readiness statement for SDK/framework publication in 4.8
```

## Prompt 4.7-D: Release readiness audit

```text
Perform a release-readiness audit for Sockudo release 4.7.

Determine whether Sockudo now has a complete advanced AI interaction layer suitable for public SDKs.

Audit dimensions:
- transcript tree integrity
- interruption and concurrency semantics
- tool/HITL durability
- reasoning stream policy safety
- user experience coherence across devices
- operational debuggability

Deliver:
- go/no-go
- blocker list
- release note draft
- explicit callout of any remaining product-policy decisions needed before broad SDK exposure
```
