# Release 4.6 Production Prompt Pack

## Prompt 4.6-01: Architecture and release contract

```text
You are implementing Sockudo release 4.6: AI Transport Core.

Read and follow all AGENTS.md instructions.

Primary context:
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-ai-transport-research.md
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md

Before coding, produce the release contract for:
- sessions
- turns
- server transport
- client transport
- token streaming
- cancellation
- reconnect/recovery
- history hydration
- optimistic updates
- agent presence

The contract must explicitly state goals, non-goals, invariants, and blockers.
```

## Prompt 4.6-02: Session, turn, and transcript data model

```text
Design and implement the release 4.6 data model.

Scope:
- session identity
- turn identity
- turn lifecycle state
- transport metadata
- transcript item structure
- active-turn tracking
- persisted versus ephemeral fields
- relationship to existing mutable-message and annotation infrastructure

Requirements:
- durable sessions independent of any single connection
- durable turn records
- replayable transcript state
- compatibility with later branching work
```

## Prompt 4.6-03: AI transport wire contract and protocol surfaces

```text
Implement the initial AI transport protocol contract for release 4.6.

Scope:
- Sockudo-native wire metadata for sessions and turns
- event/message shapes for user messages, agent outputs, control signals, and turn end states
- HTTP entrypoint contract for initiating turns if applicable
- channel attachment and replay expectations
- error semantics

Rules:
- keep the wire contract framework-neutral
- keep it V2-native
- make it extensible enough for later branching, tool calling, and SDK work
```

## Prompt 4.6-04: Auth, capabilities, and cancellation security

```text
Implement the release 4.6 auth and capability model.

Scope:
- session access control
- participant identity
- agent identity where applicable
- turn creation authorization
- turn cancellation authorization
- separation of user, agent, and admin capabilities

Requirements:
- cancellation must be explicitly authorized
- identity must survive reconnect and multi-device use
- control-plane messages must not become spoofable
```

## Prompt 4.6-05: Server transport implementation

```text
Implement the server-side transport for release 4.6.

Ship:
- session attach/use model
- turn creation
- user message persistence
- agent token streaming publication
- append rollup or equivalent efficiency layer
- turn completion
- turn cancellation routing
- history publishing consistency

Do not stop at scaffolding. Complete the server transport behavior end to end.
```

## Prompt 4.6-06: Client transport implementation

```text
Implement the client-side transport behavior for release 4.6.

Ship:
- subscribe-before-attach or equivalent no-gap startup behavior
- active turn tracking
- reconnect and short-gap recovery
- long-gap history hydration
- optimistic user message insertion
- reconciliation after server confirmation
- multi-device consistency behavior
- agent presence consumption

The client transport must be durable-session aware, not just a thin channel wrapper.
```

## Prompt 4.6-07: Test matrix and automation

```text
Build the full verification matrix for release 4.6.

Coverage must include:
- simple prompt-response session
- reconnect mid-stream
- long disconnect with history hydration
- multi-device session continuity
- cancellation by authorized actor
- cancellation rejection for unauthorized actor
- optimistic send reconciliation
- presence state transitions
- cluster scenarios if transport spans nodes

Run the relevant suites and fix issues before reporting completion.
```

## Prompt 4.6-08: Observability and operator surfaces

```text
Add observability for release 4.6.

Scope:
- metrics for sessions, turns, cancellations, recoveries, and hydrations
- metrics for optimistic reconciliation anomalies
- logs for turn lifecycle transitions
- diagnostics for stuck active turns
- diagnostics for replay/hydration failures
- operator guidance for debugging AI transport sessions
```

## Prompt 4.6-09: Docs, examples, and onboarding

```text
Complete the release 4.6 docs set.

Required docs:
- AI Transport overview
- sessions and turns
- auth setup
- cancellation model
- recovery behavior
- multi-device behavior
- optimistic update behavior
- agent presence
- end-to-end example showing durable AI session flow
```

## Prompt 4.6-10: Final verification and release audit

```text
Run the final release 4.6 verification and audit.

Prove:
- sessions are durable
- turns are well-modeled
- recovery works
- cancellation is safe
- optimistic updates reconcile correctly
- docs are sufficient

Deliver:
- findings first if any
- verification evidence
- go/no-go
- blockers if no-go
- release note draft
- explicit readiness statement for release 4.7 advanced interaction work
```
