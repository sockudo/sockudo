# Release 4.4 Production Prompt Pack

## Prompt 4.4-01: Architecture and release contract

```text
You are implementing Sockudo release 4.4: Annotation Engine And Summary Model.

Read and follow all AGENTS.md instructions.

Primary context:
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-message-annotations-research.md
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md

Assume release 4.3 exists.

Before coding, produce the release contract defining:
- annotation goals and non-goals
- raw annotation event model
- summary projection model
- supported summarizers
- named versus unnamed annotation rules
- identified-user requirements
- clipping behavior
- summary delivery behavior
- release acceptance criteria and blockers
```

## Prompt 4.4-02: Storage and data model

```text
Design and implement the storage model for release 4.4.

You must cover:
- raw annotation record schema
- annotation identity and deletion linkage
- summary projection storage or materialization strategy
- per-message annotation indexing
- cluster-safe rollup strategy
- retention and replay interaction

Requirements:
- raw annotation history must remain queryable if required by future features
- summaries must be derivable and convergent
- storage design must support high annotation churn without corrupting counts
```

## Prompt 4.4-03: Protocol and subscription contract

```text
Implement the protocol and subscription contract for release 4.4.

Scope:
- annotation publish/delete APIs
- raw annotation event shape
- summary message shape
- channel mode or subscription controls for raw annotations
- action semantics for annotation.create, annotation.delete, and summary delivery
- error and validation behavior

Rules:
- do not overload normal subscriptions in a confusing way
- make raw event subscription explicit
- keep message summaries consumable from ordinary message flows where appropriate
```

## Prompt 4.4-04: Authorization and identity requirements

```text
Implement release 4.4 auth rules.

Scope:
- identity requirements per summarizer type
- permission model for publish/delete annotation actions
- validation of client identity on distinct/unique/flag semantics
- safe handling of unidentified contributors for summarizers that allow them

Tests must prove:
- identified-only summarizers reject anonymous misuse
- allowed unidentified paths still aggregate correctly
- deletes remove only the intended contribution
```

## Prompt 4.4-05: Server/runtime implementation

```text
Implement the full annotation runtime for release 4.4.

Ship:
- annotation create path
- annotation delete path
- summary projection updates
- clipping logic
- summary emission
- replay/recovery support
- cluster convergence logic

Correctness requirements:
- `unique` must move a user between named buckets
- `multiple` must preserve per-client counts and unidentified totals
- delete semantics must update summaries correctly under churn
```

## Prompt 4.4-06: Client and consumer behavior guidance

```text
Make release 4.4 usable for clients and application developers.

Scope:
- clear client-consumption examples for summaries
- clear client-consumption examples for raw annotation events
- guidance for reactions, receipts, moderation, and scores
- any minimal helper surfaces or examples needed now

Do not leave the release as a raw protocol feature with no practical consumption guidance.
```

## Prompt 4.4-07: Test matrix and automation

```text
Build the complete verification matrix for release 4.4.

Required coverage:
- all five summarizers
- named and unnamed behavior
- publish/delete churn
- clipping behavior
- reconnect and replay
- clustered concurrency
- invalid identity paths
- summary correctness after many operations

Run the relevant test suites and fix issues before reporting completion.
```

## Prompt 4.4-08: Observability and operator surfaces

```text
Add observability for release 4.4.

Scope:
- metrics for annotation create/delete volume
- metrics for summary recomputation or projection failures
- clipping occurrence visibility
- logs for invalid annotation operations and projection anomalies
- diagnostics for summary corruption or mismatch

Document how operators should investigate annotation issues.
```

## Prompt 4.4-09: Docs, examples, and product guidance

```text
Complete the release 4.4 docs set.

Required updates:
- protocol reference for annotations
- subscription mode docs
- summary semantics docs
- clipping docs
- example recipes for reactions, read receipts, moderation, and voting
- operational notes on persistence and storage cost

Docs must be strong enough that later AI transcript metadata features can reuse them as precedent.
```

## Prompt 4.4-10: Final verification and release audit

```text
Run the final release 4.4 verification and audit.

Prove:
- annotation semantics are correct
- summaries are convergent
- clipping is safe
- raw event subscriptions work
- docs are complete

Deliver:
- findings first if any
- evidence
- go/no-go
- blockers if no-go
- release note draft
- explicit readiness statement for downstream AI metadata reuse
```
