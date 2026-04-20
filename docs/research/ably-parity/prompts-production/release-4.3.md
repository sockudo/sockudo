# Release 4.3 Production Prompt Pack

## Prompt 4.3-01: Architecture and release contract

```text
You are implementing Sockudo release 4.3: Versioned Durable Messages.

Read and follow all AGENTS.md instructions in scope.

Primary context:
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-message-annotations-research.md
- /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md

Goal:
- Establish the production-ready substrate for mutable durable messages in Sockudo without copying Ably APIs.

Before code changes, produce a release contract markdown artifact that defines:
- goals and non-goals
- V1 versus V2 boundaries
- supported message actions
- permanent message identity model
- version identity model
- latest-visible-version model
- history-position semantics
- conflict-resolution semantics
- append semantics
- acceptance criteria
- explicit release blockers

Do not start implementation until the release contract is complete, coherent, and grounded in the current repo architecture.
```

## Prompt 4.3-02: Storage and data model

```text
Design and implement the storage/data model for Sockudo release 4.3.

You must define and then build:
- original message record shape
- current/latest visible version shape
- version history record shape
- ordering keys
- indexes needed for latest lookup and version lookup
- compatibility with current durable-history infrastructure
- retention behavior
- migration path for existing history stores

Requirements:
- preserve original history position while allowing latest-version substitution on reads
- support deterministic winner selection across nodes
- support append rollup retrieval
- avoid data duplication where possible without sacrificing query correctness

Deliver:
- in-repo design artifact
- implementation
- focused tests for storage invariants
```

## Prompt 4.3-03: Protocol, HTTP, and wire contract

```text
Implement the protocol and API contract for release 4.3.

Scope:
- Sockudo-native REST/HTTP mutation APIs for update/delete/append
- Sockudo-native realtime mutation events and action fields
- request/response schemas
- validation rules
- error behavior
- version retrieval endpoints or equivalent methods
- wire-visible V2 semantics for create/update/delete/append

Rules:
- do not break V1 compatibility
- keep all mutable-message behavior V2-native unless there is a strong reason otherwise
- document the action model and version metadata clearly

Tests:
- endpoint validation
- realtime event shape
- history retrieval shape
- version retrieval shape
```

## Prompt 4.3-04: Authorization and capabilities

```text
Implement the auth and capability model for release 4.3.

Scope:
- own-versus-any mutation capabilities
- identity requirements for own semantics
- authenticated user/client binding
- permission checks for update/delete/append
- failure behavior and error codes

Requirements:
- ownership must be identity-based, not connection-instance-based
- unauthenticated or weakly identified actors must not slip through own-scoped mutation paths
- cluster and replay paths must not bypass permission guarantees

Tests:
- update-own success
- update-own rejection
- delete-own success
- delete-own rejection
- append-own success
- append-own rejection
- any-scoped privileged success paths
```

## Prompt 4.3-05: Server/runtime implementation

```text
Implement the server/runtime behavior for release 4.3.

Ship:
- update processing
- delete processing
- append processing
- version serial assignment
- conflict resolution
- history materialization
- latest version lookup
- version history lookup
- cluster convergence behavior

Performance and correctness requirements:
- preserve ordering where required
- keep append paths efficient
- avoid unbounded in-memory reconciliation state
- ensure replay/recovery paths remain correct

Do not stop at partial handlers. Complete the full runtime flow.
```

## Prompt 4.3-06: SDK/client behavior planning hooks

```text
Prepare the client-facing behavior for release 4.3, even if full SDK work lands later.

Scope:
- define how clients should interpret action fields
- define replace-versus-concatenate semantics
- define latest-version behavior for history readers
- define version-history retrieval expectations
- add any minimal client-facing examples or helper guidance needed now

Deliver:
- protocol-consumer guidance in docs
- any small helper abstractions needed in existing Sockudo client surfaces or examples

This prompt is about making the release usable, not shipping full AI Transport SDKs yet.
```

## Prompt 4.3-07: Test matrix and automation

```text
Build the full verification matrix for release 4.3.

Minimum coverage:
- publish then update
- publish then delete
- publish then append many times
- racing update/update
- racing update/delete
- append under reconnect
- latest history view correctness
- message versions retrieval correctness
- cluster cross-node convergence
- authorization matrix
- V1 non-regression

You must:
- add or extend unit tests
- add integration tests where runtime/history interactions matter
- add clustered tests where convergence matters
- run the narrowest verification first, then broaden

If verification exposes defects, fix them before closing the release.
```

## Prompt 4.3-08: Observability and operator surfaces

```text
Add production-grade observability for release 4.3.

Scope:
- metrics for mutation attempts and outcomes
- metrics for version conflicts and superseded writes
- logs for high-risk mutation transitions
- diagnostics for history/version retrieval mismatches
- operator-visible signals for degraded history or recovery interaction

Requirements:
- instrumentation must be actionable
- avoid generic noisy counters with no operational value
- document how operators should interpret the new signals
```

## Prompt 4.3-09: Docs, examples, and migration notes

```text
Complete the release 4.3 documentation set.

Required updates:
- protocol reference
- HTTP endpoint reference
- history docs
- recovery docs if affected
- compatibility docs explaining this is Sockudo-native V2 behavior
- at least one worked example of update/delete/append usage
- migration notes for users moving from immutable-only mental models

The docs must explain:
- action semantics
- shallow-mixin behavior
- append replace-versus-concatenate rules
- latest-version versus version-history behavior
- authorization model
```

## Prompt 4.3-10: Final verification and release audit

```text
Run the final verification and release audit for Sockudo release 4.3.

You must prove:
- the mutable-message substrate is production-ready
- history and replay are correct
- cluster convergence is deterministic
- authorization is enforced
- docs are in sync with behavior

Deliver:
- findings first if any
- exact verification evidence
- release go/no-go
- blocker list if no-go
- release note draft
- explicit readiness statement for releases 4.4 and 4.6
```
