# Release 4.3 Prompt Pack

## Prompt 4.3-A: Architecture and acceptance

```text
You are implementing Sockudo release 4.3: Versioned Durable Messages.

Context:
- Repository: /Users/radudiaconu/Desktop/Code/Rust/sockudo
- Read and follow all AGENTS.md instructions in scope.
- Behavioral target is feature parity with Ably message updates, deletes, appends, message actions, latest-version history semantics, and message version retrieval.
- Do not implement Ably API compatibility. Preserve Sockudo-native APIs and naming.
- Use these internal source documents as the product brief:
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-message-annotations-research.md
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md

Mission:
Produce the full release 4.3 architecture and acceptance package before code changes begin.

Required outputs:
1. A concise architecture note covering:
   - message identity model
   - version identity model
   - storage layout for original message versus latest version versus version history
   - cluster convergence and winning-version rules
   - update/delete shallow-mixin semantics
   - append aggregation semantics for realtime versus history
   - authorization model for own versus any mutations
2. A file-by-file implementation plan naming the exact crates, modules, tests, docs, and API surfaces to touch.
3. A risk register with concrete mitigations for:
   - race conditions
   - history corruption
   - replay/recovery regressions
   - append ordering issues
   - protocol V1/V2 leakage
4. A release acceptance checklist that is specific, testable, and complete.

Constraints:
- Reuse existing history, recovery, and extras patterns where viable.
- Prefer deletion and extension of existing abstractions over introducing parallel systems.
- Do not defer clustered correctness or history semantics.
- If the current durable-history model is insufficient, say exactly what must change.

Verification requirements for this planning step:
- Confirm which existing crates already own history, recovery, and message serialization behavior.
- Confirm which current docs and tests are already closest to the target behavior.

Deliver the architecture and acceptance package in-repo as markdown under a release-4.3 planning path, then summarize the plan.
```

## Prompt 4.3-B: Implementation

```text
Implement Sockudo release 4.3: Versioned Durable Messages.

Context:
- Repository: /Users/radudiaconu/Desktop/Code/Rust/sockudo
- Follow all AGENTS.md instructions.
- Behavioral target is the release 4.3 scope in:
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/ably-message-annotations-research.md
  - /Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md
- Use the architecture artifact produced for release 4.3 as binding implementation guidance.

Ship all of the following in one coherent release slice:
- stable top-level message identity
- per-version identity and ordering
- Sockudo-native update/delete/append APIs
- message action model with create/update/delete/append semantics
- latest-version replacement in history at the original message position
- version-history retrieval API
- append behavior where history yields full rolled-up state while realtime can deliver incremental state
- mutation authorization with own-versus-any semantics tied to authenticated user identity
- clustered winning-version convergence
- tests and docs for all of the above

Implementation standards:
- Keep protocol V1 behavior untouched unless a V2-native path is explicitly required.
- Avoid ad hoc per-feature state; use durable, queryable models.
- Treat append flows as latency-sensitive and recovery-sensitive.
- If you need a new internal protocol/event shape, keep it V2-native and documented.

Minimum test coverage:
- original publish then update
- original publish then delete
- original publish then multiple appends
- racing update/update
- racing update/delete
- append stream with reconnect/recovery
- version-history retrieval correctness
- latest history view shows winning visible version while version history preserves prior versions
- own/any mutation authorization
- clustered cross-node convergence

Required docs updates:
- HTTP/API reference for mutation endpoints or methods
- protocol reference for new V2 action semantics
- server docs for history/version behavior
- compatibility notes explaining this is Sockudo-native rather than Pusher-compatible behavior

Run the narrowest meaningful verification first, then widen to the full set needed to credibly claim the release is implemented.
```

## Prompt 4.3-C: Verification and docs hardening

```text
Audit and harden Sockudo release 4.3: Versioned Durable Messages.

Scope:
- Review the completed implementation for correctness, regressions, and documentation gaps.
- Use the release 4.3 acceptance checklist and product brief as the source of truth.

You must verify:
- history semantics match the intended latest-version behavior
- append behavior is consistent between realtime delivery and history retrieval
- intermediate mutation conflation does not violate final-state correctness
- recovery and replay are not broken by versioned messages
- cluster ordering is deterministic
- all mutation authorization paths are enforced
- docs are accurate and complete

Required actions:
1. Run focused tests for touched crates and integration paths.
2. Run broader workspace verification where cross-crate contracts changed.
3. Identify any remaining risks or untested paths with precision.
4. Tighten comments, names, and docs only where necessary to improve clarity.
5. If defects are found, fix them before reporting.

Output format:
- findings first, ordered by severity if any exist
- then change summary
- then exact verification evidence
- then explicit remaining risks or zero-risk statement if genuinely none
```

## Prompt 4.3-D: Release readiness audit

```text
Perform a release-readiness audit for Sockudo release 4.3: Versioned Durable Messages.

Goal:
- Decide whether 4.3 is actually fit to serve as the substrate for annotations and AI Transport.

Audit dimensions:
- storage durability
- clustered convergence
- history correctness under churn
- recovery correctness under mutation traffic
- API ergonomics
- documentation completeness
- operational visibility

Required deliverable:
- a go/no-go recommendation
- a short punch list of any blockers
- if go, a concise release note draft describing what 4.3 adds and how users should think about it

Do not restate implementation details. Focus on release fitness and downstream dependency readiness for releases 4.4 and 4.6.
```
