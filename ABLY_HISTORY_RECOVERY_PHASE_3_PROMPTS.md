# Ably-Grade Message History, Recovery, And Rewind Prompt Pack
# Phase 3: Production Hardening, Distributed Safety, And Release Readiness

This file continues from:

- [`ABLY_HISTORY_RECOVERY_PROMPTS.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ABLY_HISTORY_RECOVERY_PROMPTS.md)
- [`ABLY_HISTORY_RECOVERY_PHASE_2_PROMPTS.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ABLY_HISTORY_RECOVERY_PHASE_2_PROMPTS.md)

Assumed completed before starting:

- phase-1 substrate and public surfaces exist
- phase-2 feature-completion work is complete, tested, and committed

Phase 3 is for closing the remaining production and distributed-systems risks so the stack can be treated as operationally mature.

Use these prompts in order. Each prompt assumes the previous one is complete, tested, and committed.

## Phase 3 Goals

- close the remaining distributed-failure risks
- make degradation signaling trustworthy across nodes
- add operator controls for stream inspection, reset, and repair
- expand backend coverage only where it materially improves product readiness
- add the missing transport-backed/chaos-style evidence for clustered behavior
- produce a release packet that is safe for broad production rollout

## Carry-Forward Constraints

Still non-negotiable:

- correctness beats silent false-success recovery
- no global publish locks
- preserve hot replay buffer as the reconnect fast path
- preserve raw payload fidelity
- no casual dependency/storage-engine sprawl
- prefer explicit degradation and backpressure over implicit behavior

## Current Risks Phase 3 Must Close

These are the main risks still left after phase 2:

- degraded-channel propagation is strongest only when cache is shared; node-local cache weakens clustered fail-closed behavior
- no full transport-backed clustered history/recovery harness exists yet
- cold recovery is correct but still not deeply optimized for very large reconnect gaps
- operator controls for stream purge/reset/repair are still minimal
- durable-store and cache partial-failure behavior still needs broader automated fault coverage
- additional durable backends may still be needed depending on product support expectations
- any SDKs not fully updated in phase 2 still block a fully professional release

## Prompt 1: Distributed Degradation Coordination Beyond Shared Cache

```md
Strengthen degraded-channel coordination so clustered recovery safety does not depend primarily on a best-effort shared cache marker.

First inspect:
- crates/sockudo-server/src/history.rs
- crates/sockudo-adapter/src/handler/recovery.rs
- crates/sockudo-adapter/src/horizontal_adapter_base.rs
- docs/content/2.server/23.multinode-history-ops.md

Goals:
- make degraded history/recovery state a first-class distributed safety mechanism
- define exactly which store/source is authoritative for:
  - stream degraded state
  - recovery allowed vs denied
  - stream reset required vs recoverable
- avoid split-brain where one node treats a channel as degraded and another continues cold recovery

Requirements:
- do not rely solely on node-local memory for degraded-state decisions
- if shared cache remains part of the design, clearly define its failure semantics
- prefer durable metadata when correctness depends on cross-node agreement
- make recovery/history fail closed when agreement cannot be reached

Deliver:
1. Code changes for stronger degraded-state ownership/propagation.
2. Clear operator/debug surface for the state.
3. Docs updates explaining the distributed source of truth.

Verification:
- automated tests for degraded-state propagation across simulated nodes
- tests proving cold recovery is blocked consistently when a channel is degraded
```

## Prompt 2: Full Transport-Backed Multinode History/Recovery Test Lane

```md
Build the strongest credible automated clustered test lane for history, rewind, and recovery using the repo’s existing horizontal adapter/transports.

First inspect:
- crates/sockudo-adapter/src/transports/*
- crates/sockudo-adapter/src/horizontal_adapter_base.rs
- existing horizontal adapter integration/failure/race tests

Goals:
- prove the real clustered behavior rather than only shared-store/local simulations
- specifically verify:
  - publish on node A, subscriber on node B
  - publish on node A, reconnect on node B
  - node restart with surviving durable history
  - replay buffer loss with cold recovery fallback
  - degraded durable path causing explicit recovery failure
  - rewind + live handoff across nodes

Requirements:
- use the narrowest real transport harness the repo can support without making CI impossible
- if a full transport matrix is too expensive, pick one representative clustered transport and explain why
- keep tests deterministic and bounded

Deliver:
1. Multinode integration tests.
2. Supporting harness code if needed.
3. Honest statement of what remains outside automated coverage.
```

## Prompt 3: Operator Controls For Stream Inspection, Reset, And Repair

```md
Add the operational controls needed to manage durable channel streams in production.

First inspect:
- crates/sockudo-server/src/http_handler.rs
- crates/sockudo-server/src/history.rs
- docs/content/2.server/23.multinode-history-ops.md

Goals:
- give operators a safe way to inspect stream metadata
- support intentional stream reset/rotation for irrecoverable channels
- support purge/truncate tooling where appropriate
- make destructive operations explicit, auditable, and hard to misuse

Requirements:
- any destructive control must be clearly authenticated/authorized
- do not add a vague admin API; every control needs explicit semantics
- resetting a stream must intentionally rotate `stream_id`
- docs must explain downstream effects on rewind/history/recovery

Possible outputs:
- authenticated admin HTTP endpoints
- CLI tooling
- debug endpoints plus documented manual SQL/ops flow

Deliver:
1. Operator control surface.
2. Tests for stream reset/purge behavior.
3. Documentation with warnings and expected client impact.
```

## Prompt 4: Fault Injection And Chaos Safety Review

```md
Add targeted fault-injection coverage for the history/recovery stack.

First inspect:
- durable history writer path
- recovery path
- horizontal adapter broadcast path
- replay buffer path

Goals:
- simulate the important failure modes directly in automated tests:
  - writer queue full
  - durable write failure
  - cache marker write/read failure
  - node restart
  - replay buffer loss
  - retention boundary crossing during reconnect
- confirm the system fails closed where it should

Requirements:
- prefer narrow deterministic failure injection over large flaky chaos setups
- every injected fault should map to an explicit expected client-visible outcome
- document any remaining blind spots

Deliver:
1. Fault-injection tests.
2. Failure matrix mapping fault -> expected history/rewind/recovery behavior.
3. Any code changes needed to make failure behavior cleaner and more explicit.
```

## Prompt 5: Durable Backend Expansion And Conformance

```md
Decide whether additional durable history backends are required for product readiness, and if so, add them with conformance tests.

First inspect:
- crates/sockudo-core/src/history.rs
- crates/sockudo-server/src/history.rs
- app/database backends already present in repo

Goals:
- either:
  - justify keeping Postgres as the only durable production backend for now
  - or add one additional durable backend with real conformance coverage

If adding a backend:
- do it through the existing `HistoryStore` abstraction
- preserve continuity-token semantics
- preserve append-only write discipline
- preserve history pagination semantics
- preserve degraded-state behavior

Deliver:
1. Backend decision record.
2. Code if a backend is added.
3. Cross-backend conformance tests for:
  - serial monotonicity
  - stream_id continuity
  - retention behavior
  - cursor pagination
  - degraded-state semantics
```

## Prompt 6: Production Dashboards, Alerts, And SLOs

```md
Turn the new metrics into an actual production operating model.

First inspect:
- crates/sockudo-core/src/metrics.rs
- crates/sockudo-metrics/src/prometheus.rs
- docs/content/2.server/23.multinode-history-ops.md
- grafana/ and monitoring/ if relevant

Goals:
- define concrete SLOs for:
  - history write success
  - recovery success/failure
  - queue depth
  - degraded channels
- add dashboards/alert rules/examples if the repo has an established monitoring surface
- ensure operators know which signals are page-worthy vs just warning-level

Deliver:
1. Metrics review and any missing additions.
2. Alert guidance or concrete alert configs.
3. Dashboard or dashboard guidance if the repo supports it.
4. Updated operator docs.
```

## Prompt 7: Cross-SDK Release Conformance

```md
Do a final release-conformance pass across all official client and server SDKs.

Goals:
- ensure the shipped server behavior, docs, and every official SDK agree exactly
- ensure example code compiles or is at least syntax-verified where build systems are available
- ensure no SDK still emits or expects stale payloads like legacy-only recovery shapes unless that is explicitly preserved for compatibility

Required checks:
- every client SDK:
  - recovery token tracking
  - rewind support or explicit non-support
  - resume success/failure event handling
  - docs/examples
  - build/test verification
- every server SDK:
  - history API support
  - query parameter mapping
  - pagination examples
  - build/test verification

Deliver:
1. A final client SDK conformance matrix.
2. A final server SDK conformance matrix.
3. Fixes for any discovered mismatches.
4. Honest list of any SDKs that still cannot be considered production-ready.
```

## Prompt 8: Final Release Candidate Audit

```md
Act as the final release owner for the history/recovery/rewind stack after phase 3 hardening.

Review with a bias for:
- silent data loss
- false-positive recovery
- distributed divergence
- degraded-state inconsistency
- replayed-wire-shape drift
- duplicate delivery under rewind/recovery
- unbounded memory/queue growth
- operator confusion
- rollback complexity
- SDK coverage gaps

Then:
- add or fix any missing tests or docs
- rerun the full relevant suite
- produce a release-candidate report

The report must include:
1. Changed files.
2. Simplifications made.
3. Remaining risks.
4. Rollback strategy.
5. Deployment prerequisites.
6. Metrics and alerts that must be live before rollout.
7. What “done” means for this feature after phase 3.
8. SDK coverage status for every official client and server SDK.
```

## Expected Phase 3 Exit Criteria

Do not call Phase 3 complete unless all of these are true:

- degraded-state coordination is robust across clustered nodes
- clustered transport-backed tests exist for at least one representative transport
- operator controls exist for stream inspection/reset/repair
- fault-injection coverage exists for the major failure classes
- production metrics and alert guidance are explicit
- all official client and server SDKs are either fully updated and verified or explicitly excluded with a documented product decision
- the release-candidate audit says the feature can be rolled out broadly with known residual risk only
