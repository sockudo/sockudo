# Ably-Grade Presence History Prompt Pack For Sockudo

Researched against Sockudo's current presence and history architecture in this repository, plus the Ably behavior described in the user-provided brief.

Primary Sockudo context:

- `crates/sockudo-adapter/src/presence.rs`
- `crates/sockudo-adapter/src/horizontal_adapter.rs`
- `crates/sockudo-adapter/src/horizontal_adapter_base.rs`
- `crates/sockudo-protocol/src/messages.rs`
- `docs/content/2.server/3.http-api.md`
- `docs/content/2.server/4.scaling.md`
- `docs/content/2.server/12.webhooks.md`
- `docs/content/2.server/15.connection-recovery.md`
- `docs/content/2.server/21.channel-history.md`
- `docs/content/2.server/23.multinode-history-ops.md`
- `FEATURE_COMPARISON.md`

Use these prompts in order. Each prompt assumes the previous one is complete, tested, and committed.

The goal is not a cosmetic `presence.history()` API. The goal is a production-grade, horizontally safe presence-event history system that:

- preserves current live presence behavior
- records presence membership transitions durably
- exposes bounded historical retrieval for a single channel
- remains operationally safe under multinode fanout, reconnects, and node death cleanup
- avoids turning presence history into a fake general-purpose database

## Product Boundary

Presence history is not a long-term database and should not be designed as one.

Phase-1 product intent:

- support historical retrieval of presence events for a single channel
- support pagination and bounded time windows
- support server-side retrieval first, then selected SDK surfaces
- preserve current live presence semantics and current member listing APIs

Phase-1 non-goals unless explicitly justified:

- multi-channel presence history queries
- search by arbitrary content
- full-text search
- cross-channel aggregation
- thread/group analytics
- replacing a durable application database

## Non-Negotiable Engineering Constraints

- Preserve current presence channel behavior for joins, leaves, and member listing.
- Do not regress current first-join / last-leave correctness semantics.
- Treat presence history as a separate durable subsystem from current-state presence membership.
- Do not make history writes a new correctness bottleneck for the live join/leave path.
- No duplicate historical rows for a single authoritative presence transition.
- Design for horizontal scale first: multiple nodes, duplicate cleanup signals, reconnects, and node-death orphan cleanup.
- Prefer append-only event storage, bounded retention, opaque cursors, and explicit metrics.
- If historical continuity cannot be proven after a persistence incident, fail closed instead of silently returning misleading gaps as complete data.

## Data Model Guidance

The implementation should explicitly separate:

- current presence state
- live presence fanout events
- durable historical presence events

Recommended event vocabulary:

- `member_added`
- `member_removed`
- optional future internal kinds such as `member_timeout_removed`, `member_orphan_cleanup_removed`, or `snapshot_boundary`, but only if they materially improve correctness or operator visibility

Recommended durable event shape:

- `app_id`
- `channel`
- `presence_stream_id` or equivalent continuity token
- monotonic per-channel presence-event serial
- event kind
- `user_id`
- optional `connection_id` / socket identifier if needed for forensics, not as the primary user-facing query surface
- published timestamp
- durable cause / source metadata when removal is synthetic or cleanup-driven
- canonical serialized payload returned by APIs

The system must answer this cleanly:

- "What presence events happened on this channel during this bounded period?"

It does not need to answer:

- "Show me all channels this user touched across the whole app."

## Prompt 1: Architecture And ADR

```md
You are implementing production-grade presence history for Sockudo.

First inspect:
- crates/sockudo-adapter/src/presence.rs
- crates/sockudo-adapter/src/horizontal_adapter.rs
- crates/sockudo-adapter/src/horizontal_adapter_base.rs
- crates/sockudo-protocol/src/messages.rs
- docs/content/2.server/3.http-api.md
- docs/content/2.server/4.scaling.md
- docs/content/2.server/12.webhooks.md
- docs/content/2.server/21.channel-history.md
- docs/content/2.server/23.multinode-history-ops.md
- FEATURE_COMPARISON.md

Then produce an ADR and implementation plan before code changes.

Requirements:
- Sockudo currently supports live presence membership and webhooks, but not durable presence history retrieval.
- Design a dedicated presence history subsystem. Do not blur it with message history or current member state.
- Match the useful Ably-style semantics without blindly cloning Ably internals:
  - per-channel historical retrieval
  - pagination
  - bounded time-range queries
  - clear distinction between current members and historical events
- Explain whether presence history should reuse the durable history substrate, share storage primitives with it, or remain a distinct store with a common abstraction layer.
- Define a continuity model strong enough to detect resets, retention truncation, and persistence incidents.
- Design for multinode correctness, orphan cleanup, duplicate suppression, and operational repair.
- Keep current V1/Pusher-compatible live presence behavior intact.

Deliver:
1. A concise ADR.
2. Product boundary and non-goals.
3. Data model changes.
4. API and SDK surface proposal.
5. Failure model.
6. Metrics and observability plan.
7. A phased implementation plan with test strategy.

Be explicit about what is out of scope for phase 1.
```

## Prompt 2: Durable Presence Event Storage

```md
Implement phase 1 of Sockudo presence history storage.

Start from the approved ADR. Before editing, re-read:
- crates/sockudo-adapter/src/presence.rs
- crates/sockudo-adapter/src/horizontal_adapter.rs
- crates/sockudo-protocol/src/messages.rs
- current history storage abstractions and config

Build a durable presence-history subsystem with these properties:
- append-only write path
- per app/channel partitioning
- monotonic per-channel presence-event serial
- continuity token / stream identity
- retention by age and optionally count/bytes if justified
- efficient cursor-based reads in newest-first and oldest-first order
- minimal extra allocation on the live presence transition path

Implementation requirements:
- Add a storage abstraction instead of hardcoding one backend into the presence handlers.
- Reuse the durable history substrate only if that improves correctness and operational simplicity without conflating message history and presence history contracts.
- Store enough metadata to distinguish:
  - normal member join
  - normal member leave
  - node-death orphan cleanup removal
  - timeout/forced disconnect removal if such causes exist or are added
- Persist only authoritative presence transitions. Do not write duplicate rows for redundant socket churn that does not change effective channel membership.
- Add config for:
  - enable/disable presence history
  - retention window
  - max page size
  - optional backend tuning knobs
- Add metrics:
  - presence history writes
  - write latency
  - write failures
  - queue depth if async
  - retained events
  - eviction counts

Verification:
- unit tests for ordering, deduplication, and retention
- integration tests for join/leave then history read
- no regression in existing live presence behavior tests

At the end, summarize changed files, invariants, and open risks.
```

## Prompt 3: Public HTTP API For Presence History

```md
Implement the server-side presence history retrieval surface on top of the new durable presence-history layer.

Re-read:
- docs/content/2.server/3.http-api.md
- docs/content/2.server/21.channel-history.md
- the approved ADR

Goals:
- expose presence history through a stable HTTP API first
- support:
  - limit
  - direction: newest-first and oldest-first
  - cursor pagination
  - bounded queries by timestamp, and by serial too if the storage layer supports it cleanly
  - channel-scoped history only
- do not design an unsafe offset API; prefer opaque cursors
- return continuity metadata with each page:
  - stream/token
  - oldest/newest retained event serial
  - oldest/newest retained timestamp
  - whether the result is complete or truncated by retention

Required behavior:
- default page size must be conservative and bounded
- max page size must be enforced server-side
- only valid for presence channels
- invalid or expired cursors must fail with explicit errors
- document clearly that presence history is event history, not current membership state
- preserve the existing current-members endpoint instead of overloading it

Suggested endpoint shape:
- `GET /apps/{appId}/channels/{channelName}/presence/history`

If you choose a different route, justify it in code comments and docs.

Verification:
- integration tests for pagination across multiple pages
- newest-first and oldest-first correctness
- correct filtering to presence channels only
- cursor invalidation behavior

Return:
- code
- tests
- docs
- explicit request/response examples
```

## Prompt 4: SDK Surface And Backend Proxy Guidance

```md
Add a production-appropriate client and server-consumption surface for presence history.

Goals:
- keep the trusted-server-first model
- expose a clean server-side API / SDK surface where appropriate
- only add direct client SDK methods where the auth model is clear and the contract is stable

Requirements:
- evaluate whether presence history should be backend-only initially, mirroring the current durable message history guidance
- if adding SDK support, keep the scope disciplined:
  - one primary SDK first if that is the repo's safer path
  - typed page model
  - clear limit/direction/start/end semantics
- do not overpromise direct browser/mobile usage if the server auth model still expects a backend proxy
- document the difference between:
  - current members
  - historical presence events
  - live presence event subscriptions

Verification:
- examples compile or are syntactically correct
- naming, event kinds, and parameter names match the server contract
- docs state backend proxy expectations explicitly
```

## Prompt 5: Multinode Correctness And Duplicate Suppression

```md
Harden presence history for multinode Sockudo deployments.

Inspect current horizontal presence synchronization and cleanup behavior before editing anything.

Goals:
- exactly one durable historical row per authoritative presence transition
- correct behavior under:
  - concurrent joins on different nodes
  - reconnect churn
  - disconnect + reconnect races
  - node death orphan cleanup
  - duplicate inter-node notifications
- no fake completeness after persistence incidents

Implementation expectations:
- define the authoritative writer for a presence transition
- prevent duplicate durable writes across nodes
- make duplicate suppression explicit in code and tests
- document what happens when:
  - a node dies before its durable write
  - orphan cleanup emits removals
  - storage is degraded but live presence still functions
  - retention has truncated older events

Verification:
- multinode integration tests if the harness supports them
- otherwise add the narrowest credible simulation and describe the remaining gap honestly
- tests for duplicate join/remove suppression across nodes
- tests for orphan-cleanup historical recording
```

## Prompt 6: Repair, Reset, And Operational Semantics

```md
Add production-grade operational semantics for presence history degradation and repair.

Requirements:
- presence history must not silently look healthy after durable write failure
- define explicit channel stream state for:
  - healthy
  - degraded
  - reset_required if justified
- expose enough operator visibility to distinguish:
  - live presence still works, but history is degraded
  - retained floor advanced due to retention
  - continuity cannot be proven after a store incident
- if admin/debug endpoints are justified, add the minimum safe set rather than a broad control surface

I want explicit answers in code and docs for:
- what happens during node restart
- what happens during durable-store outage
- what happens during replay of delayed cleanup messages
- what happens during retention compaction
- how operators detect degradation before users do

Verification:
- tests for degraded-state fail-closed behavior
- metrics and/or stats surface updates
- operator documentation
```

## Prompt 7: Documentation, Examples, And Compatibility Notes

```md
Finish the presence history feature professionally.

Minimum documentation scope:
- server docs
- config docs
- HTTP API reference
- compatibility notes
- examples for reading presence history
- migration guidance for teams currently relying only on live webhooks or custom persistence

Important:
- explicitly describe that presence history is historical event retrieval, not a database
- explicitly describe retention and failure modes
- explicitly separate:
  - live presence
  - current member queries
  - historical presence events
- do not claim stronger guarantees than the code actually enforces
- preserve V1/Pusher-compatible live presence statements where still true

Verification:
- docs build if the repo has a docs build
- examples are syntactically correct
- cross-check names, endpoint paths, config keys, and event kinds against the implementation
```

## Prompt 8: Final QA, Load Review, And Production Readiness

```md
Act as the final engineer responsible for shipping Sockudo presence history.

Do a full review of the implemented changes with a bias for:
- duplicate historical rows
- missing rows under race conditions
- retention edge cases
- node-death cleanup correctness
- lock contention on hot presence channels
- unbounded memory growth
- API ambiguity
- multinode divergence
- confusing overlap between current-members and history APIs

Then:
- add or fix missing tests
- run relevant test suites
- add focused benchmarks if the repo has an established benchmark surface
- write a short production-readiness report

The final report must include:
- changed files
- simplifications made
- remaining risks
- what should be deferred to phase 2
- what metrics must be watched in production
```

## Suggested Phase-2 Follow-Ups

- Presence history filters by event kind when the baseline API is stable.
- Presence history filters by `user_id` only if the storage/index tradeoff is explicitly accepted.
- Presence snapshot markers if products need efficient "state as of time T" reconstruction.
- Selected direct SDK `presence.history()` support after the server-side contract has stabilized.
- Cross-subsystem correlation between message history and presence history only if a concrete product need emerges.

## Phase 2: Ably-Style SDK And API Parity

The prompts above get Sockudo to a production-grade baseline. The prompts below are for the next lane: making the feature feel and behave much more like Ably in practice.

Phase-2 parity goals:

- direct `presence.history()` style SDK surfaces
- consistent parameter names and paging semantics
- server and realtime interfaces that expose the same conceptual model
- a clean distinction between current members and historical presence events
- enough compatibility that application code written with Ably-style expectations does not need architectural rewrites

Phase-2 still does not turn presence history into a database.

## Prompt 9: Direct `presence.history()` SDK Parity

```md
Implement direct Ably-style `presence.history()` support for selected Sockudo SDKs on top of the server contract.

Start by inspecting:
- current presence subscription/member APIs in the official SDKs
- current server-side history endpoints
- docs/content/5.reference/2.http-endpoints.md
- docs/content/3.client/*

Goal:
- expose a first-class `presence.history()` API that feels natural to users familiar with Ably

Required behavior:
- method lives on the presence surface, not as a generic raw HTTP helper
- supports:
  - `start`
  - `end`
  - `direction`
  - `limit`
- returns a paged result model with:
  - `items`
  - `hasNext()`
  - `next()`
- direction semantics must be explicit and consistent with the server API
- server-enforced limit cap must still apply
- if the SDK cannot safely call the history API directly due to auth model constraints, implement the narrowest honest abstraction rather than pretending parity exists

Scope discipline:
- ship one primary SDK first if that is the safest path
- then add the others if the abstraction is clean and low-risk
- do not add copy-pasted inconsistent implementations

Verification:
- SDK tests for parameter encoding
- paging behavior tests
- examples for retrieving presence history
- docs explaining any backend-proxy requirement
```

## Prompt 10: REST And Realtime Contract Alignment

```md
Align Sockudo presence history semantics across REST/server-side and realtime-facing SDK surfaces so the product behaves like one coherent feature.

Requirements:
- presence history parameters should map cleanly between transport surfaces
- response ordering and paging semantics must match
- errors for invalid time bounds, direction, or page requests must be normalized
- current-member retrieval must remain a separate operation from presence history retrieval
- live presence subscriptions must continue to behave independently from historical retrieval

I want explicit treatment of these cases:
- fetch history before subscribing
- fetch history after subscribing
- fetch the latest page only
- fetch multiple pages backwards
- fetch a bounded time slice

If there is any behavior that cannot match Ably cleanly under Sockudo's auth or transport model, document the mismatch explicitly rather than hiding it.

Verification:
- cross-surface tests or contract snapshots
- docs examples showing equivalent REST and SDK usage
- error-shape consistency checks
```

## Prompt 11: Presence Snapshot Reconstruction

```md
Implement the minimum additional capability needed for applications to reconstruct presence state from historical events without abusing the current-members API.

This is not asking for a new database feature. It is asking for a production-usable way to answer practical questions such as:
- "What happened in this presence channel during the last 10 minutes?"
- "What was the effective membership around time T?"

Requirements:
- evaluate whether snapshot markers, periodic checkpoints, or pure event replay is the right phase-2 mechanism
- avoid making every historical read replay the entire retained stream if that becomes operationally expensive
- keep the user-facing API honest:
  - current members endpoint returns current state
  - presence history returns events
  - any reconstructed snapshot endpoint or helper must be clearly named and bounded

If you add snapshot support:
- keep it channel-scoped
- keep retention and continuity semantics explicit
- document reconstruction cost and limits

Verification:
- tests for reconstructing membership around a bounded timestamp
- tests for joins/leaves/orphan cleanup interactions
- docs that explain when to use current-members vs history vs snapshot reconstruction
```

## Phase 3: Ably-Like Completion And Conformance

Phase 3 is the closing lane: remove product rough edges, verify compatibility expectations, and ship something that behaves credibly like Ably for presence history, not just something that exposes similar nouns.

Phase-3 goals:

- parity-focused behavior review
- compatibility/conformance coverage
- load, failure, and operator readiness
- documentation polished enough for external users

## Prompt 12: Ably-Parity Gap Audit And Closure

```md
Act as the engineer responsible for closing the remaining gap between Sockudo presence history and the Ably-style product expectations documented so far.

Audit the implemented feature against these expectations:
- presence history exists as a first-class concept
- pagination works predictably
- start/end/direction/limit semantics are clear
- single-channel history only
- current members and historical events are clearly separate
- SDK ergonomics are credible for users expecting `presence.history()`
- failure and retention semantics are explicit

Then:
- list every remaining parity gap
- classify each as:
  - must-fix before calling the feature complete
  - acceptable documented difference
  - defer to later phase
- implement the must-fix items
- tighten docs and tests accordingly

Do not hand-wave "close enough". Make the call explicit.

Verification:
- a written parity matrix
- automated tests added for every must-fix gap you close
- docs updated anywhere user expectations would otherwise be misleading
```

## Prompt 13: Chaos, Load, And Failover Verification

```md
Run the final production-hardening lane for presence history.

Focus on:
- hot presence channels
- rapid join/leave churn
- multinode duplicate-risk scenarios
- node death orphan cleanup
- durable-store slowness or outage
- retention compaction boundaries
- SDK paging under large retained windows

Requirements:
- use the repo's existing integration and multinode harnesses where possible
- add focused load or benchmark coverage if a benchmark surface exists
- explicitly test fail-closed behavior when continuity cannot be trusted
- verify operator signals are sufficient to detect degradation

The final report must answer:
- does the feature still behave correctly under pressure?
- where does it degrade?
- what metrics must alert first?
- what documented limits remain?
```

## Prompt 14: Ship Review And Launch Artifacts

```md
Finish the feature as if it is about to be released publicly.

Deliver:
- release notes
- migration notes
- operator runbook additions
- API reference examples
- SDK examples
- a concise "how this differs from using a database" warning

I want the final state to be usable by:
- backend engineers integrating server-side history reads
- SDK users expecting `presence.history()`
- operators debugging degraded history on clustered deployments

Verification:
- docs build cleanly
- examples are accurate
- naming is consistent across code, docs, and SDKs
- every public claim is backed by code or tests
```
