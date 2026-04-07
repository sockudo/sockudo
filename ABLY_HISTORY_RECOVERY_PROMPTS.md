# Ably-Grade Message History, Recovery, And Channel Rewind Prompt Pack For Sockudo

Researched against the latest Ably docs accessed on 2026-04-04, plus Sockudo's current architecture:

- `FEATURE_COMPARISON.md` section 3.2
- `FEATURE_COMPARISON.md` section 2.1 (`Channel Rewind`)
- `crates/sockudo-adapter/src/replay_buffer.rs`
- `crates/sockudo-adapter/src/handler/recovery.rs`
- `crates/sockudo-adapter/src/handler/connection_management.rs`
- `crates/sockudo-protocol/src/messages.rs`
- `docs/content/2.server/15.connection-recovery.md`
- `docs/content/2.server/17.extras-envelope.md`

Ably research anchors:

- Pub/Sub history: https://ably.com/docs/storage-history/history
- Connections overview: https://ably.com/docs/connect
- Chat history retention and pagination: https://ably.com/docs/chat/rooms/history

Use these prompts in order. Each prompt assumes the previous one is complete, tested, and committed. The goal is not superficial feature parity. The goal is a production-grade, horizontally scalable, high-throughput history, recovery, and channel rewind subsystem that fits Sockudo's existing V2 protocol, preserves V1 compatibility, and does not regress hot-path publish latency.

Important scope split from `FEATURE_COMPARISON.md`:

- `3.2 Message History & Recovery` covers durable history, pagination, stream continuity, and resume/recovery.
- `2.1 Channel Rewind` is a separate channel-level attach/subscribe behavior built on top of the history substrate.
- In other words: rewind depends on history, but it should be specified and implemented as a distinct channel feature, not collapsed into generic history APIs.

## Non-Negotiable Engineering Constraints

- Preserve current V1 behavior. All new history and recovery semantics are V2-first unless there is a strong compatibility reason otherwise.
- Do not replace the current replay path with a slow persistence-first path. Keep a hot in-memory recovery buffer for very recent replay and add a durable history layer beside it.
- No global locks on the publish path. Shard by `app_id + channel`.
- No full-message JSON parse and reserialize on the critical path unless unavoidable. Sockudo already protects byte ordering for delta compatibility; preserve that discipline.
- Design for horizontal scale first: multiple nodes, out-of-order cross-node delivery risk, partial persistence outages, and channel hot spots.
- Do not add a dependency or storage engine casually. If a new dependency is required, justify it with measured tradeoffs.
- Prefer append-only writes, cursor-based reads, bounded memory, backpressure, and explicit metrics over implicit behavior.
- Recovery correctness is more important than pretending recovery succeeded. If continuity cannot be proven, fail fast with a structured recovery failure.

## Prompt 1: Architecture And ADR

```md
You are implementing Ably-grade message history, recovery, and channel rewind for Sockudo.

First inspect:
- FEATURE_COMPARISON.md section 2.1 (`Channel Rewind`)
- FEATURE_COMPARISON.md section 3.2
- crates/sockudo-adapter/src/replay_buffer.rs
- crates/sockudo-adapter/src/handler/recovery.rs
- crates/sockudo-adapter/src/handler/connection_management.rs
- crates/sockudo-protocol/src/messages.rs
- docs/content/2.server/15.connection-recovery.md

Then produce an ADR and implementation plan before code changes.

Requirements:
- Sockudo currently has replay-buffer-based recovery only. Design a general-purpose durable history system.
- Match the important latest Ably semantics without blindly cloning Ably internals:
  - paginated history
  - forward and reverse iteration
  - history from explicit bounds or cursor
  - channel rewind as an attach-time or subscribe-time option
  - continuous handoff between recent history and live delivery
  - automatic recovery for brief disconnects
  - hard failure when continuity cannot be guaranteed
- Introduce a stream continuity model stronger than today's `serial` only approach. I want a channel stream token that can detect resets, truncation, or storage loss. If you use `epoch`, justify how it changes and who owns it.
- Separate hot recovery and cold history:
  - hot: in-memory or near-memory replay for ultra-fast reconnect
  - cold: durable persisted history for long-range fetch
- Design for multinode ordering and recovery, not just single-process recovery.
- Keep V1 clients unaffected.
- Bias for maximum throughput and operational simplicity.

Deliver:
1. A concise ADR.
2. Data model changes.
3. API and protocol changes.
4. Failure model.
5. Metrics and observability plan.
6. A phased implementation plan with test strategy.

Be explicit about what should remain out of scope for phase 1.
```

## Prompt 2: Durable History Storage Layer

```md
Implement phase 1 of the new message history system in Sockudo.

Start from the approved ADR. Before editing, re-read:
- crates/sockudo-adapter/src/replay_buffer.rs
- crates/sockudo-adapter/src/handler/connection_management.rs
- crates/sockudo-protocol/src/messages.rs
- config/config.toml

Build a durable history subsystem with these properties:
- append-only write path
- per app/channel partitioning
- monotonic channel serial
- stream epoch or equivalent continuity token
- retention policy by age and optionally by count/bytes
- efficient cursor-based reads in newest-first and oldest-first order
- minimal extra allocation on publish
- clear boundary between hot replay buffer and durable history store

Implementation requirements:
- Add a storage abstraction instead of hardcoding a single backend into the handler.
- Default phase-1 backend may be the existing cache/database layer if appropriate, or a new internal backend if justified.
- Store enough metadata to support future updates/deletes/appends without redesigning the schema.
- Preserve raw payload fidelity for V2 messages.
- Keep ephemeral messages out of durable history unless there is a deliberate documented reason not to.
- Add config for:
  - enable/disable durable history
  - retention window
  - max page size
  - optional backend-specific tuning knobs
- Add metrics:
  - history writes
  - write latency
  - write failures
  - retained bytes/messages
  - eviction counts

Verification:
- unit tests for ordering, retention, and cursor behavior
- integration tests for publish then history read
- no regression in existing recovery tests

At the end, summarize changed files, invariants, and open risks.
```

## Prompt 3: History Read API And Pagination

```md
Implement the public history retrieval surface for Sockudo on top of the new durable history layer.

Re-read:
- FEATURE_COMPARISON.md section 2.1 (`Channel Rewind`) for boundary clarity
- FEATURE_COMPARISON.md section 3.2
- docs/content/5.reference/2.http-endpoints.md
- server SDK event publish surfaces
- client SDK recovery docs

Goals:
- expose history through a stable HTTP API first
- support:
  - limit
  - direction: newest-first and oldest-first
  - cursor pagination
  - bounded queries by serial, timestamp, or both if the storage layer supports both cleanly
  - channel-scoped history
- do not design an offset API that becomes unsafe under retention and concurrent writes; prefer opaque cursors
- return continuity metadata with each page:
  - channel serial range
  - stream epoch/token
  - whether the result is complete or truncated by retention

Required behavior:
- default page size should be conservative and bounded
- max page size must be enforced server-side
- history queries must not block the live publish path
- invalid or expired cursors must fail with explicit errors
- V1/V2 compatibility rules must be documented

Also:
- add server docs
- add at least one server SDK surface if there is already a clear abstraction for it
- do not add half-designed APIs across every SDK in one pass if that will reduce quality

Verification:
- integration tests for pagination across multiple pages
- newest-first and oldest-first correctness
- cursor invalidation behavior
- performance sanity test for large channel history reads

Return:
- code
- tests
- docs
- explicit examples of request/response shapes
```

## Prompt 4: Continuous History, Rewind, And Attach Semantics

```md
Implement channel rewind as a distinct channel-level feature on top of Sockudo's new durable history layer.

Research basis to honor:
- FEATURE_COMPARISON.md section 2.1 (`Channel Rewind`)
- Ably history vs rewind behavior
- Ably continuous history using rewind or untilAttach

Sockudo-specific goals:
- support a V2-native attach/subscribe option to request rewind on first attach
- support count-based rewind and time-based rewind if the storage layer can answer both efficiently
- keep rewind separate from the generic history API surface
- ensure no duplicate-gap window between historical replay and live subscription
- preserve current direct live delivery performance

Implementation expectations:
- define the protocol shape for channel rewind on attach/subscribe
- make the API boundary explicit:
  - history = explicit fetch API
  - rewind = channel attach/subscribe option
- ensure ordering guarantees are explicit
- deduplicate against replay/recovery using existing `message_id` where appropriate
- if a perfect no-gap attach cannot be guaranteed under the current architecture, introduce the necessary server-side attach barrier or sequencing mechanism instead of hand-waving
- do not break existing pusher-compatible subscribe flows

Verification:
- tests proving no gap between history handoff and live delivery
- tests for duplicate suppression
- tests for rewind by count
- tests for rewind by duration if implemented

Document the exact semantics, especially what is guaranteed and what is best-effort.
```

## Prompt 5: Recovery V3 With Stream Continuity Tokens

```md
Upgrade Sockudo connection recovery from replay-buffer-only serial replay to a continuity-aware recovery protocol.

Re-read:
- crates/sockudo-adapter/src/handler/recovery.rs
- docs/content/2.server/15.connection-recovery.md
- the approved ADR for history/recovery

Requirements:
- current recovery only trusts `channel -> last_serial`
- introduce a stronger resume payload such as:
  - last serial
  - stream epoch/token
  - optionally last message_id for dedup assistance
- for brief disconnects, prefer the hot replay buffer
- if hot replay cannot satisfy the request, fall back to durable history only when continuity can still be proven
- if continuity cannot be proven, fail recovery explicitly and force resubscribe
- include per-channel results and structured reasons
- handle node restarts and durable-store partial loss correctly

Performance requirements:
- reconnect fast path must remain cheap
- avoid full history scans for recent resumptions
- keep per-channel recovery independent so one failed channel does not poison all others

Deliver:
- protocol changes
- server implementation
- SDK-facing event/documentation updates where needed
- tests for:
  - hot-buffer recovery success
  - cold-store recovery success
  - epoch mismatch
  - retention gap
  - channel partially recoverable / partially failed reconnects
```

## Prompt 6: Multinode Correctness, Ops, And Observability

```md
Harden the history and recovery system for multinode Sockudo deployments.

Inspect current horizontal adapter and transport code before changing anything.

Goals:
- make history writes and recovery semantics safe under horizontal fanout
- define the source of truth for channel serial and stream epoch ownership
- prevent duplicate durable writes across nodes
- prevent recovery divergence when publishers and subscribers land on different nodes
- surface clear operational metrics and alerts

Required outputs:
- code changes for multinode correctness
- metrics and tracing additions
- admin/debug surfaces if justified
- documentation for operators

I want explicit answers in code and docs for:
- what happens during node restart
- what happens during durable-store outage
- what happens during replay buffer loss
- what happens during retention compaction
- how operators detect recovery degradation before customers do

Verification:
- multinode integration tests if the existing harness supports them
- otherwise build the narrowest credible automated simulation and state the remaining gap honestly
```

## Prompt 7: SDK, Docs, And Migration Layer

```md
Finish the history/recovery feature professionally.

Update the public contract across docs and selected SDKs without overextending the change.

Minimum scope:
- server docs
- config docs
- HTTP API reference
- at least one client SDK example for V2 recovery and history usage
- migration notes from replay-buffer-only recovery to durable history + continuity tokens

Important:
- explicitly describe V1 vs V2 behavior
- explicitly describe retention and failure modes
- include examples for history pagination and resume failure handling
- do not claim stronger guarantees than the code actually enforces

Verification:
- docs compile if the docs build exists
- examples are syntactically correct
- cross-check names, event strings, and config keys against the implementation
```

## Prompt 8: Final QA, Benchmarks, And Production Readiness

```md
Act as the final engineer responsible for shipping Sockudo's new history and recovery stack.

Do a full review of the implemented changes with a bias for:
- data loss risk
- recovery false positives
- retention edge cases
- hot-channel performance
- unbounded memory growth
- lock contention
- duplicate delivery
- API ambiguity
- multinode inconsistencies

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
