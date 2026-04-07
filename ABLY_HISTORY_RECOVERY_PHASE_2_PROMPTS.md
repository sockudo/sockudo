# Ably-Grade Message History, Recovery, And Rewind Prompt Pack
# Phase 2: Feature Completion And Contract Stabilization For Sockudo

This file continues from the original prompt pack in [`ABLY_HISTORY_RECOVERY_PROMPTS.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/ABLY_HISTORY_RECOVERY_PROMPTS.md).

Assumed completed before starting:

- ADR written and accepted
- phase-1 durable history substrate implemented
- HTTP history API implemented
- rewind implemented
- continuity-aware recovery implemented
- initial multinode hardening and operator docs implemented
- public contract/docs pass completed

This phase is for turning the current implementation into a feature-complete GA candidate rather than a “works in principle” stack.

Use these prompts in order. Each prompt assumes the previous one is complete, tested, and committed.

## Phase 2 Goals

- close the known correctness and usability gaps left after the initial implementation
- stabilize the public contract across protocol, docs, and selected SDKs
- add end-to-end verification where helper/unit tests currently stand in for real delivery paths
- improve hot-channel and cold-recovery performance enough for real production load
- add per-app and per-namespace policy where the current implementation is still global-only

## Carry-Forward Constraints

These remain non-negotiable:

- Preserve current V1 behavior. All new history, rewind, and recovery semantics are V2-first unless there is a strong compatibility reason otherwise.
- Do not replace the hot replay path with a persistence-first slow path.
- No global locks on the publish path. Shard by `app_id + channel`.
- Preserve raw payload fidelity. The bytes recovered or rewinded for V2 must match the canonical V2 wire shape.
- Prefer append-only writes, bounded memory, explicit backpressure, explicit degradation, and explicit metrics.
- If continuity cannot be proven, fail recovery explicitly.
- Do not claim stronger guarantees in docs or SDKs than the code actually enforces.

## Known Gaps To Close In Phase 2

The current implementation is close, but these gaps still exist and Phase 2 should treat them as concrete closure targets:

- Rewind no-gap semantics are validated mostly through local barrier/helper logic, not a full end-to-end websocket harness.
- Cold recovery is correctness-first but not yet optimized for large gaps or hot channels.
- Durable history policy is still primarily global; per-app and per-namespace policy is not yet a stable, complete public surface.
- Client SDK docs are updated, but first-class client SDK ergonomics for continuity-aware recovery and rewind are not complete.
- The Ruby server SDK was updated, but broader SDK verification and selected client SDK runtime support still need real completion.
- The public contract should be frozen only after the SDK/runtime/docs surfaces all agree exactly.

## Prompt 1: Per-App And Per-Namespace History/Rewind Policy

```md
Complete the history/recovery/rewind policy model so it is not effectively global-only anymore.

First inspect:
- crates/sockudo-core/src/app.rs
- crates/sockudo-core/src/options.rs
- crates/sockudo-app/*_app_manager.rs
- crates/sockudo-adapter/src/handler/validation.rs
- crates/sockudo-adapter/src/handler/subscription_management.rs
- docs/content/2.server/1.configuration.md
- docs/content/5.reference/3.config-reference.md

Goals:
- add a stable public config/policy surface for:
  - per-app durable history enable/disable
  - per-app history retention limits
  - per-app rewind enable/disable
  - per-namespace history retention overrides
  - per-namespace rewind allowance
- define clear precedence:
  - global config
  - per-app override
  - per-namespace override
- make validation explicit when a client requests rewind on a channel where policy forbids it

Requirements:
- do not break existing global defaults
- keep the runtime fast: policy resolution should be read-only and cheap
- update app-manager persistence so policy survives reload for the supported backends already in repo
- ensure history API, rewind, and recovery all consult the same resolved policy instead of drifting

Deliver:
1. Code changes for policy structs and resolution helpers.
2. Validation changes for forbidden rewind/history use.
3. Persistence changes for app managers that already store `AppPolicy`.
4. Docs updates for config and policy precedence.

Verification:
- unit tests for precedence resolution
- persistence round-trip tests for app managers already covered by the repo
- integration tests proving rewind is rejected when disabled by policy
- integration tests proving namespace override beats app/global where intended
```

## Prompt 2: Full Client SDK Support For Recovery And Rewind

```md
Upgrade all official client SDKs so continuity-aware recovery and rewind are real supported features, not just doc-level examples.

First inspect:
- client-sdks/sockudo-js
- client-sdks/sockudo-kotlin
- client-sdks/sockudo-swift
- client-sdks/sockudo-flutter
- docs/content/3.client/3.usage.md
- docs/content/3.client/4.official-client-features.md
- docs/content/2.server/15.connection-recovery.md
- docs/content/2.server/22.channel-rewind.md

Goals:
- expose continuity-aware resume positions:
  - `stream_id`
  - `serial`
  - `last_message_id`
- expose rewind subscribe options in the SDK surface
- expose typed handling for:
  - `resume_success`
  - `resume_failed`
  - `rewind_complete`
- keep V1 compatibility mode untouched

Requirements:
- do not invent unstable APIs; follow existing SDK style
- update every official client SDK in this repository, not just one or two
- examples must match the actual event names and payloads the server emits
- if one SDK cannot support the same ergonomic shape as the others, document the divergence explicitly and justify it

Minimum coverage:
- JavaScript / TypeScript
- Kotlin
- Swift
- Flutter

Each touched SDK must have:
- runtime support for continuity-aware recovery positions
- typed or documented handling for `resume_success`, `resume_failed`, and `rewind_complete`
- subscribe-time rewind support where the SDK already has subscription options
- updated README/examples
- tests or compile/build verification

Deliver:
1. SDK code changes.
2. SDK tests.
3. Updated examples and README/docs.
4. A client SDK coverage matrix showing support status for every recovery/history/rewind capability.

Verification:
- SDK unit/integration tests where available
- build/compile checks for every touched SDK
- cross-check with protocol docs and server event strings
```

## Prompt 3: Full Server SDK Support For History APIs

```md
Upgrade all official server SDKs so the new history API is available consistently where those SDKs expose channel/application query methods.

First inspect:
- server-sdks/sockudo-http-ruby
- server-sdks/sockudo-http-php
- server-sdks/sockudo-http-dotnet
- server-sdks/sockudo-http-swift
- any other official server SDK folders in this repository
- docs/content/2.server/3.http-api.md
- docs/content/5.reference/2.http-endpoints.md

Goals:
- expose channel history retrieval in every official server SDK that already exposes channel query/info APIs
- keep request parameter naming aligned with the underlying SDK conventions while preserving server semantics:
  - `limit`
  - `direction`
  - `cursor`
  - `start_serial`
  - `end_serial`
  - `start_time_ms`
  - `end_time_ms`
- add examples for paginated history usage

Requirements:
- do not stop after one server SDK; update all official server SDKs in the repository unless one is clearly abandoned or structurally incompatible
- if an SDK cannot be updated cleanly, document that explicitly in the final report
- avoid adding half-designed high-level abstractions where the SDK is intentionally thin; a clean low-level method is acceptable

Deliver:
1. SDK code changes across server SDKs.
2. README/example updates.
3. Tests or request-shape verification for each updated server SDK.
4. A server SDK coverage matrix.

Verification:
- run unit/spec/test suites where present
- at minimum, compile/build/static verification for each touched SDK
- cross-check generated request paths and query params against the server implementation
```

## Prompt 4: End-To-End Rewind And Recovery Harness

```md
Replace helper-only confidence with real end-to-end delivery evidence for rewind and recovery.

First inspect:
- crates/sockudo-adapter/src/handler/subscription_management.rs
- crates/sockudo-adapter/src/handler/recovery.rs
- crates/sockudo-core/src/websocket.rs
- existing adapter/server test harnesses

Goals:
- build a narrow but real websocket/runtime harness that proves:
  - rewind history handoff has no gap before live delivery resumes
  - duplicate suppression works across rewind + live buffer
  - hot recovery succeeds with real replayed deliveries
  - cold recovery succeeds with real replayed deliveries
  - partial per-channel recovery results are surfaced correctly

Requirements:
- do not settle for only helper-function tests if a realistic harness can be built with the repo’s current runtime surfaces
- if the repo truly cannot support a full end-to-end harness cleanly, build the narrowest credible simulation and state the exact remaining gap
- keep the tests deterministic and bounded in runtime

Deliver:
1. New end-to-end tests.
2. Any harness helpers required.
3. Brief note on what remains unverified after the new harness.

Verification:
- the new test suite itself
- existing relevant suites remain green
```

## Prompt 5: Hot-Channel And Large-Gap Performance Pass

```md
Optimize history/recovery/rewind for hot channels and larger retained histories without weakening correctness.

First inspect:
- crates/sockudo-adapter/src/handler/connection_management.rs
- crates/sockudo-adapter/src/handler/recovery.rs
- crates/sockudo-adapter/src/replay_buffer.rs
- crates/sockudo-server/src/history.rs
- crates/sockudo-core/benches/history_bench.rs

Goals:
- keep reconnect fast path cheap
- reduce avoidable cold-recovery paging overhead
- reduce avoidable rewind overhead for count-based replay
- avoid unnecessary clones or serialization work
- keep queue/backpressure and degradation semantics explicit

Requirements:
- no weakening of continuity checks
- no reintroduction of publish-path JSON parse/reserialize work
- no global locks
- benchmark before/after where the repo has an established bench surface

Focus areas:
- large-gap cold recovery scan shape
- history page fetch efficiency
- rewind handoff buffering cost
- replay buffer lookup cost

Deliver:
1. Code improvements.
2. Focused benchmarks and/or benchmark updates.
3. Before/after notes for the main hot/cold paths.

Verification:
- existing tests
- benchmark target compiles and runs
- no regressions in correctness tests
```

## Prompt 6: Public Contract Freeze And Ambiguity Audit

```md
Freeze the public contract for history, rewind, and continuity-aware recovery.

First inspect:
- docs/content/5.reference/1.protocol.md
- docs/content/2.server/3.http-api.md
- docs/content/2.server/15.connection-recovery.md
- docs/content/2.server/21.channel-history.md
- docs/content/2.server/22.channel-rewind.md
- docs/content/2.server/23.multinode-history-ops.md
- selected SDK docs you touched in phase 2

Goals:
- remove contract ambiguity
- ensure all request/response shapes match implementation
- ensure every guarantee is labeled either:
  - guaranteed
  - best-effort
  - intentionally unsupported
- ensure V1 vs V2 behavior is explicit everywhere

Requirements:
- do not leave stale legacy examples as the primary path when newer continuity-aware forms exist
- include resume failure examples with real structured payloads
- include history pagination examples with real continuity metadata
- include rewind completion examples
- document what still requires a backend proxy vs direct client support

Deliver:
1. Protocol/doc updates.
2. SDK README/example updates as needed.
3. A concise “contract freeze” summary listing the canonical event names and payloads.

Verification:
- docs build
- link checks if available
- cross-check event strings and config names against implementation
```

## Prompt 7: SDK Coverage Audit Before Phase 2 Signoff

```md
Before declaring Phase 2 complete, do a dedicated SDK coverage audit across all official client and server SDKs.

Goals:
- verify that every official SDK in the repository is in one of these states:
  - fully updated
  - intentionally unsupported for a clearly documented reason
  - blocked with an explicit follow-up item
- eliminate accidental gaps where one SDK was simply forgotten

Required output:
1. Full list of official client SDKs and their status.
2. Full list of official server SDKs and their status.
3. Missing features per SDK:
   - continuity-aware recovery
   - rewind
   - history API
   - docs/examples
   - tests/build verification
4. Exact blockers if any SDK is not fully updated.

Do not mark Phase 2 complete if SDK coverage is partial without that being explicitly called out and justified.
```

## Prompt 8: Phase 2 Ship Review

```md
Act as the release engineer for Phase 2.

Review the current history/recovery/rewind stack with a bias for:
- false-success recovery
- replayed-wire-shape mismatches
- rewind gap/duplicate edge cases
- retention floor confusion
- policy precedence drift
- SDK/runtime/doc mismatch
- hot-path regression risk

Then:
- add or fix any missing tests needed to close phase-2 feature gaps
- rerun the relevant suites
- write a short phase-2 readiness report

The report must include:
1. What became feature-complete in phase 2.
2. What remains phase-3 work.
3. Changed files.
4. Remaining risks.
5. Recommended rollout scope after phase 2.
```

## Expected Phase 2 Exit Criteria

Do not call Phase 2 complete unless all of these are true:

- per-app/per-namespace history/rewind policy is real and documented
- at least one client SDK and one server SDK have first-class usable surfaces where appropriate
- all official client and server SDKs have been audited and either updated or explicitly accounted for
- rewind/recovery are covered by a real end-to-end harness or the remaining gap is explicitly bounded and justified
- public contract/docs/SDKs agree exactly on payloads and names
- performance has been reviewed with at least focused benchmarks or measured sanity checks
