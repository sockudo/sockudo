# Ably-Grade Message Annotations, Updates, Deletes, And Appends Prompt Pack For Sockudo

Researched against the latest Ably docs accessed on 2026-04-04, plus Sockudo's current architecture:

- `FEATURE_COMPARISON.md` section 3.4
- `crates/sockudo-protocol/src/messages.rs`
- `crates/sockudo-adapter/src/handler/connection_management.rs`
- `crates/sockudo-adapter/src/handler/message_handlers.rs`
- `crates/sockudo-adapter/src/local_adapter.rs`
- `docs/content/2.server/17.extras-envelope.md`
- `docs/content/2.server/15.connection-recovery.md`

Ably research anchors:

- Message annotations: https://ably.com/docs/messages/annotations
- Updates, deletes and appends: https://ably.com/docs/messages/updates-deletes
- AI transport `message.append` delivery semantics: https://ably.com/docs/ai-transport/token-streaming/message-per-response

These prompts assume the history/recovery foundation exists or is being implemented in parallel, because durable message identity and persistence are prerequisites for doing this correctly.

## Non-Negotiable Engineering Constraints

- Do not bolt this on as ad hoc event-name conventions. Add first-class protocol and storage semantics.
- Message identity must be stable and durable. `serial` alone is not enough unless the underlying continuity model guarantees its durability and uniqueness within stream scope.
- Realtime subscribers may receive summarized or conflated operations, but history and point lookups must converge on a single authoritative latest state.
- Do not require clients to replay arbitrary historical deltas to understand the current message state.
- Preserve V1 compatibility by stripping or translating unsupported fields cleanly.
- Design for high-frequency append use cases and avoid per-append heavy read-modify-write costs when possible.
- Annotation summaries must be derivable, testable, and bounded in memory.
- Permissions and audit metadata must be part of the design, not an afterthought.

## Prompt 1: Architecture And Semantic Model

```md
You are implementing Ably-grade message annotations and message modifications for Sockudo.

First inspect:
- FEATURE_COMPARISON.md section 3.4
- crates/sockudo-protocol/src/messages.rs
- crates/sockudo-adapter/src/handler/connection_management.rs
- crates/sockudo-adapter/src/handler/message_handlers.rs
- crates/sockudo-adapter/src/local_adapter.rs
- current history/recovery design and storage model

Then produce an ADR before code changes.

The ADR must define:
- the message action model for Sockudo V2
  - `message.create`
  - `message.update`
  - `message.delete`
  - `message.append`
  - `message.summary`
  - `annotation.create`
  - `annotation.delete`
- which actions are stored durably
- which actions are delivered live
- what history returns
- how point lookup and version lookup work
- how append semantics differ from update semantics
- how annotation summary state is represented
- how permissions map to actions
- how V1 clients are isolated from new behavior

Match the latest Ably behavior where it is product-correct, especially:
- updates and deletes replace the visible latest version in history
- delete is a soft delete, not physical removal
- append is optimized for incremental streaming but history returns full rolled-up messages
- subscribers may receive appends conflated or resynchronized as full updates
- annotation events feed a derived `message.summary`

Be explicit about phase 1 vs later phases.
```

## Prompt 2: Protocol And Data Model Changes

```md
Implement the protocol and storage model for message actions and versions.

Requirements:
- extend Sockudo V2 message model to carry:
  - action
  - stable message identifier
  - origin serial
  - version serial
  - operation metadata
  - optional annotation summary
- keep wire compatibility disciplined
- do not bloat every V1 delivery with V2-only metadata

Data model requirements:
- original message record
- latest visible version
- version chain for updates/deletes
- append handling that preserves a full materialized latest payload
- room for annotation summary state

Operation metadata should include:
- actor/client identifier when available
- description/reason
- opaque metadata map
- operation timestamp

Verification:
- protocol serialization tests
- backward compatibility tests for V1 stripping
- storage tests for latest-version and version-history correctness
```

## Prompt 3: Update, Delete, And Point-Version APIs

```md
Implement message update and delete behavior on top of the new model.

Requirements:
- expose server-side APIs for:
  - update message
  - delete message
  - get latest message by id/serial
  - get all message versions
- update/delete should publish a new action event but logically mutate the latest visible version
- delete must be soft delete only
- history must return the latest visible version in the position of the original message
- version history must retain prior versions
- support shallow mixin semantics for updating `data`, `name`, and `extras` only if that remains the chosen design after ADR review; otherwise document the alternative clearly

Important:
- authorize operations correctly
- reject updates against expired/nonexistent messages with explicit errors
- do not hide race conditions; use optimistic concurrency or another deliberate strategy

Verification:
- concurrent update/delete tests
- history shows latest version
- version history shows full lineage
- authorization tests
```

## Prompt 4: High-Rate Append Semantics

```md
Implement `message.append` in a way that is credible for token streaming and other incremental payload growth workloads.

Use the latest Ably docs as the semantic reference, especially:
- realtime subscribers may receive incremental `message.append`
- history and rewind should surface full materialized messages
- the first catch-up delivery after resume/rewind may be a full `message.update`
- append deliveries may be conflated

Engineering requirements:
- design for very high append frequency on a single hot message
- do not force a full durable read-modify-write on every append if you can avoid it
- if you use an append log plus asynchronously maintained materialized latest state, make the consistency contract explicit
- preserve ordering under realtime publish from the same connection
- define behavior for concurrent append and update/delete races
- define what subscribers should do when they receive `message.update` after prior appends

Deliver:
- server implementation
- wire/action behavior
- documentation comments in code for the tricky parts
- tests for:
  - append-only growth
  - append conflation or coalescing if implemented
  - append followed by update
  - append followed by delete
  - reconnect/resume with resynchronizing `message.update`
```

## Prompt 5: Annotation Events And Summary Engine

```md
Implement first-class message annotations in Sockudo.

Research basis:
- Ably annotation types and derived summaries
- `annotation.create`, `annotation.delete`, and `message.summary`

Requirements:
- annotations target an existing message by stable identifier
- support a typed annotation model with a namespace and summarization mode
- phase 1 should include a deliberately chosen subset of summary modes if implementing all of Ably's modes at once would reduce quality
- each annotation event is durably recorded
- each target message has a derived summary that can be sent live and returned in history/latest-message views
- summary updates must be idempotent and bounded

I want a senior design here:
- define which aggregation modes Sockudo supports first and why
- ensure cardinality limits are explicit
- ensure memory and storage costs are bounded
- ensure summary recomputation is either incremental and safe or batch-rebuildable from source events
- do not let annotation fanout crush hot channels

Verification:
- tests for publish/delete annotation
- tests for summary correctness
- tests for duplicate or repeated client actions
- tests for bounded cardinality behavior
```

## Prompt 6: Capabilities, Auth, And Abuse Controls

```md
Add the authorization and abuse-control layer for message modification and annotation features.

Requirements:
- define capability model for:
  - history read
  - message-update-own
  - message-update-any
  - message-delete-own
  - message-delete-any
  - annotation publish/delete
- make ownership semantics explicit
- ensure unidentified clients cannot use modes that require stable actor identity
- add rate limiting and abuse controls for:
  - append storms
  - annotation spam
  - expensive version-history queries

Also:
- add audit-friendly operation metadata
- add metrics for rejected operations by reason

Verification:
- auth tests
- rate-limit tests where the harness makes this realistic
- docs for each capability and error mode
```

## Prompt 7: Delivery, History, SDK, And Docs Surface

```md
Finish the public surface for annotations and message modification features.

Requirements:
- live subscribe behavior for all actions
- history behavior for latest visible message state
- latest-message lookup
- version-history lookup
- annotation summary in the message shape where appropriate
- selected SDK and docs updates

Be explicit in docs about:
- what actions clients can receive live
- which actions are durable vs derived
- how clients should process `message.append` vs `message.update`
- what history returns after updates/deletes/appends
- what annotation summaries include and exclude
- V1 vs V2 differences

Do not over-promise:
- if some SDKs are not updated in this pass, fix the gap
- if some summary modes are deferred, say so clearly and implement them

Verification:
- docs/examples compile where applicable
- action names and payload fields match implementation exactly
```

## Prompt 8: Final Review For Performance, Scale, And Correctness

```md
Act as the final reviewer for Sockudo's annotations and message modification stack.

Review the implementation with a bias for:
- high-frequency append performance
- version-chain storage growth
- summary corruption risk
- race conditions
- duplicate live deliveries
- incorrect history materialization
- authorization holes
- cardinality blowups
- multinode inconsistency

Then:
- fix the highest-risk issues
- add missing tests
- run relevant verification
- produce a short production-readiness report

The final report must include:
- changed files
- simplifications made
- remaining risks
- phase-2 items
- recommended production metrics and alerts
```

