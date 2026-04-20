# Release 4.4 Ultra Prompt Pack — Message Annotations

Prerequisite: Release 4.3 (Versioned Durable Messages) is complete. The versioned message substrate,
`message.create`/`message.update`/`message.delete`/`message.append` actions, durable serial-based identity,
winner-selection under racing mutations, and own/any capability enforcement are all shipped and tested.

Research binding context for every prompt in this pack:
- `docs/research/ably-parity/ably-message-annotations-research.md`
- `docs/research/ably-parity/sockudo-ably-parity-release-plan.md`

---

## Prompt 4.4-01: Architecture and release contract
```text
Implement Sockudo release 4.4: Annotation Engine And Summary Model.

Before writing any code, produce the release contract and commit it to the repo.

The contract must address each of the following explicitly:

Goals:
- Deliver Ably-equivalent annotation capability on top of the 4.3 versioned message substrate.
- Support all five Ably summarization methods: total.v1, flag.v1, distinct.v1, unique.v1, multiple.v1.
- Deliver summary changes as `message.summary` events at the original message's history position.
- Support individual raw annotation event streams via an opt-in subscription mode.
- Preserve Pusher/Sockudo V2 boundaries: annotations are V2-native only.

Non-goals:
- No UI or client SDK is required in this release — protocol and server runtime are the deliverables.
- Do not add annotation support to V1 channels.

Annotation type system:
- An annotation type has the format `namespace:summarizer.version` (e.g. `reactions:distinct.v1`).
- The namespace scopes aggregation — different namespaces never merge into the same summary bucket.
- The summarizer method (total, flag, distinct, unique, multiple) determines rollup rules.
- The version suffix (v1) is a forward-compat indicator; all current summarizers are v1.

Summary delivery model:
- Standard channel subscription continues to deliver ordinary messages via `subscribe()`.
- When an annotation changes a summary, Sockudo delivers a new event with action `message.summary`.
- The summary payload is nested at `message.annotations.summary` keyed by namespace+type.
- Clipping: when the contributing clientId list would exceed the configured max message size,
  `clipped` must be set to true, `total` must remain exact, and `clientIds` becomes partial.
  For `multiple.v1`, `totalClientIds` gives the exact count of distinct contributing identified clients.

Raw annotation subscription:
- Clients opting in to raw annotation events receive individual `annotation.create` and
  `annotation.delete` events through a separate subscription mode (`ANNOTATION_SUBSCRIBE`).
- Enabling this mode must not suppress ordinary message delivery; callers must add all needed
  modes explicitly. Document this hazard clearly.

Annotation persistence:
- Annotation events must be stored independently of message payloads.
- Summary projections are derived state rebuilt from the annotation event log.
- Enabling annotations on a channel forces message persistence (same as 4.3 versioned messages).

Release blockers that must be resolved before this release ships:
- Summary state must be convergent under concurrent annotation churn on a single node and across cluster nodes.
- Clipping must be deterministic and tested at boundary counts.
- Identified-client requirements must be enforced for flag.v1, distinct.v1, unique.v1, and multiple.v1.
- Summary delivery must survive channel reconnect and replay correctly.
```

---

## Prompt 4.4-02: Domain model and invariants
```text
Design and implement the annotation domain model for release 4.4.

Annotation entity shape (matches Ably's documented annotation message properties):
- `id`: unique annotation identifier
- `action`: `annotation.create` or `annotation.delete`
- `serial`: annotation's own serial (for ordering and deduplication)
- `messageSerial`: the top-level serial of the target message (from 4.3 identity model)
- `type`: `namespace:summarizer.version`
- `name`: optional string identifying the variant (required by distinct.v1, unique.v1, multiple.v1)
- `clientId`: publishing client identity (required by flag.v1, distinct.v1, unique.v1 for ownership semantics)
- `count`: integer for multiple.v1 (optional, default 1)
- `data`: raw annotation payload — not included in rolled-up summaries, only available via raw subscription
- `encoding`: optional
- `timestamp`

Invariants by summarizer:

total.v1:
- Counts the number of annotations of a given type on a message.
- Does not attribute to individual clients in the summary.
- Unidentified clients may contribute.
- Same client publishing twice increments twice.
- Delete decrements total by 1.
- Summary shape: `{ total: u64 }`

flag.v1:
- Counts distinct identified clients who published a given type.
- Identified clients are required — unidentified publishes are rejected.
- A client contributes at most once per type (idempotent subsequent publishes are ignored or no-op).
- Delete removes the client's contribution and decrements total.
- Summary shape: `{ total: u64, clientIds: Vec<String>, clipped: bool }`

distinct.v1:
- Counts distinct identified clients per annotation `name` under a type.
- A client may contribute once per name (publishing again to the same name is a no-op).
- The same client may contribute to multiple different names simultaneously.
- Delete removes the client from that named bucket and decrements that bucket total.
- Summary shape: `{ name → { total: u64, clientIds: Vec<String>, clipped: bool } }`

unique.v1:
- Like distinct.v1 but each client may only hold one active name at a time.
- Publishing a different name automatically removes the client from the previous name bucket.
- Delete removes the client from their active name bucket.
- Summary shape: same as distinct.v1

multiple.v1:
- Tracks total and per-client counts for a named annotation.
- Same client may contribute multiple times to the same name (each adds `count`).
- Unidentified contributions tracked via `totalUnidentified`.
- Delete removes all contributions by that `clientId` for that name.
- Summary shape: `{ name → { total: u64, clientCounts: Map<String, u64>, totalUnidentified: u64, clipped: bool, totalClientIds: u64 } }`

Projection model:
- A summary projection is the materialized rollup of all annotation events for one (channelId, messageSerial, type).
- Projections are rebuilt from the annotation event log if lost; they are derived, not canonical.
- The annotation event log is canonical.
```

---

## Prompt 4.4-03: Storage schema, migrations, and backfill
```text
Design and implement storage for annotation events and summary projections for release 4.4.

Annotation event table (add to all existing history backends: memory, MySQL, ScyllaDB, DynamoDB, SurrealDB):
- `channel_id`: channel identifier
- `message_serial`: the stable serial of the target message
- `annotation_serial`: the annotation's own serial (ordering + deduplication key)
- `type`: namespace:summarizer.version string
- `name`: nullable string
- `client_id`: nullable string
- `count`: nullable integer
- `action`: enum annotation.create | annotation.delete
- `data`: nullable blob
- `timestamp`

Indexes required:
- `(channel_id, message_serial, type)` — primary lookup for summary rebuild
- `(channel_id, message_serial, annotation_serial)` — for ordering and dedup
- `(channel_id, annotation_serial)` — for raw subscription replay

Summary projection table (cache/materialized view layer):
- `(channel_id, message_serial, type)` → serialized summary JSON + last_annotation_serial
- Purpose: fast delivery of current summary without replaying full annotation log
- If this table is lost or stale, it must be rebuilt from the annotation event log

Migration strategy:
- Annotation tables are additive; no existing message or history tables change.
- Memory backend: add annotation event store and summary projection map.
- Persistent backends: provide migrations under `ops/migrations/`.
- Backfill: channels with no annotations have empty annotation stores — no backfill needed.
- Rollback: drop annotation tables; no impact on message tables.

Retention:
- Annotation events follow the same retention rules as their parent message's history.
- When a message is evicted from history, its annotation events may also be evicted.
- Document this boundary clearly in operator guidance.
```

---

## Prompt 4.4-04: Protocol, API, and subscription contract
```text
Implement the Sockudo-native protocol for annotation publish/delete and summary subscription.

HTTP API (add to `crates/sockudo-server/src/http_handler.rs`):

POST /apps/{app_id}/channels/{channel_id}/messages/{message_serial}/annotations
  Body: { type, name?, count?, data? }
  Auth: write capability on channel + annotation-specific capability
  Returns: { annotationSerial }

DELETE /apps/{app_id}/channels/{channel_id}/messages/{message_serial}/annotations/{annotation_serial}
  Auth: annotation-delete-own or annotation-delete-any capability

GET /apps/{app_id}/channels/{channel_id}/messages/{message_serial}/annotations
  Query: type?, limit?, from_serial?
  Returns: paginated list of raw annotation events
  Auth: history capability + ANNOTATION_SUBSCRIBE mode

V2 WebSocket protocol (add to `crates/sockudo-protocol/src/messages.rs`):

annotation.create event shape:
{
  "event": "sockudo_internal:annotation",
  "data": {
    "action": "annotation.create",
    "id": "<annotation_id>",
    "serial": "<annotation_serial>",
    "messageSerial": "<target_message_serial>",
    "type": "reactions:distinct.v1",
    "name": "thumbsup",
    "clientId": "user-123",
    "count": 1,
    "data": null,
    "timestamp": 1700000000000
  }
}

annotation.delete event shape:
{
  "event": "sockudo_internal:annotation",
  "data": {
    "action": "annotation.delete",
    "serial": "<annotation_serial>",
    "messageSerial": "<target_message_serial>",
    "type": "reactions:distinct.v1",
    "name": "thumbsup",
    "clientId": "user-123",
    "timestamp": 1700000000000
  }
}

message.summary event shape (delivered on ordinary channel subscription):
{
  "event": "sockudo_internal:message",
  "data": {
    "action": "message.summary",
    "serial": "<target_message_serial>",
    "annotations": {
      "summary": {
        "reactions:distinct.v1": {
          "thumbsup": { "total": 5, "clientIds": ["a","b","c","d","e"], "clipped": false },
          "heart": { "total": 2, "clientIds": ["a","c"], "clipped": false }
        }
      }
    }
  }
}

Subscription mode for raw annotation events:
- V2 subscribe message gains an optional `modes` array.
- Adding `"ANNOTATION_SUBSCRIBE"` to modes enables raw annotation event delivery.
- Not adding it means the client only receives `message.summary` snapshots on ordinary subscribe.
- Warning: explicitly setting `modes` replaces defaults — the caller must include all required modes.

Replay:
- Raw annotation events participate in the connection recovery replay buffer (same serial ordering as other V2 messages).
- Summary events do not need to be replayed; the server delivers a fresh summary snapshot on reattach.
```

---

## Prompt 4.4-05: Auth, capabilities, and security
```text
Implement authorization and identity enforcement for release 4.4.

New capabilities (add to capability model in `crates/sockudo-core/src/options.rs` and auth validation):

`annotation-publish`:
  - Required to publish annotations to a channel.
  - Without this, HTTP annotation POST and realtime annotation publishes are rejected with 403.

`annotation-delete-own`:
  - Client may delete their own annotations (matched by `clientId`).
  - Ownership is identity-based: the deleting client's `clientId` must match the annotation's `clientId`.
  - Unidentified clients cannot use this capability safely.

`annotation-delete-any`:
  - Client may delete any annotation on the channel regardless of ownership.
  - Requires identified client.

`annotation-subscribe`:
  - Required to receive raw `annotation.create` / `annotation.delete` events.
  - Summary delivery (`message.summary`) does not require this — it arrives via ordinary subscribe.

Identity requirements by summarizer:
- `total.v1`: unidentified clients are permitted to publish.
- `flag.v1`: identified `clientId` required on publish; reject unidentified with 400 + clear error message.
- `distinct.v1`: identified `clientId` required on publish.
- `unique.v1`: identified `clientId` required on publish.
- `multiple.v1`: unidentified contributions are tracked separately under `totalUnidentified`.

Validation rules:
- Reject annotation publish if the target `messageSerial` does not exist in channel history.
- Reject annotation publish if annotations are not enabled on the channel (feature gate check).
- Reject annotation delete if the annotation referenced by `annotation_serial` does not exist (return 404, not 500).
- `annotation-delete-own` must verify `clientId` match before deleting; failing silently or deleting anyway is a security bug.
```

---

## Prompt 4.4-06: Server and runtime implementation
```text
Implement the full annotation runtime for release 4.4.

Annotation publish path (`crates/sockudo-adapter/src/handler/mod.rs` and `local_adapter.rs`):
1. Validate capability and identity requirements for the summarizer type.
2. Validate that the target `messageSerial` exists in the channel's history store.
3. Assign a new `annotation_serial` (use the same serial generation mechanism as V2 message serials).
4. Write the annotation event to the annotation event store.
5. Load the current summary projection for `(channel_id, message_serial, type)`.
6. Apply the summarizer delta to the projection (see invariants from Prompt 4.4-02).
7. Check if the updated summary would exceed the configured max message payload size:
   - if yes, set `clipped: true`, trim `clientIds` list, but preserve exact `total` count.
8. Write the updated projection back to the summary projection store.
9. Deliver the updated `message.summary` event to all subscribers of this channel.
10. If any subscribers have `ANNOTATION_SUBSCRIBE` mode, also deliver the raw `annotation.create` event.

Annotation delete path:
1. Validate capability (annotation-delete-own or annotation-delete-any).
2. For annotation-delete-own: verify `clientId` match with the stored annotation.
3. Write `annotation.delete` event to the annotation event log.
4. Recompute the summary projection delta (inverse of the original contribution).
5. Update the summary projection and deliver updated `message.summary`.
6. Deliver raw `annotation.delete` to ANNOTATION_SUBSCRIBE subscribers.

Summary rebuild path (for recovery or cache miss):
- Replay all annotation events for `(channel_id, message_serial, type)` in `annotation_serial` order.
- Apply each create/delete delta according to the summarizer rules.
- Final state is the rebuilt projection.

The five summarizer engines must be implemented as separate, tested modules:
- `TotalSummarizer` — stateless per-event delta
- `FlagSummarizer` — tracks contributing clientId set
- `DistinctSummarizer` — tracks per-name clientId sets
- `UniqueSummarizer` — tracks per-client active name + per-name clientId sets
- `MultipleSummarizer` — tracks per-name per-client count + totalUnidentified

Do not build a monolithic switch statement; each summarizer should be a distinct type implementing a common trait.
```

---

## Prompt 4.4-07: Cluster and distributed correctness
```text
Harden release 4.4 for clustered operation.

The central risk is that summary projections are materialized state derived from annotation events.
Under concurrent annotation churn across cluster nodes, projections can diverge.

Requirements:

Annotation event ordering:
- Annotation serials must be globally ordered across all nodes (same rules as V2 message serials).
- Two concurrent annotation events for the same (channel, messageSerial, type) must produce
  deterministic, convergent summary projections regardless of which node processes them first.

Summary convergence strategy:
- Option A (event sourcing): never cache summary projections on individual nodes; always rebuild
  from the authoritative annotation event log in the shared backing store.
  Pro: always correct. Con: read-heavy under high annotation rates.
- Option B (versioned projections): cache the projection with a `last_annotation_serial` watermark.
  On summary read, compare watermark against the annotation event log's max serial; if stale, replay delta.
  This is the preferred approach for high-throughput summarizers.

Write contention:
- `flag.v1`, `distinct.v1`, `unique.v1`, and `multiple.v1` require atomic read-modify-write on the projection.
- Use optimistic concurrency on the projection record (CAS on `last_annotation_serial`).
- If the CAS fails, retry by re-reading the current projection and re-applying the delta.

Summary delivery after cluster mutation:
- When a node updates a projection, it must fan out the `message.summary` event to all cluster nodes
  so every subscriber regardless of which node they are on receives the updated summary.
- Use the existing cluster adapter pub/sub for fan-out (same path as ordinary V2 message broadcast).

Recovery:
- After a cluster node restart, summary projection cache is cold.
- First annotation publish or read triggers lazy rebuild from annotation event log.
- This must not block delivery of subsequent events while rebuild is in progress.
```

---

## Prompt 4.4-08: Client and SDK behavior
```text
Define the client-facing consumption model for release 4.4 and add examples.

Summary consumption (simplest and most common path):
- Client subscribes to the channel normally (no extra modes required).
- When any annotation changes the summary, Sockudo delivers a `message.summary` event.
- The client merges the summary payload into its local message state:
  `message.annotations.summary[type]` = new summary object.
- There is no guarantee the client sees every intermediate summary snapshot; only the latest matters.

Raw annotation stream consumption (opt-in):
- Client subscribes with `modes: ["SUBSCRIBE", "ANNOTATION_SUBSCRIBE"]`.
- Individual `annotation.create` and `annotation.delete` events arrive.
- Client is responsible for updating its own local summary projection from raw events if it uses them.
- Warning: explicitly setting `modes` removes default modes — document this prominently in examples.

Reconnect behavior:
- On reconnect, the client should request a fresh summary snapshot via history or the server's
  latest-summary endpoint rather than replaying raw annotation events from the last known point.
  Raw annotation replay is only useful for precise audit, not for UX summary display.

Examples to add to `examples/` directory:

1. `reactions_summary.js` — subscribe to a channel, render live reaction counts from `message.summary` events
   (uses distinct.v1 reactions, shows clipping handling when `clipped: true`)

2. `read_receipts.js` — publish `flag.v1` annotations as read receipts and consume the running summary
   (shows identified-client requirement and total + clientIds in the summary)

3. `moderation_flags.js` — annotate messages with `multiple.v1` moderation scores, consume per-client counts
   (shows totalUnidentified for anonymous reporting)

These examples should be self-contained, runnable against a local Sockudo instance, and referenced from docs.
```

---

## Prompt 4.4-09: Framework integration and release engineering
```text
Prepare release 4.4 for downstream adoption.

Channel feature flag:
- Annotations must be enabled per-channel or per-namespace via a channel rule.
- Add `annotations_enabled: bool` to the channel options model in `crates/sockudo-core/src/options.rs`.
- When annotations are disabled on a channel, annotation publish attempts return 403 with a clear message.
- Enabling annotations implicitly enables message persistence on that channel
  (add this enforcement to the channel creation / rule-update path).

App manager schema:
- Add `annotations_enabled` to the App entity so it can be stored and queried by all app manager backends.

Config reference:
- Document `annotations_enabled` in the channel rule config section of the docs.
- Document the persistence billing implication (enabling annotations forces message retention).

Release engineering checklist:
- Confirm all five summarizers have isolated unit tests with deterministic inputs/outputs.
- Confirm cluster convergence is tested with concurrent annotation churn.
- Confirm clipping boundary is tested at exact threshold count (e.g. clientIds truncated at configurable max).
- Confirm identified-client rejection is tested for flag/distinct/unique summarizers.
- Confirm summary delivery happens on every annotation change, not only on net-positive changes.
- Confirm annotation delete against a non-existent annotation returns 404.
- Confirm annotation events survive channel reconnect replay.
- Confirm the `ANNOTATION_SUBSCRIBE` mode warning is documented in SDK examples.

Release note draft input:
- New feature: five annotation summarizers (total, flag, distinct, unique, multiple).
- New event: `message.summary` delivered on ordinary subscribe when annotation state changes.
- New mode: `ANNOTATION_SUBSCRIBE` for raw annotation event streams.
- New capabilities: `annotation-publish`, `annotation-delete-own`, `annotation-delete-any`.
- Breaking: none — annotations are opt-in per channel.
- Dependency: requires Release 4.3 (Versioned Durable Messages) to be present.
```

---

## Prompt 4.4-10: Test matrix and automation
```text
Build and run the full test matrix for release 4.4.

Add tests to `crates/sockudo-adapter/tests/` following the existing pattern.

Unit tests per summarizer (in `crates/sockudo-adapter/src/annotations/` or similar):
- total.v1: increment on create, decrement on delete, same client increments twice
- flag.v1: one contribution per client, delete removes contribution, unidentified client rejected
- distinct.v1: per-name buckets, same client in multiple names, delete from named bucket
- unique.v1: client moves between names on re-publish, only one active name per client
- multiple.v1: per-client counts, unidentified tracked separately, delete removes all for clientId

Clipping tests:
- Configure max clientIds per summary to a small number (e.g. 3).
- Publish annotations from N > 3 distinct clients.
- Assert `clipped: true`, `total` equals N, `clientIds.len()` equals 3.
- For multiple.v1: assert `totalClientIds` equals N.

Integration tests (full message flow):
- Publish a message, publish annotations, assert `message.summary` is delivered.
- Publish annotation as unidentified client for flag.v1 — assert 400 rejection.
- Delete annotation — assert summary decrements and updated `message.summary` is delivered.
- Subscribe with `ANNOTATION_SUBSCRIBE` — assert both summary and raw events are received.
- Subscribe without `ANNOTATION_SUBSCRIBE` — assert only summary events are received.
- Replay after reconnect — assert summary snapshot is coherent.

Cluster tests:
- Two nodes, concurrent annotation publishes on the same message from both nodes.
- Assert final summary on both nodes converges to identical state.
- Assert no double-counting or missed contributions.

Auth tests:
- annotation-delete-own: client deletes own annotation (success), deletes another's (403).
- annotation-delete-any: client deletes any annotation (success with capability).
- Missing annotation-publish capability: publish rejected with 403.

Fix all discovered failures before completing this prompt.
```

---

## Prompt 4.4-11: Observability, docs, and operator guidance
```text
Add metrics, logs, docs, and operator guidance for release 4.4.

Metrics (add to `crates/sockudo-metrics/src/prometheus.rs`):
- `sockudo_annotations_published_total{channel, type}` — counter per annotation publish
- `sockudo_annotations_deleted_total{channel, type}` — counter per annotation delete
- `sockudo_annotation_summary_deliveries_total{channel}` — counter per summary delivery
- `sockudo_annotation_summary_clipped_total{channel, type}` — counter per clipped summary (signals oversized contributor list)
- `sockudo_annotation_projection_rebuild_total{channel}` — counter per full projection rebuild (cache miss recovery)
- `sockudo_annotation_projection_rebuild_duration_seconds{channel}` — histogram for rebuild latency

Logs (structured, using `tracing`):
- WARN when a summary is clipped (include channel, messageSerial, type, contributor count)
- WARN when a projection rebuild is triggered on a hot channel (include channel, annotation count)
- ERROR when an annotation event fails to fan out to the cluster broadcast path

Docs updates:
- Add `docs/content/2.server/22.annotations.md` covering:
  - annotation type format (`namespace:summarizer.version`)
  - all five summarizers with use cases and summary shapes
  - clipping behavior and configuration
  - channel feature flag (`annotations_enabled`)
  - identified-client requirements
  - `ANNOTATION_SUBSCRIBE` mode and its mode-override hazard
  - HTTP API reference
  - V2 wire event shapes
  - operator guidance: monitoring clipping metrics, rebuilding projections, retention implications

- Update `docs/content/5.reference/1.protocol.md` with annotation event shapes.
- Update `docs/content/5.reference/4.compatibility.md` noting annotations are V2-only.
- Update `docs/content/5.reference/3.http-api.md` with annotation endpoints.
```

---

## Prompt 4.4-12: Final verification and release audit
```text
Run the final release audit for 4.4.

Verification checklist:

Functional correctness:
- All five summarizers produce correct summary state under unit test inputs.
- Clipping is deterministic at the configured threshold.
- `message.summary` is delivered on every annotation change.
- Raw annotation events are delivered only to subscribers with `ANNOTATION_SUBSCRIBE` mode.
- Identified-client enforcement is active for flag.v1, distinct.v1, unique.v1.
- multiple.v1 correctly separates identified and unidentified contributions.
- Annotation delete against non-existent annotation returns 404.
- Summary projection rebuilds correctly from annotation event log.

Security:
- `annotation-delete-own` does not allow deleting another client's annotation.
- Unidentified clients are rejected on summarizers that require identity.
- Annotation publish on a channel with `annotations_enabled: false` returns 403.

Cluster correctness:
- Concurrent annotation churn from multiple nodes converges to the same summary.
- No double-counting or ghost contributions after node failover.

Observability:
- Clipping metrics fire when expected.
- Rebuild metrics fire on cache miss.

Docs:
- All event shapes in docs match implementation.
- Summarizer invariants are documented correctly (especially unique.v1 name-switch behavior).
- `ANNOTATION_SUBSCRIBE` mode hazard is clearly documented.

Deliver:
1. Any findings and exact evidence.
2. Go/no-go decision with justification.
3. If no-go: enumerated blockers with owner and fix scope.
4. Release note draft for 4.4.
5. Readiness statement for proceeding to 4.5 (Push Notifications).
```
