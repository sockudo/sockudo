# ADR 0001: V2 Durable History, Rewind, And Verified Recovery

Status: Proposed

## Context

Sockudo V2 currently supports only hot recovery via an in-memory per-channel replay buffer:

- [`crates/sockudo-adapter/src/replay_buffer.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-adapter/src/replay_buffer.rs) assigns a channel-local `serial`, stores pre-serialized bytes, and replays by `serial`.
- [`crates/sockudo-adapter/src/handler/recovery.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-adapter/src/handler/recovery.rs) handles `pusher:resume` with `{ channel_serials }` and fails only when the in-memory window no longer covers the request.
- [`crates/sockudo-adapter/src/handler/connection_management.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-adapter/src/handler/connection_management.rs) stamps `serial` and `message_id` on the publish path, then stores serialized bytes in the replay buffer.
- [`crates/sockudo-protocol/src/messages.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-protocol/src/messages.rs) has `serial` and `message_id`, but no durable stream token, history cursor, or rewind semantics.
- [`docs/content/2.server/15.connection-recovery.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/content/2.server/15.connection-recovery.md) documents buffer-only recovery.
- [`FEATURE_COMPARISON.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/FEATURE_COMPARISON.md) calls out missing rewind, pagination, forward/reverse history, and stream epoch tracking.

That design is fast, but it cannot:

- survive node restart or process-local buffer loss,
- support paginated history,
- support rewind on attach/subscribe,
- prove recovery continuity across multinode delivery,
- distinguish retention expiry from stream reset or persistence loss.

The new design must preserve the current hot path discipline:

- V1 remains unchanged.
- V2 keeps the fast in-memory replay buffer.
- No global publish lock.
- No full JSON parse and reserialize on the critical path.
- Multinode ordering and failure semantics are first-class.

## Decision

Introduce a dual-layer V2 history system:

1. `Hot recovery layer`
   - Keep the existing per-channel in-memory replay buffer.
   - Upgrade it from `serial`-only to `stream_id + serial + bytes`.
   - Use it for ultra-fast reconnect and live handoff after rewind.

2. `Cold history layer`
   - Add a durable append-only `HistoryStore` beside the hot buffer.
   - Persist pre-serialized message bytes plus small indexed columns.
   - Support history pagination, forward/reverse iteration, rewind, and recovery beyond the hot window.

3. `Channel stream continuity token`
   - Every V2 channel stream is identified by `(stream_id, serial)`.
   - `stream_id` is the channel epoch. It is stable across normal appends and normal retention trimming.
   - `stream_id` rotates only when continuity from the prior stream can no longer be proven.
   - `serial` remains a monotonically increasing per-channel sequence within a single `stream_id`.

4. `Per-channel sequencer`
   - Ordering is assigned before fanout through a channel-sharded sequencer, not per-node local counters.
   - The sequencing surface is `reserve_next(channel)` or `reserve_block(channel, n)`.
   - This is a per-`app_id + channel` coordination point, not a global lock.

5. `Verified recovery, not optimistic recovery`
   - Recovery succeeds only when Sockudo can prove continuity from the client token to the available stream.
   - If `stream_id` mismatches, serial gaps exist, the durable floor is ahead of the requested serial, or the persistence layer is degraded in a way that breaks proof, the server returns a structured failure.

## Why This Shape

- Replacing the current replay path with synchronous persistence would violate the throughput constraint.
- Keeping only the replay buffer cannot satisfy history or multinode recovery correctness.
- A channel epoch without durable metadata ownership is hand-wavy; the durable stream metadata record must own epoch minting and rotation.
- Storing serialized bytes preserves byte ordering for delta compatibility and avoids a parse-reserialize loop on publish.

## Phase 1 Backend Choice

Phase 1 adds a generic `HistoryStore` trait, but ships one durable backend: PostgreSQL.

Rationale:

- Sockudo already carries SQLx/Postgres support, so this avoids a new infrastructure dependency.
- Postgres can do append-only writes plus atomic per-channel metadata updates with predictable operational behavior.
- A Kafka/Pulsar/NATS-backed durable log would add more moving parts, more operator burden, and more failure modes than phase 1 needs.

Non-Postgres backends remain out of scope for phase 1, but the trait boundary should make MySQL/Scylla implementations possible later.

## Stream Continuity Model

### Token

The logical stream token is:

`ChannelStreamToken { stream_id, serial }`

Properties:

- `stream_id`: opaque UUID/ULID-like string, channel-specific, owned by durable stream metadata.
- `serial`: `u64`, increasing within a `stream_id`.

### Epoch Ownership

`stream_id` is owned by a durable `channel_streams` metadata record keyed by `(app_id, channel)`.

Only the `HistoryStore` may mint or rotate it.

### When `stream_id` Changes

`stream_id` does not change for normal retention expiry.

It changes only when continuity cannot be proven anymore, for example:

- the stream metadata was lost and recreated,
- a durable append gap cannot be repaired,
- an operator purge/reset intentionally truncates the stream,
- the system resumes after a storage failure with an unknown durable head,
- a channel is reinitialized after unrecoverable corruption.

### What Detects Truncation

Normal retention trimming is detected by:

- `oldest_available_serial`,
- `newest_available_serial`,
- and cursor/history bounds.

Abnormal loss/reset is detected by `stream_id` change.

This is stronger than serial-only because:

- serial alone cannot distinguish “you are too old” from “the stream was reset,”
- serial alone cannot detect loss after node restart or storage bootstrap,
- serial alone cannot prove continuity across multinode writers.

## Data Model Changes

### New Runtime Types

- `ChannelStreamToken { stream_id: String, serial: u64 }`
- `BufferedMessage { stream_id, serial, message_bytes, published_at }`
- `HistoryAppend { app_id, channel, stream_id, serial, published_at_ms, message_id, event_name, payload_bytes, node_id }`
- `RecoveryPosition { stream_id, serial, last_message_id? }`

### Durable Metadata Table

`channel_streams`

- `app_id`
- `channel`
- `stream_id`
- `next_serial`
- `durable_head_serial`
- `oldest_available_serial`
- `history_enabled`
- `durable_state` (`healthy | degraded | reset_required`)
- `last_persisted_at`
- `updated_at`

Primary key:

- `(app_id, channel)`

Purpose:

- owns epoch,
- assigns or reserves serials,
- tracks the durable floor/head,
- records whether continuity is currently provable.

### Durable History Table

`channel_history`

- `app_id`
- `channel`
- `stream_id`
- `serial`
- `published_at_ms`
- `message_id`
- `event_name`
- `payload_bytes`
- `node_id`

Primary key:

- `(app_id, channel, stream_id, serial)`

Indexes:

- `(app_id, channel, published_at_ms desc, serial desc)` for time-bounded reverse reads
- `(app_id, channel, published_at_ms asc, serial asc)` or equivalent for forward reads

Notes:

- `payload_bytes` stores the already serialized V2 payload.
- Indexed side columns are small and extracted from the in-memory `PusherMessage`, not by reparsing stored bytes.
- Ephemeral messages never enter either hot recovery or durable history.

### Hot Buffer Changes

Extend the current replay buffer to store:

- `stream_id`
- `serial`
- `message_bytes`
- `published_at`

The hot buffer remains bounded by TTL and count.

## API And Protocol Changes

### Wire Messages

Keep V1 unchanged.

For V2 broadcast messages:

- keep `serial`
- add `stream_id`
- keep `message_id`

This keeps existing V2 consumers mostly intact while exposing a tokenizable continuity model.

Example:

```json
{
  "event": "price-update",
  "channel": "market:BTC",
  "data": "{\"price\":42000}",
  "stream_id": "01HSZ7J0J4P8S7X3JCM4H9G2A1",
  "serial": 57,
  "message_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

### Resume Protocol

Extend `pusher:resume` for V2 to accept:

```json
{
  "channel_positions": {
    "market:BTC": {
      "stream_id": "01HSZ7J0J4P8S7X3JCM4H9G2A1",
      "serial": 54
    }
  }
}
```

Compatibility:

- continue accepting legacy `channel_serials` in phase 1,
- but treat it as hot-buffer-only compatibility,
- and fail with `continuity_unverifiable` when continuity cannot be proven without `stream_id`.

### Structured Recovery Failure

Replace vague expiry-only failures with structured reasons:

```json
{
  "event": "sockudo:resume_failed",
  "channel": "market:BTC",
  "data": {
    "code": "continuity_lost",
    "reason": "stream_id_mismatch",
    "expected_stream_id": "01HSZ7...",
    "current_stream_id": "01HT0A...",
    "oldest_available_serial": 80,
    "newest_available_serial": 120
  }
}
```

Codes:

- `continuity_unverifiable`
- `stream_reset`
- `history_gap`
- `position_expired`
- `persistence_unavailable`
- `invalid_position`

### Rewind On Subscribe

Extend V2 `pusher:subscribe` payload with:

```json
{
  "channel": "market:BTC",
  "rewind": {
    "count": 50
  }
}
```

Also support:

- `rewind: { since: { stream_id, serial } }`
- `rewind: { start_ms, end_ms, limit, direction }`

Phase 1 should support:

- `count`
- `since`

Time-bounded rewind can land in phase 2 if it threatens scope.

### History HTTP API

Add:

`GET /apps/{app_id}/channels/{channel}/history`

Query params:

- `limit`
- `direction=backwards|forwards`
- `cursor`
- `start_ms`
- `end_ms`
- `from_serial`
- `from_stream_id`

Response:

- `items`
- `has_more`
- `next_cursor`
- `stream_id`
- `oldest_available_serial`
- `newest_available_serial`

Cursor format:

- opaque, base64url-encoded server token
- contains channel, stream, direction, and boundary serial
- not user-constructed

### Subscribe/Live Handoff

For rewinded subscriptions:

1. capture current channel head token,
2. mark the subscription as live-gated,
3. send history up to the captured head,
4. replay any hot-buffer messages after that head if needed,
5. clear the gate and allow normal live delivery.

This avoids a gap between history delivery and live fanout.

## Publish Path

Phase 1 publish sequence for history-enabled V2 channels:

1. resolve channel history policy,
2. reserve `(stream_id, serial)` from the channel sequencer,
3. stamp `stream_id`, `serial`, `message_id`,
4. serialize once,
5. store bytes in hot buffer,
6. enqueue append to durable history shard,
7. fan out live delivery immediately.

Important:

- durable append is asynchronous,
- live fanout does not wait for disk,
- continuity state is updated by the durable writer and recovery/history APIs consult that state before claiming success.

## Failure Model

### Recoverable Cases

- Client asks for a serial still present in hot buffer: recover from hot layer.
- Client asks for older serial still present in durable history with matching `stream_id`: recover from cold layer.
- Durable history is briefly behind live head but hot buffer covers the gap: recover by combining hot and cold layers.

### Hard Fail Cases

Fail recovery or history rather than silently healing when:

- requested `stream_id` does not match current stream,
- requested serial is older than `oldest_available_serial`,
- durable writer marked the stream `degraded` with unproven continuity,
- durable append queue overflow caused unknown loss,
- storage outage or bootstrap leaves head/floor unknown,
- the client sends only legacy `channel_serials` and the hot layer cannot prove continuity.

### Persistence Outage Semantics

On durable append failure:

- keep live fanout running,
- keep hot buffer running,
- mark the affected channel stream metadata `degraded`,
- emit metrics and logs,
- fail history requests and cold recovery for that channel until continuity is re-established or the stream is intentionally rotated.

Do not pretend persistence succeeded.

### Queue Backpressure

Use bounded per-shard append queues.

When a queue is full:

- reject the enqueue,
- mark the stream degraded,
- increment a backpressure metric,
- keep live delivery fast,
- force recovery/history APIs to fail until continuity is restored or epoch is rotated.

This is explicit backpressure, not silent dropping.

## Metrics And Observability Plan

Add counters/gauges/histograms for:

- `history_appends_total`
- `history_append_failures_total`
- `history_append_latency_ms`
- `history_queue_depth`
- `history_queue_overflow_total`
- `history_reads_total`
- `history_read_latency_ms`
- `history_items_returned_total`
- `recovery_attempts_total`
- `recovery_success_total`
- `recovery_failure_total{code=...}`
- `recovery_hot_hits_total`
- `recovery_cold_hits_total`
- `recovery_mixed_hits_total`
- `stream_epoch_rotations_total`
- `stream_oldest_available_serial`
- `stream_durable_head_serial`
- `stream_degraded_channels`
- `rewind_requests_total`
- `rewind_handoff_latency_ms`

Logs and structured fields:

- `app_id`
- `channel`
- `stream_id`
- `serial`
- `requested_stream_id`
- `requested_serial`
- `failure_code`
- `queue_depth`
- `storage_backend`
- `node_id`

Admin/debug surfaces to add:

- history store health in `/metrics` and `/stats`,
- channel stream metadata inspection endpoint or debug dump,
- explicit degraded-stream log on first transition, not every failed append.

## Phased Implementation Plan

### Phase 0: Contract And Scaffolding

- Add the ADR and protocol notes.
- Add `HistoryStore` trait and disabled implementation.
- Add `ChannelStreamToken` and metadata types.
- Add config stubs for history and rewind policy.
- Add tests for token serialization, config parsing, and API shape only.

### Phase 1: Verified Hot + Cold Recovery

- Add `stream_id` to V2 messages.
- Upgrade replay buffer to track `stream_id`.
- Add Postgres-backed `HistoryStore`.
- Add `channel_streams` and `channel_history` migrations.
- Add per-channel sequencer backed by `channel_streams`.
- Add async append worker with bounded shard queues.
- Extend `pusher:resume` to accept `channel_positions`.
- Recover from hot buffer first, then durable history when needed.
- Return structured failure when continuity is not provable.

Test strategy:

- unit tests for sequencer, stream rotation rules, cursor encoding, and failure mapping,
- adapter tests for hot-only, cold-only, mixed recovery, and degraded-stream failure,
- integration tests for multinode ordered publish with one shared Postgres store,
- restart tests proving hot loss but cold recovery success,
- outage tests proving recovery fails when continuity cannot be proven.

### Phase 2: History API And Rewind

- Add `GET /channels/{channel}/history`.
- Add forward and reverse pagination.
- Add cursor-based reads.
- Add subscribe-time rewind with live gate and hot handoff.
- Add namespace/app policy for history retention and rewind allowance.

Test strategy:

- API pagination tests,
- rewind count tests,
- rewind plus live publish race tests,
- cursor round-trip tests,
- retention floor tests.

### Phase 3: Policy, Retention, And Additional Backends

- Per-namespace history retention and limits.
- Time-bounded rewind and time-bounded history filters.
- Additional `HistoryStore` backends if needed.
- Operator tooling for purge/reset that intentionally rotates `stream_id`.

## Config And Policy Changes

Global config:

```toml
[history]
enabled = false
backend = "postgres"
append_queue_shards = 64
append_queue_capacity = 10000
default_ttl_seconds = 86400
default_max_items = 100000
```

Per-app policy:

```toml
[app_manager.array.apps.policy.history]
enabled = true
ttl_seconds = 86400
max_items = 100000
allow_rewind = true
```

Per-namespace policy should arrive in phase 2 or 3, not phase 1, unless it falls out naturally from existing namespace plumbing.

## Out Of Scope For Phase 1

- V1 history or V1 recovery changes.
- New non-Postgres durable backends.
- Cross-region globally ordered history.
- Compaction or conflation-aware history storage.
- Message-level selective “skip durable history” beyond existing `ephemeral`.
- Historical replay of delta chains as deltas; phase 1 replays full stored messages.
- Full Ably parity for every history bound form.
- Operator UX for manual stream repair beyond basic reset/purge primitives.

## Consequences

Positive:

- Sockudo gains durable history, rewind, and correctness-first recovery.
- Multinode delivery has a single continuity model instead of process-local counters.
- The fast replay buffer remains the primary path for brief reconnects.

Tradeoffs:

- History-enabled V2 channels now depend on a per-channel sequencer and a durable metadata store.
- Recovery may fail more often than today in ambiguous cases, by design.
- Phase 1 backend coverage is intentionally narrow to keep the system operable.

## Implementation Notes

- Preserve the existing byte-preserving discipline: serialize once, then store/fan out the same bytes.
- Keep the recovery and history logic V2-only.
- Reuse existing app policy and subscription extension points rather than inventing parallel config or protocol surfaces.
