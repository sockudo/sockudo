# Sockudo AI Transport Wire Protocol

Status: Draft locked for S1+ implementation
Scope: Sockudo server platform primitives for Ably AI Transport parity
Source of truth: code audit on 2026-06-02, plus `plans/ai-transport/01-server-parity-prompts.md` sections 1 and 2

This document defines the canonical server wire protocol and architecture for Sockudo AI Transport parity. Part A documents the existing Sockudo primitives with source references. Part B specifies the remaining platform gaps. The AI SDK owns turns, trees, view reduction, codecs, and agent orchestration; Sockudo supplies validated realtime, history, recovery, identity, presence, push, and error primitives.

## Part A: Audited Existing Behavior

### Versioned Messages

Sockudo already has a mutable-message protocol. `MessageAction` has `Create`, `Update`, `Delete`, `Append`, and `Summary`; operation strings are `message.create`, `message.update`, `message.delete`, `message.append`, and `message.summary`, and V2 wire events are `sockudo:{operation}` (`crates/sockudo-protocol/src/versioned_messages.rs:12`, `crates/sockudo-protocol/src/versioned_messages.rs:22`). The core domain enum mirrors those variants (`crates/sockudo-core/src/versioned_messages.rs:62`).

`VersionedRealtimeMessage` flattens `PusherMessage` into the top-level JSON object and adds `action`, `message_serial`, optional `history_serial`, optional `delivery_serial`, and optional `version` (`crates/sockudo-protocol/src/versioned_messages.rs:83`). The `version` object contains `serial`, optional `client_id`, `timestamp_ms`, optional `description`, and optional `metadata` (`crates/sockudo-protocol/src/versioned_messages.rs:62`). V2 validation requires non-empty `message_serial`, a matching `event`, a non-empty `channel`, present `version`, present `history_serial`, present `delivery_serial`, and `message.serial == delivery_serial` (`crates/sockudo-protocol/src/versioned_messages.rs:97`).

The flattened `PusherMessage` fields are `event`, `channel`, `data`, `name`, `user_id`, `tags`, `sequence`, `conflation_key`, `message_id`, `stream_id`, `serial`, `idempotency_key`, `extras`, `__delta_seq`, and `__conflation_key` (`crates/sockudo-protocol/src/messages.rs:219`). `tags` use `BTreeMap` for deterministic serialization (`crates/sockudo-protocol/src/messages.rs:231`). The top-level `idempotency_key` is internal pipeline metadata and is never sent to WebSocket clients (`crates/sockudo-protocol/src/messages.rs:252`, `crates/sockudo-adapter/src/v2_broadcast.rs:15`).

`message_serial` is the stable logical identity for a mutable message. `version_serial` identifies one concrete version. Both reject empty values, whitespace, and values over 128 bytes (`crates/sockudo-core/src/versioned_messages.rs:7`, `crates/sockudo-core/src/versioned_messages.rs:43`). `history_serial` is a `u64` attached to `MessageIdentity`; `delivery_serial` is a `u64` replay position (`crates/sockudo-core/src/versioned_messages.rs:84`). Update/delete/append preserve message identity and require strictly increasing `version_serial` and `delivery_serial` (`crates/sockudo-core/src/versioned_messages.rs:168`, `crates/sockudo-core/src/versioned_messages.rs:198`, `crates/sockudo-core/src/versioned_messages.rs:242`). Version chains reject mixed identities, mixed `history_serial`, duplicate `version_serial`, and duplicate `delivery_serial`; replay requires contiguous delivery serials after the requested position (`crates/sockudo-core/src/versioned_messages.rs:260`, `crates/sockudo-core/src/versioned_messages.rs:291`, `crates/sockudo-core/src/versioned_messages.rs:310`).

`VersionStore` reserves delivery positions, appends versions, reads latest versions, pages version history, replays after a delivery serial, projects latest-by-history, reports stream state, and purges old records (`crates/sockudo-core/src/version_store.rs:129`). `reserve_delivery_position` returns a stable per-channel `stream_id` and monotonic `delivery_serial`; the memory store starts at 1 and advances with saturating add (`crates/sockudo-core/src/version_store.rs:71`, `crates/sockudo-core/src/version_store.rs:252`, `crates/sockudo-core/src/version_store.rs:276`). `get_latest` returns the max `version_serial` for one logical message (`crates/sockudo-core/src/version_store.rs:339`). `latest_by_history` returns one latest version per logical message, sorted by original `history_serial` ascending (`crates/sockudo-core/src/version_store.rs:454`).

The server builds wire `VersionedRealtimeMessage` records from stored versions by using the action event name, the stored channel/data/name/extras, `serial = delivery_serial`, `history_serial`, `delivery_serial`, and version metadata (`crates/sockudo-server/src/http_handler.rs:685`). HTTP routes expose `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}`, `GET /messages/{messageSerial}/versions`, and `POST /messages/{messageSerial}/{update|delete|append}` (`crates/sockudo-server/src/main.rs:3200`, `crates/sockudo-server/src/main.rs:3206`, `crates/sockudo-server/src/main.rs:3230`).

HTTP update accepts optional `name`, `data`, `extras`, `clear_fields`, `client_id`, `socket_id`, `description`, `metadata`, and `op_id`, and requires at least one patch or operation metadata field (`crates/sockudo-protocol/src/versioned_messages.rs:150`). HTTP delete accepts optional `data`, `extras`, `clear_fields`, `client_id`, `socket_id`, `description`, `metadata`, and `op_id` (`crates/sockudo-protocol/src/versioned_messages.rs:199`). HTTP append accepts required non-empty string `data` and optional `extras`, `client_id`, `socket_id`, `description`, `metadata`, and `op_id` (`crates/sockudo-protocol/src/versioned_messages.rs:231`). Mutation responses are `{ channel, message_serial, action, accepted, version_serial, history_serial, delivery_serial, status }`; replayed `op_id` mutations return `status: "duplicate"` with the original serial acknowledgement (`crates/sockudo-protocol/src/versioned_messages.rs:280`, `crates/sockudo-server/src/http_handler.rs:2527`, `crates/sockudo-server/src/http_handler.rs:2684`, `crates/sockudo-server/src/http_handler.rs:2838`).

Authorization for mutable messages is capability-based. Connection capabilities expose `message_update_own`, `message_update_any`, `message_delete_own`, `message_delete_any`, `message_append_own`, and `message_append_any`, with `*`, exact, and wildcard channel matching (`crates/sockudo-core/src/websocket.rs:305`, `crates/sockudo-core/src/websocket.rs:376`). Authorization grants `*_any` first; `*_own` requires an identified actor, identified original creator, and matching client IDs; privileged server requests bypass as server-authorized (`crates/sockudo-core/src/versioned_message_auth.rs:38`). HTTP mutations with `socket_id` require a connected V2 socket, optional requested `client_id` matching the signed-in actor, and matching connection capabilities; without `socket_id`, the mutation is privileged server-side (`crates/sockudo-server/src/http_handler.rs:734`, `crates/sockudo-server/src/http_handler.rs:790`, `crates/sockudo-server/src/http_handler.rs:816`).

Normal HTTP publish still uses `PusherApiMessage` with `name`, `data`, `channel`, `channels`, `socket_id`, `info`, `tags`, `delta`, `idempotency_key`, `message_id`, and `extras` (`crates/sockudo-protocol/src/messages.rs:608`). `POST /apps/{appId}/events` returns `{ "ok": true }`, or `{ "channels": ... }` when `info`, `message_id`, or an AI event requests acknowledgement; AI/versioned publish acknowledgements include `message_serial`, `history_serial`, `delivery_serial`, and `version_serial` (`crates/sockudo-server/src/http_handler.rs:1929`, `crates/sockudo-server/src/http_handler.rs:2057`, `crates/sockudo-server/src/http_handler.rs:2105`). `POST /apps/{appId}/batch_events` uses `{ "batch": [PusherApiMessage] }` and returns `{}` unless any item requested info, supplied `message_id`, or used an AI event name, in which case it returns `{ "batch": [...] }` in request order (`crates/sockudo-protocol/src/messages.rs:643`, `crates/sockudo-server/src/http_handler.rs:2142`, `crates/sockudo-server/src/http_handler.rs:2247`, `crates/sockudo-server/src/http_handler.rs:2349`). Versioned create is generated by the broadcast path when versioned messages and durable history are enabled; WS inbound dispatch currently handles subscribe/unsubscribe/signin/ping/pong/delta/resume/client events and has no inbound handler for message mutations (`crates/sockudo-adapter/src/handler/connection_management.rs:235`, `crates/sockudo-adapter/src/handler/mod.rs:657`).

### Extras, Message IDs, And Idempotency

`MessageExtras` is a V2-only metadata envelope with `headers`, `ephemeral`, `idempotency_key`, `push`, and `echo` (`crates/sockudo-protocol/src/messages.rs:22`). `headers` are scalar-only; nested objects and arrays are rejected before deserialization (`crates/sockudo-protocol/src/messages.rs:58`). `extras` are stripped from V1 deliveries and included only for V2 (`crates/sockudo-protocol/src/messages.rs:258`, `crates/sockudo-adapter/src/local_adapter.rs:1171`).

`message_id` is generated as UUIDv4 (`crates/sockudo-protocol/src/messages.rs:80`). HTTP publish pre-fills `message_id` only when global connection recovery is enabled (`crates/sockudo-server/src/http_handler.rs:1331`). The broadcast path fills missing message IDs when history or recovery is enabled (`crates/sockudo-adapter/src/handler/connection_management.rs:282`). Direct V2 sends fill a missing message ID for non-ping/pong messages (`crates/sockudo-adapter/src/local_adapter.rs:1167`, `crates/sockudo-adapter/src/local_adapter.rs:1482`).

HTTP publish idempotency uses either body `idempotency_key` or `X-Idempotency-Key`, with the body value winning, and rejects empty or over-length keys (`crates/sockudo-server/src/http_handler.rs:1485`). Single-event dedupe uses cache key `app:{app_id}:idempotency:{key}`, stores `__processing__`, waits briefly for concurrent completion, then caches the final response (`crates/sockudo-server/src/http_handler.rs:1519`, `crates/sockudo-server/src/http_handler.rs:1555`). Batch idempotency supports a batch-level key and per-event keys; duplicate per-event keys are skipped (`crates/sockudo-server/src/http_handler.rs:1755`, `crates/sockudo-server/src/http_handler.rs:1849`). `extras.idempotency_key` dedupes by channel-scoped key `idempotency:{app}:{channel}:{key}` and silently drops duplicates (`crates/sockudo-adapter/src/handler/connection_management.rs:221`). Push has separate durable idempotency: `PublishRequest` has `publish_id` but no idempotency key; the idempotency key is a stable hash of canonical `PublishIntent` persisted as `IdempotencyRecord` (`crates/sockudo-server/src/push_http.rs:202`, `crates/sockudo-push/src/domain.rs:894`, `crates/sockudo-push/src/storage.rs:115`).

### Durable History

`HistoryStore` exposes `reserve_publish_position`, `append`, `read_page`, `stream_runtime_state`, `stream_inspection`, `reset_stream`, `purge_stream`, `runtime_status`, and `purge_before` (`crates/sockudo-core/src/history.rs:387`). `HistoryAppendRecord` stores app, channel, `stream_id`, monotonic `serial`, `published_at_ms`, optional `message_id`, optional event name, operation kind, raw `Bytes` payload, and retention policy (`crates/sockudo-core/src/history.rs:81`). `HistoryItem` returns stream/serial/time/message metadata plus raw `Bytes` payload (`crates/sockudo-core/src/history.rs:102`).

`HistoryCursor` contains version, app, channel, stream, serial, direction, and bounds; it encodes as Sonic JSON followed by URL-safe base64 without padding, and decode enforces version 1 (`crates/sockudo-core/src/history.rs:33`). `HistoryReadRequest` validates non-zero limits, serial/time bounds, and cursor app/channel/direction/bounds consistency (`crates/sockudo-core/src/history.rs:330`). Direction is `newest_first` or `oldest_first` (`crates/sockudo-core/src/history.rs:11`). HTTP history accepts `limit`, `direction`, `cursor`, `start_serial`, `end_serial`, `start_time_ms`, `end_time_ms`, plus `start`/`end` aliases (`crates/sockudo-server/src/http_handler.rs:235`). Direction aliases include `backwards`, `reverse`, `forwards`, and `forward` (`crates/sockudo-server/src/http_handler.rs:557`).

Memory history starts each channel at serial 1, reserves positions with a stable stream ID, appends records, and evicts by retention window, max messages, and max bytes (`crates/sockudo-core/src/history.rs:554`, `crates/sockudo-core/src/history.rs:650`, `crates/sockudo-core/src/history.rs:592`). Purge modes are `all`, `before_serial`, and `before_time_ms`, and validation enforces the required bound shape (`crates/sockudo-core/src/history.rs:221`, `crates/sockudo-core/src/history.rs:248`). `purge_before` is also available for retention workers (`crates/sockudo-core/src/history.rs:452`).

The HTTP route is `GET /apps/{appId}/channels/{channelName}/history` (`crates/sockudo-server/src/main.rs:3250`). The handler validates the channel, resolves history policy, rejects disabled history, decodes cursor/bounds, reads a page, and returns `items`, `direction`, `limit`, `has_more`, `next_cursor`, `bounds`, `continuity`, and `stream_state` (`crates/sockudo-server/src/http_handler.rs:2884`, `crates/sockudo-server/src/http_handler.rs:2994`, `crates/sockudo-server/src/http_handler.rs:3006`). When versioned messages are enabled, history rows that reference a mutable message are substituted with `version_store.get_latest(...)`, so HTTP history returns the aggregated latest visible message where possible (`crates/sockudo-server/src/http_handler.rs:2943`).

History state, reset, and purge routes exist at `/history/state`, `/history/reset`, and `/history/purge`; reset/purge require destructive-operation confirmation (`crates/sockudo-server/src/main.rs:3291`, `crates/sockudo-server/src/main.rs:3298`, `crates/sockudo-server/src/main.rs:3305`, `crates/sockudo-server/src/http_handler.rs:276`, `crates/sockudo-server/src/http_handler.rs:285`). These endpoints are signed app API surfaces, not client-token surfaces.

### Rewind On Subscribe

`SubscriptionRewind` supports count or seconds. Count may be sent as an integer, numeric string, or `{ "count": n }`; seconds may be sent as `{ "seconds": n }` or `{ "duration_seconds": n }` (`crates/sockudo-adapter/src/handler/types.rs:13`). Count uses no history time bounds; seconds converts to `start_time_ms = now - seconds * 1000` (`crates/sockudo-adapter/src/handler/types.rs:45`). Rewind is V2-only and requires `history.rewind_allowed()` (`crates/sockudo-adapter/src/handler/validation.rs:71`).

Subscribe starts a rewind gate, performs subscription, reads history, delivers historical items, drains buffered live messages, and sends `sockudo:rewind_complete` with `historical_count`, `live_count`, `complete`, `truncated_by_retention`, and `truncated_by_limit` (`crates/sockudo-adapter/src/handler/message_handlers.rs:206`, `crates/sockudo-adapter/src/handler/subscription_management.rs:427`, `crates/sockudo-adapter/src/handler/subscription_management.rs:527`). Count rewind reads newest-first up to `min(count, max_page_size)` and reverses for delivery; seconds rewind reads oldest-first with time bounds (`crates/sockudo-adapter/src/handler/subscription_management.rs:1045`, `crates/sockudo-adapter/src/handler/subscription_management.rs:1064`). Buffered live messages are filtered to serials greater than the history head and to message IDs not already delivered (`crates/sockudo-adapter/src/handler/subscription_management.rs:1024`).

### Connection Recovery

Recovery constants are `pusher:resume`, `pusher:resume_success`, and `pusher:resume_failed` (`crates/sockudo-protocol/src/constants.rs:19`). Protocol prefix rewriting exposes them as `sockudo:resume*` to V2 clients (`crates/sockudo-protocol/src/protocol_version.rs:140`). `pusher:resume` data accepts `channel_positions: { channel: { serial, stream_id?, last_message_id? } }`; legacy `channel_serials: { channel: serial }` is accepted for hot replay only (`crates/sockudo-adapter/src/handler/recovery.rs:600`).

`pusher:resume_success` has no channel and string JSON data `{ "recovered": [{ "channel", "source", "replayed" }], "failed": [...] }` (`crates/sockudo-adapter/src/handler/recovery.rs:185`). Per-channel `pusher:resume_failed` has channel set and string JSON data with `code`, `reason`, `expected_stream_id`, `current_stream_id`, `oldest_available_serial`, and `newest_available_serial` (`crates/sockudo-adapter/src/handler/recovery.rs:506`).

Resume first asks the hot replay buffer. Hot recovery returns source `"hot"`; stream mismatch returns `stream_reset`; expiry falls through to cold recovery (`crates/sockudo-adapter/src/handler/recovery.rs:222`). Legacy serial-only positions cannot use cold recovery (`crates/sockudo-adapter/src/handler/recovery.rs:249`). If versioned messages are enabled, cold recovery uses `VersionStore::stream_state` and `replay_after`, validates stream ID, and emits source `"cold"` (`crates/sockudo-adapter/src/handler/recovery.rs:260`). Otherwise cold recovery reads durable history oldest-first from `start_serial = serial + 1`, validates retained stream ID, and fails retention gaps as `position_expired` (`crates/sockudo-adapter/src/handler/recovery.rs:413`). `stream_reset` is used for stream mismatch/reset-required; `position_expired` is used for retained floor gaps and hot-expired-without-durable-history (`crates/sockudo-adapter/src/handler/recovery.rs:43`, `crates/sockudo-adapter/src/handler/recovery.rs:347`, `crates/sockudo-adapter/src/handler/recovery.rs:447`).

### Annotations

Annotations already cover Ably-style message annotations. Annotation actions are `annotation.create` and `annotation.delete` (`crates/sockudo-core/src/annotations.rs:95`). Annotation records carry `id`, `action`, `serial`, `message_serial`, `type`, optional `name`, optional `client_id`, optional `count`, optional `data`, optional `encoding`, and `timestamp` (`crates/sockudo-core/src/annotations.rs:112`). Wire raw annotation payloads use camelCase and serialize action/id/serial/messageSerial/type/name/clientId/count/data/encoding/timestamp (`crates/sockudo-protocol/src/messages.rs:97`).

The internal event names are `sockudo_internal:annotation` and `sockudo_internal:message` (`crates/sockudo-protocol/src/messages.rs:85`). Summary payloads are `{ action: "message.summary", serial, annotations: { summary } }` (`crates/sockudo-protocol/src/messages.rs:120`). Runtime delivery broadcasts the summary event first and then the raw annotation event (`crates/sockudo-adapter/src/handler/annotations.rs:322`). Summary messages are ephemeral via `extras.ephemeral = true` (`crates/sockudo-adapter/src/handler/annotations.rs:449`).

Supported summarizers are `total`, `flag`, `distinct`, `unique`, and `multiple` (`crates/sockudo-core/src/annotations.rs:45`). Summary outputs are `Total { total }`, `Flag { total, client_ids, clipped }`, `Distinct { name -> identified }`, `Unique { name -> identified }`, and `Multiple { name -> multiple }` (`crates/sockudo-core/src/annotations.rs:181`). Projection rebuild sorts events by annotation serial, deduplicates repeated serials, and applies the summarizer engine (`crates/sockudo-core/src/annotations.rs:235`). Oversized projections can be rebuilt with lower client-id limits and emit clipping metrics (`crates/sockudo-adapter/src/handler/annotations.rs:220`, `crates/sockudo-adapter/src/handler/annotations.rs:372`).

HTTP annotations are `POST /apps/{appId}/channels/{channelName}/messages/{messageSerial}/annotations`, `GET` on the same path, and `DELETE /annotations/{annotationSerial}` (`crates/sockudo-server/src/main.rs:3213`, `crates/sockudo-server/src/main.rs:3222`). Publish requires valid channel, versioned messages, annotations enabled, durable history enabled, and an existing message (`crates/sockudo-server/src/http_handler.rs:2442`). Delete only targets an existing `annotation.create`; repeated delete is idempotent if a delete event for the same annotation ID already exists (`crates/sockudo-server/src/http_handler.rs:2508`, `crates/sockudo-adapter/src/handler/annotations.rs:115`). Listing returns `{ channel, message_serial, limit, has_more, next_cursor, items }` and can filter by annotation type/from serial (`crates/sockudo-server/src/http_handler.rs:317`, `crates/sockudo-server/src/http_handler.rs:2599`).

### Push

Sockudo has a `sockudo-push` crate and a feature-gated HTTP surface under `/apps/{appId}/push/*` (`crates/sockudo-server/src/main.rs:3327`). Routes include credentials, templates, device registrations, channel subscriptions, publish, batch publish, status, scheduled-job delete, and delivery status (`crates/sockudo-server/src/main.rs:3330`, `crates/sockudo-server/src/main.rs:3372`, `crates/sockudo-server/src/main.rs:3390`, `crates/sockudo-server/src/main.rs:3410`, `crates/sockudo-server/src/main.rs:3428`, `crates/sockudo-server/src/main.rs:3450`).

Push auth uses signed app HTTP auth plus `x-sockudo-push-capability`; absent capability defaults to admin, and `push-subscribe` is narrower (`crates/sockudo-server/src/push_http.rs:1279`). Device registration is `POST /push/deviceRegistrations`; admin can create or rotate a device identity token, while subscribe-mode updates require an existing device token (`crates/sockudo-server/src/push_http.rs:448`). Device details include app ID, device ID, optional client ID, form factor, platform, metadata, hashed device secret, timezone, locale, last activity, push details, and optional rate policy (`crates/sockudo-push/src/domain.rs:378`). Device identity tokens are verified against stored hashed secrets (`crates/sockudo-server/src/push_http.rs:1296`).

Channel subscriptions are `POST/GET/DELETE /push/channelSubscriptions`; the model is app, channel, device ID, optional client ID, provider, token hash, and optional credential version (`crates/sockudo-server/src/push_http.rs:583`, `crates/sockudo-push/src/domain.rs:479`). Publish requests contain `publishId`, `recipients`, `payload`, `providerOverrides`, `sync`, `notBeforeMs`, and `expiresAtMs` (`crates/sockudo-server/src/push_http.rs:202`). Targets include device, client, channel, registered topic, user topic, raw recipient, provider topic, provider condition, and indexed filter; `Channel { channel }` is explicit (`crates/sockudo-push/src/domain.rs:803`). Publish acceptance persists status, durable idempotency, and a publish log event, then enqueues work (`crates/sockudo-server/src/push_http.rs:835`, `crates/sockudo-server/src/push_http.rs:954`, `crates/sockudo-server/src/push_http.rs:998`). Realtime HTTP publish with `extras.push` enqueues a V2 channel push for each target channel (`crates/sockudo-server/src/http_handler.rs:1658`).

Scheduled jobs exist in storage and the scheduler polls due jobs (`crates/sockudo-push/src/storage.rs:105`, `crates/sockudo-push/src/storage.rs:316`, `crates/sockudo-push/src/scheduler.rs:41`). The current public HTTP surface exposes delete/cancel at `DELETE /push/scheduled/{jobId}` but no route that creates `ScheduledPushJob` (`crates/sockudo-server/src/main.rs:3450`, `crates/sockudo-server/src/push_http.rs:774`).

### Existing Error And Limit Surface

WebSocket errors are `pusher:error` payloads with `{ code, message }` and are prefix-rewritten for V2 (`crates/sockudo-protocol/src/messages.rs:430`, `crates/sockudo-adapter/src/local_adapter.rs:1482`). Existing relevant WS numeric codes include 4000, 4001, 4003, 4004, 4006-4009, 4100, 4200-4202, 4300-4303 (`crates/sockudo-core/src/error.rs:180`, `crates/sockudo-adapter/src/handler/recovery.rs:89`). HTTP errors are JSON `{ error, code, status }` with codes such as `auth_failed`, `forbidden`, `missing_channel_info`, `limit_exceeded`, `rate_limited`, `backpressure`, `payload_too_large`, `invalid_input`, `feature_disabled`, and `not_found` (`crates/sockudo-server/src/http_handler.rs:107`).

Existing limit/config surfaces include `[event_limits].max_payload_in_kb`, `[event_limits].max_batch_size`, `[http_api].request_limit_in_mb`, `[history].max_page_size`, `[history].retention_window_seconds`, `[history].rewind_enabled`, `[versioned_messages].max_page_size`, `[versioned_messages].retention_window_seconds`, `[presence].max_members_per_channel`, `[presence].max_member_size_in_kb`, `[presence_history]` retention/page caps, and `[push]` worker/fanout/status/quota/scheduler knobs (`config/config.toml:280`, `config/config.toml:382`, `crates/sockudo-core/src/options.rs:1315`, `crates/sockudo-core/src/options.rs:1142`, `crates/sockudo-core/src/options.rs:1607`, `crates/sockudo-core/src/options.rs:1440`).

### CLAUDE.md Discrepancies

`CLAUDE.md` says the workspace has 12 crates and omits `sockudo-push` (`CLAUDE.md:9`); the actual workspace includes `crates/sockudo-push/Cargo.toml` and the server has push routes (`crates/sockudo-server/src/main.rs:3330`). `CLAUDE.md` lists V2 recovery, deltas, and tag filtering but omits versioned messages, mutable message wire events, durable history, annotations, and push (`CLAUDE.md:145`, `CLAUDE.md:163`). The code contains versioned-message protocol types (`crates/sockudo-protocol/src/versioned_messages.rs:12`), durable `HistoryStore` (`crates/sockudo-core/src/history.rs:387`), annotation stores and summaries (`crates/sockudo-core/src/annotations.rs:444`), and push domain/storage/runtime surfaces (`crates/sockudo-push/src/domain.rs:378`, `crates/sockudo-push/src/storage.rs:316`).

## Part B: Required AI Transport Gaps

### Feature Gates And Runtime Gates

New AI Transport production code MUST be behind Cargo feature `ai-transport`, propagated through `crates/sockudo-server/Cargo.toml`. The feature belongs in `full`, not default, until GA. Runtime activation is `[ai_transport] enabled = true` plus channel pattern matching under `[[ai_transport.channels]]`.

Protocol V1 behavior is immutable. AI Transport validation, headers, capability tokens, rollup, untilAttach history, and presence timeout are V2-only. Any new field on an existing frame MUST be optional. Any behavior that changes timing or visible semantics for existing clients MUST default off unless explicitly specified otherwise.

### AI Events And Headers

The five AI event names are plain channel events:

- `ai-input`: client-to-agent codec/input events.
- `ai-output`: agent-to-client codec/output events.
- `ai-turn-start`: turn lifecycle start.
- `ai-turn-end`: turn lifecycle end.
- `ai-cancel`: cancellation signal.

These events ride the existing publish path and, for streaming content, the existing `sockudo:message.create|append|update|delete` mutable-message events. Sockudo remains domain-blind: it validates the transport tier and passes the codec tier opaquely.

AI metadata is a convention over the existing `extras` field:

```json
{
  "extras": {
    "ai": {
      "transport": {
        "turn-id": "turn_123",
        "codec-message-id": "msg_123",
        "status": "streaming"
      },
      "codec": {
        "provider-specific-key": "opaque string"
      }
    }
  }
}
```

`extras.ai.transport` and `extras.ai.codec` are string-to-string maps. Per tier: at most 32 keys, key at most 64 bytes and matching `[a-z0-9-]+`, value at most 256 bytes of valid UTF-8. Unknown transport keys are rejected. Codec keys are not semantically interpreted but still obey tier limits.

Transport keys:

| Key | Domain |
| --- | --- |
| `turn-id` | Non-empty opaque string |
| `turn-client-id` | Verified client identity string |
| `turn-reason` | `complete`, `cancelled`, `error`, `suspended` |
| `codec-message-id` | Non-empty opaque string; equals Sockudo `message_serial` for streamed mutable messages |
| `parent` | Parent `codec-message-id` |
| `fork-of` | Fork source `codec-message-id` |
| `msg-regenerate` | `true` or `false` |
| `stream` | `true` or `false` |
| `stream-id` | Non-empty opaque string |
| `status` | `streaming`, `complete`, `cancelled` |
| `discrete` | `true` or `false` |
| `invocation-id` | Non-empty opaque string |
| `event-id` | Non-empty opaque string |
| `input-client-id` | Verified client identity string |
| `role` | `user`, `assistant`, `system`, `tool`, `agent` |
| `turn-continue` | `true` or `false` |
| `error-code` | Non-empty opaque string |
| `error-message` | Human-readable string within value limit |

Anti-spoof rule: any `*-client-id` value MUST match the verified connection identity from authenticated socket state or capability token. Trusted signed app-key HTTP publishes are exempt. Client publishes may send `ai-input` and `ai-cancel`; `ai-output`, `ai-turn-start`, and `ai-turn-end` require trusted app key until capability tokens land, then require an agent-capable token.

Canonical sequences, expressed in existing events:

Normal streaming turn:

1. `ai-turn-start` with `turn-id`, `invocation-id`, `turn-client-id`.
2. `sockudo:message.create` for `ai-output` content with `codec-message-id = message_serial`, `stream=true`, `status=streaming`.
3. Zero or more `sockudo:message.append` events for the same message serial with `status=streaming`.
4. Final `sockudo:message.append` or `sockudo:message.update` with `status=complete`.
5. `ai-turn-end` with `turn-reason=complete`.

Cancellation:

1. Client publishes `ai-cancel` with `turn-id`.
2. Agent or server-side publisher emits final append/update with `status=cancelled`.
3. `ai-turn-end` with `turn-reason=cancelled`.

Abort partial:

1. Stream starts normally.
2. Agent emits final update with accumulated content and `status=cancelled`.
3. `ai-turn-end` with `turn-reason=cancelled`.

Error:

1. Stream starts normally.
2. Agent emits update with `status=complete`, `error-code`, and `error-message`, or emits no final content if no message was created.
3. `ai-turn-end` with `turn-reason=error`.

Suspended continuation:

1. Agent emits `ai-turn-end` with `turn-reason=suspended`.
2. Later continuation uses same `turn-id`, `turn-continue=true`, and a new `invocation-id`.

Regenerate:

1. New output create uses `msg-regenerate=true` and `fork-of=<old codec-message-id>`.
2. Parent/fork metadata is persisted in `extras.ai.transport`.

Edit:

1. Edited user input is a new `ai-input` or mutable update with `parent=<previous codec-message-id>`.
2. Downstream outputs fork from that parent.

### Idempotency Unification

Existing `idempotency_key` semantics are frozen. AI SDK-facing idempotency is additive:

- Client-supplied `message_id` on V2 publish and HTTP events becomes the preferred idempotent-create key.
- Dedupe key: `(app_id, channel, message_id)`.
- Duplicate create returns the original `message_serial`, `history_serial`, and `version_serial`; it MUST NOT rebroadcast.
- Existing body/header `idempotency_key` remains honored exactly as today.
- If both `message_id` and `idempotency_key` are present, both are registered; a duplicate by either path returns the original create result where possible.

Mutations add optional `op_id` to append/update/delete requests. Dedupe key: `(app_id, channel, message_serial, action, op_id)`. Duplicate mutation is a no-op returning the current latest version and the original mutation result if available. `op_id` values are scoped per action and logical message and are retained for at least the same TTL as publish idempotency.

### Publish And Mutation Acks

V2 publish/create acks and mutation responses MUST expose:

```json
{
  "message_serial": "msg_...",
  "history_serial": 123,
  "delivery_serial": 456,
  "version_serial": "ver_..."
}
```

HTTP mutation responses include the required serial fields while preserving the older required fields. Duplicate `op_id` mutations return the cached/current serial acknowledgement with `status: "duplicate"`.

### Append Rollup

Append rollup coalesces egress only. Persistence and version storage receive every append. The first append in a stream is delivered immediately. Subsequent appends are coalesced by `(app, channel, message_serial, fanout partition)` for a configured window: `0`, `20`, `40`, `100`, or `500` ms. Default is `40` ms. Sockudo v1 implements the fanout partition as node-local server-wide channel fanout because the adapter prepares one egress message for the channel rather than one frame per subscriber. A terminal append/update with `status=complete|cancelled` flushes pending state before the terminal event.

Connection parameter: `append_rollup_window=<ms>`. Per-app config clamps allowed values. `0` disables rollup. Current server v1 validates the connection parameter but uses `[ai_transport.rollup].default_window_ms` for node-local channel fanout. Rate limiting counts original mutation requests before rollup; egress metrics count coalesced deliveries. Rollup output must reduce to the same mutable message state as the unrolled append stream.

### UntilAttach And Client History

Subscriptions capture `attach_serial` after authorization and before live delivery. Client history access is exposed through existing SDK `channel_history` semantics, extended for V2 client access. The WS contract extends that existing action instead of adding a parallel history action:

- Request event: `sockudo:channel_history` (the unprefixed `channel_history` action is also accepted for existing SDK plumbing)
- Response event: `sockudo:channel_history`

Request data:

```json
{
  "channel": "chat",
  "limit": 100,
  "direction": "newest_first",
  "cursor": "opaque",
  "start_serial": 1,
  "end_serial": 10,
  "start_time_ms": 1710000000000,
  "end_time_ms": 1710000100000,
  "until_attach": true
}
```

`limit` MUST be `1..=1000` with default `100`; server policy may lower the effective page size. `direction` defaults to backwards/newest-first and accepts `backwards`, `reverse`, `newest_first`, `forwards`, `forward`, and `oldest_first`. `until_attach=true` bounds `end_serial` to the subscription attach serial and gives gaplessness: history returns messages with `history_serial <= attach_serial`; live delivery continues with `history_serial > attach_serial`. Returned messages are aggregated via version-store latest projection, matching HTTP history substitution. Access requires `history` capability once capability tokens land. Existing rewind remains subscribe-time replay; client history is request/response pagination.

### Capability Tokens

Capability tokens are V2-only JWTs signed with HS256. Header `kid` identifies the app key. Claims:

- `x-sockudo-capability`: JSON map from channel pattern to operations.
- `x-sockudo-client-id`: verified client identity.
- Standard `exp`, `iat`, optional `nbf`.
- Required `jti` for revocation.

Operations: `publish`, `subscribe`, `history`, `presence`. Pattern rules are exact match, namespace wildcard `ns:*`, and global `*`; matching is case-sensitive. HMAC app-key auth remains supported and trusted. Token auth is additive and never weakens existing private/presence HMAC rules.

Enforcement matrix:

| Operation | Required capability |
| --- | --- |
| Subscribe public/private | `subscribe` |
| Presence subscribe | `subscribe` + `presence` |
| Publish `ai-input`/`ai-cancel` | `publish` |
| Publish `ai-output`/turn events | `publish` plus agent/trusted token extension, or app key |
| Mutable create | `publish` |
| Mutable update/delete/append own | `publish` plus existing own mutation policy |
| Mutable update/delete/append any | app key or explicit future admin mutation capability |
| Client history | `history` |
| Push channel subscription | `subscribe` or push-subscribe legacy capability |
| Push publish to channel | `publish` or app key |

Refresh uses a new `sockudo:auth` frame. Expiry uses code `40142`. Revocation checks `jti` through the cache/storage layer and fails closed.

### Presence Update And Timeout

New frame: `sockudo:presence_update`. It mutates member data for the authenticated presence member without leave/re-enter. Presence snapshots MUST include current member data. Existing `member_added` and `member_removed` remain unchanged.

Ungraceful disconnect timeout is opt-in and defaults to 15 seconds when enabled. During the grace window, membership remains present but marked internally as pending removal. Successful V2 resume cancels removal and MUST NOT emit a member flap. Expiry emits exactly one removal.

### Error Registry

HTTP responses retain `{ error, code, status }`; WS errors retain `pusher:error` / `sockudo:error` payload shape.

| Code | Name | HTTP | When | Recovery |
| --- | --- | --- | --- | --- |
| 40000 | bad_request | 400 | malformed JSON, invalid field, invalid direction | fix request |
| 40003 | forbidden | 403 | auth/capability denied | refresh credentials or request permission |
| 40142 | token_expired | 401 | capability token expired | send `sockudo:auth` refresh |
| 40160 | token_revoked | 401 | `jti` revoked | obtain new token |
| 40009 | payload_too_large | 413 | message/header/content cap exceeded | split or reduce payload |
| 93002 | mutable_not_permitted | 403 | message mutations not permitted on this channel | use a mutable-enabled channel |
| 104000 | ai_invalid_transport_header | 400 | bad AI transport key/domain | fix headers |
| 104001 | ai_header_too_large | 400 | tier/key/value limit exceeded | reduce headers |
| 104002 | ai_client_id_spoof | 403 | `*-client-id` does not match verified identity | use verified identity |
| 104003 | ai_event_not_permitted | 403 | client published agent-only AI event | use app key/agent token |
| 104004 | ai_rollup_window_invalid | 400 | invalid append rollup value | use allowed value |
| 104005 | ai_until_attach_gap | 409 | attach/history continuity cannot be proven | resubscribe |
| 104006 | ai_op_duplicate_conflict | 409 | same op_id with different payload | use new op_id |
| 104007 | ai_stream_limit_exceeded | 413 | accumulated stream cap exceeded | start new message |
| 104008 | ai_capability_pattern_invalid | 400 | invalid token capability map | fix token |
| 104009 | ai_presence_update_invalid | 400 | invalid presence update payload | fix payload |
| 104010 | ai_reserved | 400 | reserved for SDK/platform expansion | see message |

For SDK compatibility, use code `93002` with exact message `mutations not permitted on this channel` when mutable message operations are disabled by channel/server policy.

### Limits And Defaults

| Limit | Default | Maximum | Config |
| --- | ---: | ---: | --- |
| Message payload | 64 KiB | 256 KiB per app | existing event payload limits |
| Accumulated stream content | 1 MiB | per app clamp | `[ai_transport]` |
| Appends per message | 4096 | per app clamp | `[ai_transport]` |
| Open streaming messages per channel | 1024 | per app clamp | `[ai_transport]` |
| AI transport keys per tier | 32 | 32 | fixed |
| AI header key bytes | 64 | 64 | fixed |
| AI header value bytes | 256 | 256 | fixed |
| Append rollup window | 40 ms | 500 ms | `[ai_transport]`, connection param |
| Client history page | 100 | 1000 | existing history page clamp plus client cap |
| Rewind count/page | existing history max page size | existing clamp | `[history].max_page_size` |
| Presence disconnect grace | off; 15 s when enabled | per app clamp | `[presence]` |
| History retention | existing defaults | existing backend caps | `[history]` |
| Version retention | existing defaults | existing backend caps | `[versioned_messages]` |

Do not introduce a second knob for an existing limit. AI-specific knobs only cover AI-only concepts: header registry, accumulated stream cap, append count cap, rollup clamp, and presence grace opt-in.

### Versioned Message Aggregation Appendix

Aggregation happens in the existing versioned-message subsystem. Each mutation writes a `StoredVersionRecord` through `VersionStore::append_version`; the latest aggregate is selected by `get_latest`, and channel projections use `latest_by_history`. The memory, PostgreSQL, and MySQL backends keep per-message version chains and return the highest `version_serial` as the visible aggregate. DynamoDB, ScyllaDB, and SurrealDB resolve latest pointers with backend-specific follow-up fetches; this is functionally equivalent but can be more read-amplified for large channel projections.

Append aggregation currently folds string data at mutation application time (`VersionedMessage::apply_append`). The latest record therefore carries the full accumulated string, while individual append versions remain in the replay log. S2 caps now reject AI Transport appends before delivery serial reservation when the aggregate would exceed `[ai_transport].max_accumulated_message_bytes` or the append count would exceed `[ai_transport].max_appends_per_message`.

Terminal stream status is persisted in the aggregate via `extras.ai.transport.status`. Append requests may carry `extras`; when present, those extras replace the previous aggregate extras so a terminal append with `status=complete|cancelled` remains visible through `get_latest`, HTTP message reads, and history substitution.

Deletion is a tombstone-style latest version. History reads that encounter the original create row substitute the latest visible version, so deleted messages appear as `sockudo:message.delete` with the delete version's retained/cleared fields. Version retention and history retention are independent: if version-store purge removes the version chain while history rows remain, history substitution cannot materialize the latest aggregate and falls back to the retained raw history payload.

### Scale-Out Notes

Versioned delivery positions are reserved by `VersionStore::reserve_delivery_position`; a channel has a stable stream ID and monotonic delivery serial sequence. Backends must preserve atomic reservation semantics across nodes. Durable history positions are reserved separately by `HistoryStore::reserve_publish_position`; regular publish history serials and versioned delivery serials are distinct continuity domains.

Rollup happens at egress. It must not affect durable storage, version chains, history serial reservation, webhook payloads, push triggers, or recovery replay. A node-local rollup engine is acceptable because rollup is a delivery cadence optimization; correctness is the reduced mutable state.

An orphaned-stream janitor is required for streams with `status=streaming` and no terminal update after a configured TTL. The janitor must emit operator-visible metrics and optionally a synthetic terminal update with `status=cancelled` only when app policy allows it.

### HTTP SDK Surface Appendix

S7 verified the server API surface required by `@sockudo/ai-transport`:

- `POST /apps/{appId}/events` accepts signed app-key AI events with `extras.ai.transport`, `extras.ai.codec`, and optional client-supplied `message_id`. When the event name is an AI event or `message_id` is present, the response is `{ "channels": { "<channel>": { "message_serial", "history_serial", "delivery_serial", "version_serial" } } }`. Duplicate `message_id` publishes return the original channel acknowledgement and do not append a second version.
- `X-Idempotency-Key` on `POST /apps/{appId}/events` caches the full acknowledgement response. A retried AI event with the same header returns the same serials and does not rebroadcast.
- `POST /apps/{appId}/batch_events` preserves request order in its `batch` response whenever at least one item requests info, includes `message_id`, or uses an AI event name. Per-event failures abort the request with the existing HTTP error response; already accepted earlier events keep their existing publish semantics.
- Existing message endpoints are the SDK mutation surface: `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}`, `GET /apps/{appId}/channels/{channelName}/messages/{messageSerial}/versions`, and `POST /apps/{appId}/channels/{channelName}/messages/{messageSerial}/{update|delete|append}`. Mutation requests accept optional `op_id`; duplicate `op_id` requests return `status: "duplicate"` with the same `version_serial`, `history_serial`, and `delivery_serial`.
- `GET /apps/{appId}/channels/{channelName}/history` is the signed app HTTP history surface. It supports `limit`, `direction`, `cursor`, serial bounds, and time bounds, returns continuity metadata, and substitutes latest visible versioned-message aggregates.
- `GET /apps/{appId}/channels/{channelName}` includes `ai: { active_streams, last_history_serial, message_count }` when `[ai_transport]` matches the channel. The block is omitted for non-AI channels.

## Conformance Checklist

AIT-S1 [EXISTS-VERIFY] `MessageAction` supports create/update/delete/append/summary.
AIT-S2 [EXISTS-VERIFY] V2 mutable events are `sockudo:message.create|update|delete|append|summary`.
AIT-S3 [EXISTS-VERIFY] `VersionedRealtimeMessage` flattens `PusherMessage` and adds action/serial/version fields.
AIT-S4 [EXISTS-VERIFY] `message_serial` rejects empty, whitespace, and values over 128 bytes.
AIT-S5 [EXISTS-VERIFY] `version_serial` rejects empty, whitespace, and values over 128 bytes.
AIT-S6 [EXISTS-VERIFY] Version chains reject mixed message identity.
AIT-S7 [EXISTS-VERIFY] Version chains reject mixed `history_serial`.
AIT-S8 [EXISTS-VERIFY] Version chains reject duplicate `version_serial`.
AIT-S9 [EXISTS-VERIFY] Version chains reject duplicate `delivery_serial`.
AIT-S10 [EXISTS-VERIFY] Replay requires contiguous `delivery_serial` values.
AIT-S11 [VERIFIED] `get_latest` returns the max version for one message serial.
AIT-S12 [VERIFIED] `latest_by_history` returns one latest version per message in history order.
AIT-S13 [EXISTS-VERIFY] HTTP update response preserves current mutation response fields.
AIT-S14 [EXISTS-VERIFY] HTTP delete response preserves current mutation response fields.
AIT-S15 [EXISTS-VERIFY] HTTP append response preserves current mutation response fields.
AIT-S16 [EXISTS-VERIFY] Mutations with `socket_id` require connected V2 actor.
AIT-S17 [EXISTS-VERIFY] `message_*_own` requires matching verified actor/original client IDs.
AIT-S18 [EXISTS-VERIFY] `message_*_any` permits authorized any-actor mutation.
AIT-S19 [EXISTS-VERIFY] V1 deliveries strip extras, tags, serial, stream_id, and message_id.
AIT-S20 [EXISTS-VERIFY] V2 deliveries clear top-level `idempotency_key`.
AIT-S21 [EXISTS-VERIFY] `extras.headers` rejects nested objects and arrays.
AIT-S22 [EXISTS-VERIFY] HTTP publish body `idempotency_key` wins over header key.
AIT-S23 [EXISTS-VERIFY] Single publish idempotency caches the final response.
AIT-S24 [EXISTS-VERIFY] Batch per-event idempotency skips duplicate event keys.
AIT-S25 [EXISTS-VERIFY] `extras.idempotency_key` dedupes by app/channel/key.
AIT-S26 [EXISTS-VERIFY] `HistoryCursor` is versioned encoded JSON.
AIT-S27 [EXISTS-VERIFY] History rejects zero limits.
AIT-S28 [EXISTS-VERIFY] History rejects inverted serial bounds.
AIT-S29 [EXISTS-VERIFY] History rejects inverted time bounds.
AIT-S30 [EXISTS-VERIFY] History cursor app/channel/direction/bounds must match request.
AIT-S31 [EXISTS-VERIFY] HTTP history response includes continuity metadata.
AIT-S32 [VERIFIED] HTTP history substitutes latest versioned message where available.
AIT-S33 [EXISTS-VERIFY] Rewind is V2-only.
AIT-S34 [EXISTS-VERIFY] Rewind requires enabled history rewind policy.
AIT-S35 [EXISTS-VERIFY] Count rewind delivers oldest-to-newest after newest-first read.
AIT-S36 [EXISTS-VERIFY] Seconds rewind uses time-bounded oldest-first history.
AIT-S37 [EXISTS-VERIFY] Rewind gate drains live messages after historical replay.
AIT-S38 [EXISTS-VERIFY] `sockudo:rewind_complete` reports historical/live counts and truncation flags.
AIT-S39 [EXISTS-VERIFY] Resume accepts `channel_positions`.
AIT-S40 [EXISTS-VERIFY] Resume legacy `channel_serials` is hot-replay only.
AIT-S41 [EXISTS-VERIFY] Hot recovery reports source `hot`.
AIT-S42 [EXISTS-VERIFY] Durable recovery reports source `cold`.
AIT-S43 [EXISTS-VERIFY] Stream ID mismatch fails as `stream_reset`.
AIT-S44 [EXISTS-VERIFY] Retention floor gaps fail as `position_expired`.
AIT-S45 [EXISTS-VERIFY] Annotations support create/delete actions.
AIT-S46 [EXISTS-VERIFY] Annotation summary event is `sockudo_internal:message`.
AIT-S47 [EXISTS-VERIFY] Raw annotation event is `sockudo_internal:annotation`.
AIT-S48 [EXISTS-VERIFY] Annotation summary broadcasts before raw annotation event.
AIT-S49 [EXISTS-VERIFY] Push device registration can issue device identity tokens.
AIT-S50 [EXISTS-VERIFY] Push channel subscriptions are app/channel/device scoped.
AIT-S51 [EXISTS-VERIFY] Push publish supports Channel targets.
AIT-S52 [EXISTS-VERIFY] Realtime `extras.push` enqueues channel push work.
AIT-S53 [NEW] `ai-transport` Cargo feature is propagated through `sockudo-server`.
AIT-S54 [NEW] `[ai_transport].enabled` gates all AI validation and behavior.
AIT-S55 [NEW] AI events are exactly `ai-input`, `ai-output`, `ai-turn-start`, `ai-turn-end`, `ai-cancel`.
AIT-S56 [NEW] `extras.ai.transport` validates known key registry only.
AIT-S57 [NEW] `extras.ai.codec` is opaque but bounded.
AIT-S58 [NEW] Transport/codec tiers reject more than 32 keys.
AIT-S59 [NEW] AI header keys reject values over 64 bytes or outside `[a-z0-9-]+`.
AIT-S60 [NEW] AI header values reject values over 256 bytes.
AIT-S61 [NEW] `status` only accepts `streaming|complete|cancelled`.
AIT-S62 [NEW] `turn-reason` only accepts `complete|cancelled|error|suspended`.
AIT-S63 [NEW] `*-client-id` headers must match verified identity unless app-key trusted.
AIT-S64 [NEW] Clients may publish only `ai-input` and `ai-cancel` without agent trust.
AIT-S65 [VERIFIED] Client-supplied `message_id` dedupes mutable create by app/channel/message ID.
AIT-S66 [VERIFIED] Duplicate client-supplied `message_id` returns original serials without rebroadcast.
AIT-S67 [VERIFIED] Mutation `op_id` dedupes append/update/delete by app/channel/message/action/op.
AIT-S68 [VERIFIED] Mutation acks include optional `history_serial` and `delivery_serial`.
AIT-S69 [VERIFIED] Append rollup first append is immediate.
AIT-S70 [VERIFIED] Append rollup flushes before terminal status.
AIT-S71 [VERIFIED] Append rollup never drops persisted append versions.
AIT-S72 [VERIFIED] Rollup output reduces to the same state as unrolled appends.
AIT-S73 [VERIFIED] `append_rollup_window` accepts only 0, 20, 40, 100, or 500 ms.
AIT-S74 [NEW] Subscription captures attach serial before live delivery.
AIT-S75 [NEW] Client history `until_attach` returns history at or below attach serial.
AIT-S76 [NEW] Live delivery after untilAttach starts above attach serial.
AIT-S77 [NEW] Client history page limit is capped at 1000.
AIT-S78 [NEW] Capability tokens verify HS256 and `kid` app key.
AIT-S79 [NEW] Capability token patterns match exact, namespace wildcard, and `*` only.
AIT-S80 [NEW] Capability token expiry emits 40142 flow.
AIT-S81 [NEW] Revoked `jti` fails closed.
AIT-S82 [NEW] `sockudo:auth` can refresh credentials without reconnect.
AIT-S83 [NEW] `sockudo:presence_update` changes member data without member flap.
AIT-S84 [NEW] Presence timeout defaults off for existing clients.
AIT-S85 [NEW] Enabled presence timeout uses 15 s default grace.
AIT-S86 [NEW] V2 resume cancels pending presence removal.
AIT-S87 [NEW] AI error codes 104000-104010 are emitted with documented names.
AIT-S88 [NEW] Mutable-disabled AI channels emit 93002 and exact message string.
AIT-S89 [NEW] V1 Pusher wire output remains byte-identical under default config.
AIT-S90 [NEW] AI-enabled default-off profile preserves existing SDK tests.
AIT-S91 [VERIFIED] AI append rejects aggregate content over `[ai_transport].max_accumulated_message_bytes` with 40009.
AIT-S92 [VERIFIED] AI append rejects more than `[ai_transport].max_appends_per_message` appends with 40009.
AIT-S93 [VERIFIED] AI create rejects more than `[ai_transport].max_open_streaming_messages_per_channel` open streaming messages with 40009.
AIT-S94 [VERIFIED] Terminal append `extras.ai.transport.status=complete|cancelled` persists in the latest aggregate.
AIT-S95 [VERIFIED] HTTP AI publishes return `{ message_serial, history_serial, delivery_serial, version_serial }` channel acknowledgements.
AIT-S96 [VERIFIED] HTTP `X-Idempotency-Key` retries return the cached AI publish serial acknowledgement.
AIT-S97 [VERIFIED] AI channel state includes `{ active_streams, last_history_serial, message_count }`.
AIT-S98 [VERIFIED] HTTP batch AI acknowledgements are emitted in request order when serial info is requested.
