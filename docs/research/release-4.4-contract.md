# Release 4.4: Annotation Engine And Summary Model - Binding Contract

## Status

Binding pre-implementation contract.

No release 4.4 implementation code should land unless it is consistent with this
document.

## Purpose

Release 4.4 delivers Ably-equivalent message annotation capability on top of
the release 4.3 versioned durable message substrate.

The release depends on these 4.3 contracts already being present:

- durable top-level message identity via stable message serials
- versioned message actions for `message.create`, `message.update`,
  `message.delete`, and `message.append`
- deterministic winner selection under racing message mutations
- own-versus-any capability enforcement based on identified client identity
- recovery and replay paths that can preserve mutation continuity

Release 4.4 adds annotation events, summary projections, and summary delivery.
It does not change the release 4.3 message mutation model.

## Research Binding

This contract is grounded in:

- `docs/research/ably-parity/ably-message-annotations-research.md`
- `docs/research/ably-parity/sockudo-ably-parity-release-plan.md`
- `docs/research/ably-parity/prompts-ultra/release-4.4.md`

## Goals

Release 4.4 must deliver all of the following:

- Ably-equivalent annotation capability layered on the 4.3 versioned durable
  message substrate.
- All five Ably summarization methods:
  - `total.v1`
  - `flag.v1`
  - `distinct.v1`
  - `unique.v1`
  - `multiple.v1`
- Summary changes delivered as `message.summary` events attached to the
  original message's durable history position.
- Summary payloads exposed under `message.annotations.summary`.
- Individual raw annotation event streams through an opt-in subscription mode.
- Annotation persistence that is independent of message payload storage.
- Summary projections that are treated as rebuildable derived state.
- V2-native behavior only, preserving the Pusher-compatible V1 boundary.

## Non-Goals

Release 4.4 does not include:

- UI work.
- Client SDK implementation work.
- Ably SDK or REST API compatibility as a surface-level goal.
- Annotation support on V1 channels.
- Physical deletion of annotation history.
- Redaction workflows for annotation payloads.
- Push notification, AI Transport, or transcript branching work.

Protocol and server runtime are the deliverables for this release.

## V1 And V2 Boundary

Annotations are Sockudo V2-native only.

Binding rules:

- V1 clients must never receive `message.summary`, `annotation.create`, or
  `annotation.delete` events.
- V1 channels must reject or ignore annotation-specific publish, delete,
  subscribe, history, and replay behavior.
- Annotation fields must not leak into Pusher-compatible V1 message envelopes.
- V2 clients may receive summary events through ordinary channel subscription.
- V2 clients may receive raw annotation events only when the raw annotation
  subscription mode is explicitly enabled and authorized.

Any implementation that requires changing V1 wire behavior is out of scope for
release 4.4.

## Annotation Type System

An annotation type has this format:

```text
namespace:summarizer.version
```

Example:

```text
reactions:distinct.v1
```

Binding rules:

- `namespace` scopes aggregation.
- Different namespaces never merge into the same summary bucket.
- `summarizer` selects the rollup method.
- `version` is a forward-compatibility indicator.
- Release 4.4 supports only `v1` summarizers.
- Unknown summarizers or unsupported versions must be rejected before storage.

The supported type suffixes are:

- `total.v1`
- `flag.v1`
- `distinct.v1`
- `unique.v1`
- `multiple.v1`

## Summary Semantics By Summarizer

### `total.v1`

Counts annotations of a type on a message.

Binding rules:

- Unidentified clients may contribute.
- The same client publishing multiple times increments the total multiple times.
- Delete decrements the total for the deleted contribution.
- Summary shape is `{ total }`.
- No client identifiers are included in the summary.

### `flag.v1`

Counts distinct identified clients for a type.

Binding rules:

- Identified `clientId` is required to publish.
- One client contributes at most once per type.
- Duplicate creates by the same client are idempotent for summary state.
- Delete removes that client's contribution.
- Summary shape is `{ total, clientIds, clipped }`.

### `distinct.v1`

Counts distinct identified clients per annotation `name`.

Binding rules:

- Identified `clientId` is required to publish.
- `name` is required.
- One client contributes at most once per name.
- The same client may contribute to multiple names under the same type.
- Delete removes the client from the named bucket.
- Summary shape is `{ name -> { total, clientIds, clipped } }`.

### `unique.v1`

Counts distinct identified clients per annotation `name`, with one active name
per client for the type.

Binding rules:

- Identified `clientId` is required to publish.
- `name` is required.
- Publishing a different name moves the client from the previous bucket to the
  new bucket.
- Delete removes the client from their active bucket.
- Summary shape is `{ name -> { total, clientIds, clipped } }`.

### `multiple.v1`

Tracks totals and per-client counts for named annotations.

Binding rules:

- `name` is required.
- `count` is optional and defaults to 1.
- Identified clients contribute to per-client counts.
- Unidentified contributions are allowed only as anonymous count
  contributions and must be tracked separately as `totalUnidentified`.
- Delete by an identified client removes that client's contributions for the
  named bucket.
- Summary shape is
  `{ name -> { total, clientCounts, totalUnidentified, clipped, totalClientIds } }`.

The release blocker for `multiple.v1` identity enforcement means ownership and
delete paths must require identified clients when operating on client-owned
contributions. It does not prohibit anonymous create contributions, which are
represented by `totalUnidentified`.

## Annotation Event Model

Annotation events are canonical. Summary projections are not canonical.

An annotation event contains:

- `id`: unique annotation identifier
- `action`: `annotation.create` or `annotation.delete`
- `serial`: annotation event serial used for ordering and deduplication
- `messageSerial`: target message top-level serial from release 4.3
- `type`: annotation type in `namespace:summarizer.version` form
- `name`: optional annotation variant name
- `clientId`: publishing or deleting client identity, when identified
- `count`: optional integer used by `multiple.v1`
- `data`: raw annotation payload
- `encoding`: optional raw payload encoding
- `timestamp`: event timestamp

Binding rules:

- Annotation `data` is never included in rolled-up summary payloads.
- Annotation deletes create `annotation.delete` events; they do not erase the
  historical `annotation.create` event.
- Annotation serials must be ordered and deduplicated using the same durability
  expectations as other V2 mutation serials.
- Publishing an annotation against a missing target `messageSerial` must fail.

## Persistence Contract

Annotation events must be stored independently of message payloads.

Binding rules:

- The annotation event log is the canonical source of truth.
- Summary projections are derived state rebuilt from the annotation event log.
- Losing or invalidating the projection cache must not lose annotation data.
- Annotation event retention follows the retention boundary of the parent
  message's durable history.
- When a parent message is evicted from durable history, its annotation events
  may also be evicted.
- Enabling annotations on a channel forces message persistence, matching the
  behavior required by the 4.3 versioned message substrate.

Implementation may cache materialized projections for speed, but correctness
must come from replaying canonical annotation events in annotation-serial order.

## Summary Delivery Model

Standard channel subscription continues to deliver ordinary messages.

When an annotation changes the effective summary, Sockudo delivers a V2 event
with:

```text
action = message.summary
```

Binding rules:

- The summary event is attached to the original message's stable
  `messageSerial`.
- The summary event must preserve the original message's durable history
  position semantics; it must not create a second ordinary message in the
  latest-history view.
- Live delivery may use a new delivery-continuity serial so reconnect and replay
  can observe the summary change.
- The summary payload is nested at `message.annotations.summary`.
- Summary keys are the full annotation type string, including namespace,
  summarizer, and version.
- The summary event must contain only the updated summary projection required
  for the target message and type, unless a broader snapshot is explicitly
  documented and tested.
- Standard subscribers do not need raw annotation mode to receive
  `message.summary`.

Expected shape:

```json
{
  "action": "message.summary",
  "serial": "<target_message_serial>",
  "annotations": {
    "summary": {
      "reactions:distinct.v1": {
        "thumbsup": {
          "total": 5,
          "clientIds": ["a", "b", "c", "d", "e"],
          "clipped": false
        }
      }
    }
  }
}
```

## Clipping Contract

Summary clipping is required when contributing client identifiers would exceed
the configured maximum message size.

Binding rules:

- `total` remains exact.
- `clipped` is set to `true`.
- `clientIds` becomes a deterministic partial list.
- For `multiple.v1`, `totalClientIds` remains the exact count of distinct
  identified contributing clients.
- Clipping must be deterministic for the same projection state and configured
  limit.
- Boundary counts must be tested exactly at, below, and above the clipping
  threshold.

Implementations should choose a stable ordering for clipped identifier lists,
such as lexicographic order or annotation-serial contribution order, and then
document and test that choice.

## Raw Annotation Subscription Model

Raw annotation streams are opt-in.

Clients that opt in receive individual:

- `annotation.create`
- `annotation.delete`

events through a separate subscription mode:

```text
ANNOTATION_SUBSCRIBE
```

Binding rules:

- Raw annotation mode must not suppress ordinary message delivery.
- Raw annotation mode must not suppress `message.summary` delivery.
- Authorization for raw annotation delivery is separate from ordinary summary
  delivery.
- Callers that explicitly set subscription `modes` must include every mode they
  need.
- Documentation must prominently warn that setting modes replaces defaults, so
  adding only `ANNOTATION_SUBSCRIBE` may omit ordinary subscription modes.

The safe example shape is:

```json
{
  "modes": ["SUBSCRIBE", "ANNOTATION_SUBSCRIBE"]
}
```

## Capability And Identity Contract

Release 4.4 requires annotation-specific capability checks.

Required capability concepts:

- `annotation-publish`: publish annotation creates.
- `annotation-delete-own`: delete annotations owned by the same identified
  `clientId`.
- `annotation-delete-any`: delete any annotation on the channel.
- `annotation-subscribe`: receive raw annotation event streams.

Binding rules:

- Summary delivery through ordinary subscribe does not require
  `annotation-subscribe`.
- Raw annotation events require `annotation-subscribe`.
- `flag.v1`, `distinct.v1`, and `unique.v1` publish paths require identified
  clients.
- `multiple.v1` publish paths must distinguish identified per-client
  contributions from unidentified `totalUnidentified` contributions.
- `annotation-delete-own` requires an identified deleting client and a stored
  annotation owner match.
- `annotation-delete-any` requires authorization regardless of ownership.
- Missing target annotations on delete must return a not-found result, not an
  internal error.

Failing open on annotation delete ownership is a release-blocking security bug.

## Cluster And Convergence Contract

Summary state must converge under concurrent annotation churn on one node and
across clustered nodes.

Binding rules:

- Annotation events for a given `(channel, messageSerial, type)` are applied in
  deterministic annotation-serial order when rebuilding projections.
- Projection updates must not depend on node-local arrival order.
- If two nodes see the same canonical annotation event log, they must compute
  identical summary projections.
- Cached projections must carry enough ordering metadata, such as
  `last_annotation_serial`, to detect staleness.
- Concurrent projection writes must use a convergence-safe strategy such as
  optimistic concurrency with retry or full event-log rebuild from the
  authoritative store.
- Summary updates must fan out across the existing cluster delivery path so
  subscribers on all nodes receive equivalent `message.summary` events.

The canonical event log, not an individual node's projection cache, is the
authority for correctness.

## Recovery And Replay Contract

Summary delivery must survive channel reconnect and replay correctly.

Binding rules:

- Live summary changes must participate in V2 delivery continuity so a
  reconnecting client does not silently miss a summary change.
- Reattach paths may deliver a fresh summary snapshot instead of replaying every
  intermediate summary event, provided the resulting client-visible state is
  convergent and documented.
- Raw annotation replay is available only to clients authorized for
  `ANNOTATION_SUBSCRIBE`.
- Raw annotation replay must use annotation event serial ordering and dedupe.
- Ordinary latest-history reads must expose the target message with the current
  summary attached under `message.annotations.summary` when summaries are
  requested by the relevant V2 surface.

Subscribers are not guaranteed to see every intermediate summary snapshot. They
are guaranteed that summary state converges to the canonical projection.

## Documentation Requirements

Release 4.4 implementation must update user-facing and operator documentation
for:

- annotation type format
- all five summarizers and their summary shapes
- V2-only boundary
- annotation enablement and forced persistence
- raw annotation subscription mode
- the explicit modes replacement hazard
- clipping behavior and exact-total guarantees
- capability and identity requirements
- retention behavior
- reconnect and replay behavior
- metrics and operational guidance for clipped summaries and projection rebuilds

## Release Blockers

The release cannot ship until all of the following are implemented and tested:

- Summary state is convergent under concurrent annotation churn on a single
  node.
- Summary state is convergent under concurrent annotation churn across cluster
  nodes.
- Clipping is deterministic.
- Clipping is tested at boundary counts.
- Identified-client requirements are enforced for `flag.v1`, `distinct.v1`,
  `unique.v1`, and ownership-sensitive `multiple.v1` paths.
- `multiple.v1` anonymous contributions are represented only through
  `totalUnidentified`.
- Summary delivery survives channel reconnect.
- Summary replay or snapshot refresh after reconnect is coherent.
- Raw annotation events are delivered only to clients with explicit raw
  annotation subscription mode and authorization.
- Enabling annotations forces message persistence.
- Annotation events are stored independently from message payloads.
- Summary projections can be rebuilt from the annotation event log.
- V1 clients remain unaffected.

## Acceptance Checklist

Before release 4.4 is considered complete:

- All five summarizers have isolated unit tests.
- Annotation publish and delete paths have integration tests.
- Capability enforcement has positive and negative tests.
- Identified-client rejection is tested for identity-required summarizers.
- Raw annotation subscription mode is tested separately from ordinary summary
  delivery.
- Reconnect behavior is tested for summaries and raw annotation replay.
- Cluster convergence is tested with concurrent publishes and deletes.
- Clipping uses deterministic partial contributor lists and has boundary tests.
- Docs describe the `ANNOTATION_SUBSCRIBE` mode hazard clearly.
- Docs state that annotations are V2-native only.
- Docs state that annotations force message persistence.
