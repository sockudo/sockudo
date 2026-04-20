# Release 4.3: Versioned Durable Messages - Binding Contract

## Status

Binding pre-implementation contract.

No release 4.3 code should land unless it is consistent with this document.

## Purpose

Release 4.3 establishes the durable mutation substrate that later releases depend on:

- 4.4 annotation summaries and raw annotation streams
- 4.6 AI Transport durable turns and token streaming
- 4.7 edit, regenerate, and transcript-branch behavior

This release targets Ably-like behavior for mutable durable messages without copying Ably APIs or SDK names.

## Repo Anchors

This contract is grounded in the current Sockudo architecture:

- `crates/sockudo-core/src/history.rs` owns durable history positions, cursors, retention, and stream continuity metadata.
- `crates/sockudo-adapter/src/handler/connection_management.rs` owns V2 publish-time serialization, durable-history append, replay-buffer insert, `message_id`, `stream_id`, and V2 `serial`.
- `crates/sockudo-adapter/src/handler/recovery.rs` owns hot and cold connection recovery behavior.
- `crates/sockudo-protocol/src/messages.rs` owns the public V1/V2 wire envelope via `PusherMessage`.
- `crates/sockudo-adapter/src/horizontal_adapter.rs` owns the cross-node broadcast envelope.

This matters because release 4.3 changes cannot pretend history, recovery, and wire delivery are independent. They already share serial and stream continuity assumptions today.

## Goals

Release 4.3 must deliver all of the following:

- permanent logical message identity for V2 mutable messages
- version identity distinct from logical message identity
- durable version history for create, update, delete, and append
- latest-visible-version reads at the original history position
- Sockudo-native mutation APIs for update, delete, and append
- deterministic winner selection under concurrent mutations
- authorization for own-versus-any mutations tied to identified client identity
- clustered convergence that does not rely on arrival order
- a recovery model that remains correct when updates do not create new history positions

## Non-Goals

Release 4.3 does not include:

- Ably API compatibility
- annotations, rollups, or raw annotation streams
- push platform work
- AI session or turn semantics
- physical deletion, redaction, or GDPR erasure workflows
- transcript branching or edit-regenerate UX
- cross-region or cross-backend replication beyond Sockudo's existing clustered transport assumptions

## Current Architectural Constraint

Sockudo already uses one per-channel `serial` concept for durable history and cold recovery.

That is not sufficient by itself for mutable messages if only creates occupy durable history positions.

Why:

- history wants one stable position per logical message
- recovery wants every missed live mutation to be replayable after a disconnect
- if an update/delete/append does not consume a new history position, a recovery query such as "give me messages after serial N" can miss post-create mutations entirely

This is a release blocker unless 4.3 explicitly separates history position from delivery continuity or proves an equivalent mechanism.

## Supported Message Actions In 4.3

Release 4.3 supports these actions:

- `message.create`
- `message.update`
- `message.delete`
- `message.append`

Release 4.3 reserves but does not support:

- `message.summary`

`message.summary` is part of the broader action family but has no publish path, no operator surface, and no acceptance criteria in 4.3.

## V1 And V2 Boundary

| Surface | V1 | V2 |
| --- | --- | --- |
| Mutable message actions | Not available | Supported |
| Version metadata | Not present | Present for versioned messages |
| Permanent mutable-message identity | Not present | Present |
| History latest-visible substitution | Not observable through V1 protocol | Required |
| Realtime update/delete/append delivery | Never delivered | Delivered |
| Mutation APIs | Rejected or unavailable | Supported |

Binding rule:

- V1 clients remain strictly Pusher-compatible.
- V1 sockets must never receive mutable-message action fields, version metadata, or mutation-only envelopes.
- Any feature that cannot preserve V1 isolation is out of scope for 4.3.

## Identity And Ordering Model

Release 4.3 uses four distinct identifiers or orderings. They must not be collapsed into one field.

### 1. `message_serial`

Permanent logical message identity.

- assigned exactly once at create time
- never changes across updates, deletes, or appends
- used to address update, delete, append, latest-message lookup, and version-history lookup
- must be stable across nodes

### 2. `version_serial`

Identity of one specific version of a message.

- assigned for every create, update, delete, and append
- totally ordered for winner selection
- the winning visible version is the version with the greatest valid `version_serial`

### 3. `history_serial`

Immutable position in the channel history view.

- represents where the original logical message lives in durable history ordering
- assigned when the logical message is created
- does not move when later versions arrive
- latest-visible history reads substitute the winning version into this fixed position

### 4. `delivery_serial`

Continuity ordering for live delivery and recovery.

- advances for every live mutation visible to V2 consumers
- must support replay of create, update, delete, and append after disconnect
- must be distinct in meaning from `history_serial` even if some implementation chooses to share storage

### Existing `message_id`

The existing UUID-like `message_id` remains a deduplication token, not the permanent identity of a mutable message.

## Latest-Visible-Version Model

The latest-visible-version is the single winning version for a logical message after applying deterministic winner selection over all accepted versions for that `message_serial`.

It is the source of truth for:

- `GET history` latest view
- `getMessage(message_serial)` or equivalent
- rewind from durable history
- any materialized "current message" read path

It is not, by itself, the complete source of truth for recovery, because recovery also needs mutation continuity after disconnect.

### Winner Selection

Winner selection must be deterministic and data-only:

- compare versions for the same `message_serial`
- select the greatest `version_serial`
- do not depend on node-local arrival order
- do not depend on transport timing
- do not overwrite or drop losing versions from durable version history

If two nodes see the same set of versions, they must compute the same winner.

## History Position Semantics

History is a latest-visible projection, not the raw mutation log.

Binding rules:

- a logical message appears once in channel history
- its position is determined by the original create-time `history_serial`
- later update/delete/append operations do not create additional history rows for the latest-view API
- history payload, action, and version metadata come from the winning latest-visible-version
- deleted messages remain visible as soft-deleted rows unless later product work explicitly changes that policy

This is the rule the release prompt called out explicitly: latest-visible-version history must be explained before coding begins.

## Version History Semantics

Version-history reads are separate from latest-view history.

Binding rules:

- all accepted versions for a `message_serial` remain queryable
- create, update, delete, and append versions are preserved
- version-history reads expose ordering by `version_serial`
- a losing concurrent version remains queryable even when it is not the latest-visible winner

## Mutation Semantics

### `message.create`

- creates the logical message
- assigns `message_serial`
- assigns the initial `version_serial`
- assigns the fixed `history_serial`
- emits a live delivery entry with a new `delivery_serial`

### `message.update`

- targets a prior `message_serial`
- creates a new version
- uses shallow-mixin semantics over the current winning version
- does not move the original history position
- emits a live delivery entry with a new `delivery_serial`

### `message.delete`

- targets a prior `message_serial`
- is a soft delete, not physical removal
- creates a new version with action `message.delete`
- uses the same shallow-mixin semantics as update
- does not move the original history position
- emits a live delivery entry with a new `delivery_serial`

### `message.append`

- targets a prior `message_serial`
- derives a new full visible payload by concatenating onto the current winning payload
- stores the full rolled-up state in the new version
- does not move the original history position
- emits a live delivery entry with a new `delivery_serial`

## Shallow-Mixin Rules

For update and delete:

- `data`, `name`, and `extras` are replaceable fields
- omitted fields inherit from the current winning version
- explicit empty values stay explicit; omission is not the same as clearing

This preserves Ably-like mutation behavior while keeping Sockudo-native wire and API names.

## Append Rules

Release 4.3 binds the append contract as follows:

- durable version storage keeps the full rolled-up payload
- latest-view history returns the full rolled-up payload
- latest-message lookup returns the full rolled-up payload
- realtime delivery may later optimize to incremental append payloads, but correctness cannot depend on that optimization

The minimum acceptable 4.3 behavior is:

- storage and latest-view reads are rolled up
- realtime may deliver full replacement payloads only

Incremental append delivery is allowed later, but it is not required to declare 4.3 complete.

## Recovery And Replay Contract

This is the highest-risk part of the release.

Binding rules:

- connection recovery must remain continuity-correct for mutable messages
- a client that disconnects after seeing version A and reconnects later must be able to converge to the correct latest state for all missed mutations
- cold recovery cannot rely only on the latest-view history scan by `history_serial` if update/delete/append do not create new history positions

Therefore 4.3 must do one of the following:

1. Persist a mutation-capable delivery log keyed by `delivery_serial` for create, update, delete, and append.
2. Extend the existing durable history substrate so cold recovery can replay missed mutations even when latest-view history remains one-row-per-message.
3. Prove an equivalent recovery mechanism that preserves continuity and deterministic convergence.

Until one of those is true, release 4.3 is blocked.

## Authorization Contract

Release 4.3 introduces these capability classes:

- `message-update-own`
- `message-update-any`
- `message-delete-own`
- `message-delete-any`
- `message-append-own`
- `message-append-any`

Binding rules:

- own-scope authorization is identity-based, not connection-instance-based
- own-scope checks compare the authenticated mutation actor with the original create identity
- unidentified actors cannot use own-scoped mutation permissions safely
- cluster paths, retry paths, and recovery paths must not bypass authorization decisions

## Storage Contract

Release 4.3 requires durable support for:

- logical message identity lookup by `message_serial`
- latest-visible winner lookup
- full version-history lookup by `message_serial`
- fixed history position for latest-view reads
- durable continuity for recovery of missed live mutations

It is acceptable for this to be implemented as one store or multiple cooperating stores.

It is not acceptable to fake latest-visible history by overwriting old durable rows in place if that loses version history or breaks deterministic recovery.

## Cluster Convergence Contract

Release 4.3 must converge correctly under:

- concurrent update/update
- concurrent update/delete
- append racing with update or delete
- cross-node arrival reordering
- reconnect after partial delivery

Binding rules:

- convergence is based on durable version ordering, not transport arrival order
- all nodes must compute the same latest-visible winner from the same version set
- losing versions remain queryable for inspection and audit

## Acceptance Criteria

Release 4.3 is not complete unless all of the following are true:

1. A logical message has a stable `message_serial` and at least one distinct `version_serial`.
2. `message.update`, `message.delete`, and `message.append` each create durable new versions without moving the original history position.
3. Latest-view history returns one row per logical message at the original `history_serial`, with payload and action taken from the winning version.
4. Version-history queries return all preserved versions for a message in deterministic `version_serial` order.
5. Concurrent mutations on different nodes converge to the same latest-visible winner.
6. Append storage and latest-view reads always return full rolled-up payloads.
7. V1 sockets never receive mutation-only fields or events.
8. Own-versus-any authorization succeeds and fails exactly where expected.
9. Recovery after missed mutations remains continuity-correct for hot and cold paths.
10. The implementation is explicit about the difference between latest-view history and delivery/recovery continuity.

## Release Blockers

Any one of these is a no-go for 4.3:

- The contract between `history_serial` and `delivery_serial` is still ambiguous.
- Cold recovery still depends only on the current create-position history scan and cannot replay post-create mutations.
- Latest-visible winner selection is not deterministic across nodes.
- Version-history storage can lose losing concurrent versions.
- V1 delivery leaks mutation metadata or action semantics.
- Append can produce a visible state that differs between latest-view reads and replay/recovery convergence.
- Authorization is enforced only at one node or only on the initial request path.

## Deliberate Deferrals

These are explicitly deferred and must not be smuggled into 4.3 completion claims:

- `message.summary` publish and delivery semantics
- raw annotation event streams
- per-socket incremental append optimization
- physical deletion and purge APIs for version history
- client SDK ergonomics beyond the minimum documentation needed to interpret the new behavior

## Release Gate

Implementation for release 4.3 may proceed only after downstream work adopts this contract, especially:

- latest-visible history is a fixed-position projection
- version history preserves every accepted mutation
- recovery continuity is treated as a first-class design problem, not a side effect of current history reads
