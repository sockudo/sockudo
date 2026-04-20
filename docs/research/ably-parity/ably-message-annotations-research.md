# Ably Message Annotations, Updates, Deletes, and Appends

## Scope

This research covers the Ably Pub/Sub features documented in:

- `https://ably.com/docs/messages/annotations`
- `https://ably.com/docs/messages/updates-deletes`

It includes:

- message annotations
- message updates
- message deletes
- message appends
- message versioning
- message actions
- history behavior
- conflation behavior
- capability and identity requirements
- edge cases that matter for a Sockudo implementation

## Product status and enablement

- Ably documents both message annotations and message updates/deletes as `public-preview`.
- These features are enabled per channel or channel namespace by a single rule: `Message annotations, updates, deletes, and appends`.
- Once enabled, Ably persists messages regardless of whether persistence was otherwise enabled on the channel. This is a critical product behavior because the feature depends on message retention and version history.
- Ably explicitly warns that enabling this rule can increase billed persistence usage.

## Core object model

### Permanent message identity

- Every original message has a stable top-level `serial`.
- That `serial` remains the permanent identifier for the message across updates, deletes, summaries, and appends.
- Ably also emits `version.serial`, which identifies the specific version of the message.
- Both `serial` and `version.serial` are lexicographically sortable strings.

### Message actions

Ably documents the following actions relevant to this feature family:

- `message.create`: original message
- `message.update`: updated message version
- `message.delete`: soft-deleted message version
- `message.append`: incremental append for a message
- `message.summary`: annotation summary snapshot for a message
- `annotation.create`: individual annotation event
- `annotation.delete`: individual annotation delete event
- `meta`: non-user platform events

### Immutable delivery model

- Ably does not mutate a previously delivered `Message` object in place.
- Updates and deletes are published as new channel messages with the same top-level `serial` as the original.
- Subscribers must interpret actions and version metadata; they do not receive in-place object mutation.

## Message updates

### Publish path

- Ably exposes `updateMessage()` on REST and Realtime channels.
- The update targets a message by `serial`.
- The caller can obtain the target serial from:
  - the return value of `publish()`
  - a subscribed message
  - a history query

### Update semantics

- An update publishes a new message with action `message.update`.
- History returns the latest version of the message, but in the original message's history position.
- Ably describes update semantics as a shallow mixin:
  - `data`, `name`, and `extras` may be replaced
  - any omitted field remains unchanged from the current version
- This is behavioral parity that Sockudo should preserve even if it uses different APIs.

### Return value

- `updateMessage()` returns an `UpdateDeleteResult` containing `versionSerial`.
- `versionSerial` may be `null` if the update was superseded by a later update before publication.

### Authorization model

Ably uses separate update capabilities:

- `message-update-own`
- `message-update-any`

For `message-update-own`, Ably ties "own" to matching identified `clientId` values between the original publisher and the updater. That means:

- ownership is identity-based, not connection-based
- unidentified clients cannot safely participate in "own" semantics

### Conflation behavior

- Ably may discard out-of-date intermediate updates.
- This can happen during server-side batching or inside rewind backlogs.
- Subscribers are not guaranteed to see every intermediate update.
- Ably guarantees the last update the subscriber receives represents the latest version that history or `getMessage()` will eventually return.

This is a critical parity point: the transport is convergent, not a guaranteed stream of every mutation.

### Update metadata

Ably allows operation metadata on updates:

- `clientId`
- `description`
- `metadata`

This metadata is stored in the `version` object, not in the top-level original message fields.

## Message deletes

### Publish path

- Ably exposes `deleteMessage()` on REST and Realtime channels.
- Deletes are also keyed by the original message `serial`.

### Delete semantics

- Deletes are soft deletes, not physical erasure.
- A delete is structurally just another version, but with action `message.delete`.
- History returns the deleted version as the latest version if that is the winning state.
- Full version history remains available through the versions API.

### Delete mixin semantics

- Delete uses the same shallow mixin rules as update.
- If the deleting publisher wants `data` cleared in the latest visible version, it must explicitly set `data` to an empty value in the delete payload.
- Even if the latest visible version clears data, prior versions are still available through version history.

This is an important behavioral nuance:

- delete in Ably is not equivalent to redaction
- delete in Ably is a semantic state transition layered over retained history

### Authorization model

Ably uses separate delete capabilities:

- `message-delete-own`
- `message-delete-any`

As with update, "own" relies on matching identified `clientId` values.

### Conflation behavior

- Deletes participate in the same mutation conflation model as updates.
- Intermediate update/delete churn may collapse before subscriber delivery.
- The winner is whatever version has the lexicographically highest `version.serial`.

### Delete metadata

Deletes also accept:

- `clientId`
- `description`
- `metadata`

These are stored in the message `version` block.

## Message appends

## Publish path

- Ably exposes `appendMessage()` on REST and Realtime channels.
- The append targets an existing message by original `serial`.

### Why append exists

Append is optimized for gradual message construction and token streaming.

Ably differentiates it from update:

- `updateMessage()` replaces `data` if supplied
- `appendMessage()` concatenates to the end of the most recent `data`

### Full-state versus incremental-state behavior

When Ably receives an append:

- it calculates the new full message payload by concatenating the append onto the current latest version
- the fully aggregated version is what history and rewind expose
- the incremental append is what realtime subscribers may receive

This split is central:

- history and rewind expose full messages
- realtime delivery may expose incremental appends for efficiency

### Subscriber behavior

Ably documents several important behaviors:

- The first append a subscriber sees for a message attachment is delivered as a full `message.update`, not as a raw incremental append, so the subscriber does not need to consult history to reconstruct state.
- Subsequent operations may arrive as `message.append`.
- Ably may also deliver an append at any time as a full `message.update` carrying the rolled-up payload.

Therefore the client rule is:

- if action is `message.append`, concatenate
- if action is `message.update`, replace local content with the full rolled-up payload

Sockudo should preserve this exact consumer contract if it wants parity in behavior and resilience.

### Version history behavior

- Ably states that update versions are stored in version history.
- Appends are not stored as separate historical versions in the same way; they are intended for high-frequency incremental publishing from one publisher.
- History and rewind return fully aggregated messages, not append fragments.

### Ordering behavior

- Appends can be pipelined without awaiting previous append completion.
- Ably constructs the full payload optimistically in the pipeline.
- Ably explicitly warns that a rejected append can still have been optimistically incorporated into a subsequent append in that pipeline.
- Realtime clients are preferred for high-rate append streams because Ably can guarantee publish order there.
- REST often preserves order in practice but does not guarantee it.

### Authorization model

Ably reuses update capabilities for append:

- `message-update-own`
- `message-update-any`

### Conflation behavior

Ably may conflate append traffic:

- multiple appends to the same message may be concatenated into one larger append for subscribers
- the conflated append carries the operation metadata from the most recent append
- all appends being conflated must share a compatible data type

Ably also specifies conflict resolution across append and update/delete:

- if update/delete occurs, it supersedes prior operations in the conflation window
- if appends follow an update/delete inside that same window, subscribers receive a full message including those appends
- if an update/delete arrives after pending appends, the later update/delete wins and discards those pending appends

Net effect:

- realtime mutation delivery is convergent and final-state oriented
- history/getMessage remain the source of truth for the winning state

## Get latest version and message version history

### Latest version lookup

- `getMessage()` on a REST channel retrieves the most recent version of a message by `serial` or by message object.
- This requires history capability.

### Version history lookup

- `getMessageVersions()` returns all historical versions of a message as a paginated result.
- It includes the original message and subsequent updates or deletes.
- Versions are ordered by version ordering.

### Version ordering

- To determine which version is newer, Ably says to compare `version.serial`.
- If two near-simultaneous mutations race, both may be published, but the lexicographically greatest `version.serial` wins as the version eventually returned by history.

This winning-version rule is a major implementation requirement for Sockudo if it wants deterministic convergence across clustered nodes.

## Version structure

Ably's versioned message structure contains:

- top-level `serial`: permanent message identity
- top-level original `clientId`
- top-level original `timestamp`
- top-level message payload fields
- `action`: current visible state
- nested `version`:
  - `serial`: current version identity
  - `clientId`: actor who made the latest change
  - `timestamp`: version timestamp
  - `description`
  - `metadata`

This structure separates:

- identity of the conversation artifact
- current visible state
- author and metadata of the latest mutation

## Message annotations

## High-level model

- Annotations are separate events attached to an existing message.
- They are published or deleted against a message `serial` or message object through `channel.annotations.publish()` and `channel.annotations.delete()`.
- Ably aggregates annotations into summaries automatically.
- Annotation payloads can exist without appearing in ordinary history as first-class chat messages.

### Enablement and cost

- Annotations share the same enabling rule as updates/deletes/appends.
- Enabling annotations also forces message persistence.

## Annotation type system

An annotation `type` has the format:

- `namespace:summarization.version`

Ably documents five summarization methods:

- `total.v1`
- `flag.v1`
- `distinct.v1`
- `unique.v1`
- `multiple.v1`

The namespace scopes aggregation. Different namespaces do not merge.

## Summarization methods

### `total.v1`

- Counts the number of annotations of a given type on a message.
- Does not attribute counts to individual clients in the summary.
- Unidentified clients may publish these annotations.
- The same client publishing twice increments the count twice.
- Delete decrements the count.

Use cases:

- generic counters
- lightweight metrics
- non-attributed reactions

### `flag.v1`

- Counts how many distinct identified clients published a given annotation type.
- Summary includes:
  - `total`
  - `clientIds`
  - `clipped`
- Identified clients are required.
- A client contributes at most once per annotation type.
- Delete removes the client contribution and decrements total.

Use cases:

- delivered/read flags
- moderation acknowledgements
- one-per-user reactions without named variants

### `distinct.v1`

- Counts unique identified clients for each annotation `name` under a type.
- A client may contribute once per name.
- The same client may contribute to multiple different names.
- Delete removes the client from that named bucket and decrements that bucket total.

Use cases:

- named categories
- multi-reaction systems where one user can add multiple distinct reaction names

### `unique.v1`

- Counts unique identified clients for each annotation `name`, but each client may contribute to only one name at a time within that type.
- Publishing a different name moves the client from the previous name bucket to the new one.
- Delete removes the client from the active name bucket.

Use cases:

- exclusive status markers
- one-vote-per-user polls where the vote can change

### `multiple.v1`

- Tracks total and per-client counts for a named annotation.
- Same client can contribute multiple times to the same name.
- Supports optional `count`; default increment is 1.
- Unidentified contributions are tracked via `totalUnidentified`.
- Delete removes all contributions by that `clientId` for that name.
- Summary includes:
  - `total`
  - `clientCounts`
  - `totalUnidentified`
  - `clipped`
  - `totalClientIds`

Use cases:

- ratings
- multi-vote counters
- score accumulation

## Annotation publishing semantics

- `annotations.publish()` creates an annotation event with action `annotation.create`.
- Publish target may be a message object or a message `serial`.
- Some summarization types require a `name`.
- `multiple.v1` additionally expects `count`.
- An annotation may include `data`.
- Annotation `data` is not included in rolled-up summaries.

This split matters:

- summaries are the common consumption path
- individual annotation subscriptions are required if the app cares about raw per-annotation payloads

## Annotation deletion semantics

- `annotations.delete()` publishes action `annotation.delete`.
- Deleting an annotation does not remove the historical annotation event object.
- It mutates the effective summary by removing that contribution.
- The delete carries the deleting actor's `clientId` if the client is identified.

## Summary delivery model

- Standard message subscription on the channel continues to work.
- Ordinary messages still arrive through `subscribe()`.
- When summaries change, Ably delivers a message with action `message.summary`.
- The summary is nested under `message.annotations.summary`.

### Large summary clipping

When the contributing client list would exceed max message size:

- `clipped` is set to `true`
- `total` remains correct
- `clientIds` becomes partial
- for `multiple.v1`, `totalClientIds` becomes the correct count of distinct contributing identified clients

### Individual annotation subscription

- Raw annotation events are available through `annotations.subscribe()`.
- To receive them, the channel must be opened with `ANNOTATION_SUBSCRIBE` mode.
- Ably warns that explicitly setting `modes` overrides defaults, so callers must add all other required modes explicitly.

This is a subtle but important SDK behavior that a Sockudo client API should not make easy to misuse.

## Annotation event properties

Ably documents the following annotation message properties:

- `id`
- `action`
- `serial`
- `messageSerial`
- `type`
- `name`
- `clientId`
- `count`
- `data`
- `encoding`
- `timestamp`

## Capability and identity implications

- Annotation summarization methods that track client identities rely on identified clients.
- Update/delete/append ownership semantics also depend on identified clients.
- A Sockudo parity design should treat authenticated user identity as foundational, not optional plumbing.

## Operational and architectural implications for Sockudo

To reproduce Ably behavior, Sockudo will need:

- durable per-message identity independent of current version
- durable per-version identity and ordering
- clustered winner selection based on version order
- history that preserves original history position while substituting latest visible version
- mutation conflation rules that converge to the same final state subscribers and history see
- a separate append pipeline optimized for streaming/token use cases
- annotation storage distinct from message payload storage
- summary rollup infrastructure with clipping behavior
- explicit capability enforcement for own/any mutation scopes
- identified-client semantics across REST, WebSocket, and SDK surfaces

## Edge cases Sockudo must match if parity is the goal

- A subscriber is not entitled to receive every intermediate update/delete/append.
- `message.delete` is soft delete, not physical removal.
- Delete does not imply redaction unless the delete payload clears visible fields.
- Appends may be delivered as `message.update` instead of `message.append`.
- First append after attach should be safe to consume without fetching prior history.
- Intermediate append pipeline optimism can diverge from per-append acknowledgement outcomes.
- History/getMessage must converge on the winning latest version even if subscribers saw racing mutations.
- Annotation deletes modify summaries, not historical existence of annotation events.
- Large summaries clip participant lists but preserve totals.

## Source links

- [Message annotations](https://ably.com/docs/messages/annotations)
- [Updates, deletes and appends](https://ably.com/docs/messages/updates-deletes)
