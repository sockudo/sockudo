# Release 4.3 Domain Model And Invariants

## Scope

This artifact defines the core domain model for release 4.3 before full storage, API, and runtime integration.

Implementation anchor:

- `crates/sockudo-core/src/versioned_messages.rs`

## Core Model

Release 4.3 distinguishes four concepts:

- `message_serial`: stable logical message identity
- `version_serial`: identity of one concrete version
- `history_serial`: fixed position in latest-view channel history
- `delivery_serial`: continuity position for replay and recovery

The model also defines:

- `MessageAction`: `create`, `update`, `delete`, `append`, reserved `summary`
- `VersionedMessage`: one fully materialized visible version
- `MessageFieldDelta`: tri-state field patch for shallow-mixin semantics
- `MessageAppend`: append fragment that produces a new rolled-up visible state

## Explicit Invariants

### Identity invariants

1. All versions in one version chain share the same `message_serial`.
2. All versions in one version chain share the same `history_serial`.
3. `version_serial` values are unique within one chain.

### Latest-visible-version invariants

4. The winning visible version is the version with the lexicographically greatest `version_serial`.
5. Winner selection is data-only and does not depend on arrival order.

### Mutation invariants

6. Update and delete use tri-state shallow-mixin semantics:
   - keep retains the current value
   - clear removes the visible value
   - replace writes a new value
7. Append stores the full rolled-up visible payload, not only the fragment.
8. The 4.3 core append model only accepts string payloads.

### History invariants

9. Latest-view history is one logical row per `message_serial`.
10. Mutations do not move the original `history_serial`.

### Replay invariants

11. Replay continuity is ordered by `delivery_serial`, not by `history_serial`.
12. Recovery slices must be contiguous from the requested cursor forward.
13. A domain model that cannot represent both fixed history position and contiguous replay continuity is insufficient for 4.3.

## Implementation Notes

The current implementation intentionally stops at the domain layer:

- invariants are encoded as helper functions and unit tests
- no mutation APIs are wired yet
- no durable version store is wired yet
- no recovery integration is wired yet

That separation is deliberate. Prompt 4.3-02 establishes the model and rules first so later prompts can implement storage and runtime behavior against a fixed contract.
