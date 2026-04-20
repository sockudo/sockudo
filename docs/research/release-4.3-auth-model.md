# Release 4.3 Auth, Capabilities, And Security Model

## Scope

This artifact defines the authorization model for mutable messages in release 4.3.

Implementation anchors:

- `crates/sockudo-core/src/websocket.rs`
- `crates/sockudo-core/src/versioned_message_auth.rs`

## Capability Classes

Release 4.3 adds six mutation capability classes on signed-in V2 identities:

- `message_update_own`
- `message_update_any`
- `message_delete_own`
- `message_delete_any`
- `message_append_own`
- `message_append_any`

Each field is channel-pattern scoped and follows the same wildcard matching model already used by subscribe/publish capabilities.

## Authorization Rules

### Any scope

- `*_any` permits the mutation regardless of original creator identity.
- This is the stronger grant and implies the operation is allowed on matching channels.

### Own scope

- `*_own` requires an identified actor `client_id`.
- `*_own` also requires the original created message to have an identified `client_id`.
- Authorization succeeds only when actor and original creator identities match exactly.

### Missing capability

- Mutation capabilities default to deny.
- This is intentionally stricter than subscribe/publish, which preserve their older unrestricted default when capability fields are absent.

### Privileged server

- Trusted server-side callers may be treated as `any`-scoped actors.
- This is represented in the shared auth helper as `privileged_server = true`.

## Security Notes

- Ownership is identity-bound, not connection-instance-bound.
- Unidentified actors cannot use own-scoped mutation paths safely.
- Messages created without an original identified actor cannot be mutated through own-scoped authorization.
