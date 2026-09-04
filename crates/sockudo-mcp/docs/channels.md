# Sockudo channel model

Channel names use `[A-Za-z0-9_\-=@,.;]` and are namespaced by prefix:

| Prefix | Auth | Notes |
| --- | --- | --- |
| (none) | public | Anyone with the app key can subscribe. |
| `private-` | signed | Client presents `auth = "{key}:{hmac}"`; HMAC over `"{socket_id}:{channel}"`. |
| `presence-` | signed + member data | HMAC over `"{socket_id}:{channel}:{channel_data}"`; `channel_data` JSON has `user_id` and optional `user_info`. Members are visible via the users endpoint and presence history. |
| `private-encrypted-` | signed | Payload encrypted client-side with a shared key. |
| `cache-`, `private-cache-`, `presence-cache-` | as above | Server replays the last event to new subscribers. |

User authentication (`pusher:signin`) uses HMAC over `"{socket_id}::user::{user_data}"`.

Protocol V2 adds per-connection recovery (`stream_id` + `serial`), durable history with opaque
cursors, versioned messages (`message_serial` / `version_serial`), annotations, tag filtering,
delta compression, and capability-token auth. V1 clients receive Pusher-compatible frames only;
V2-only fields are stripped for them.

Agent guidance:
- Use `sockudo_list_channels` with `filter_by_prefix` to narrow results; listing every channel on a
  large app is a cluster-wide aggregation.
- `user_count` is only valid for `presence-` channels; `subscription_count` works everywhere.
- History, presence history, versioned messages, and annotations are policy-gated per app and per
  channel namespace; a `feature_disabled` error means configuration, not a bug.
- Prefer `idempotency_key` on publishes an agent might retry.
