# Sockudo operations runbook (condensed)

## Health signals
- `sockudo_server_health` (`/up`): 200 when adapter and cache are healthy and the node accepts
  traffic; degraded webhook queues are reported but non-fatal.
- `sockudo_server_stats`: per-app connections, users, channel occupancy, presence members, plus
  history and presence-history durable state (`degraded_channels`, `reset_required_channels`,
  `queue_depth`).
- `sockudo_server_metrics`: Prometheus text. Key families: `*_connected`, `*_ws_messages_*`,
  `*_api_messages_*`, `*_broadcast_latency_ms`, `*_history_*`, `*_versioned_*`, `*_push_*`,
  `*_ai_*`, `*_horizontal_adapter_*`, `*_mcp_*`.

## Common incidents
- **Clients cannot subscribe to private/presence channels**: verify the auth signature with
  `sockudo_sign_channel_auth` against the same `socket_id` and `channel_data` the client used.
- **Publishes return 401/403**: wrong app key/secret pair, stale `auth_timestamp` (600s window), or
  the token lacks the scope. Signed requests are per-app; check `sockudo_get_app`.
- **History gaps or `reset_required`**: inspect `sockudo_get_history_state`. Only reset with an
  explicit operator decision and reason; a reset rotates `stream_id` and clients lose continuity.
- **Fanout latency**: check `broadcast_latency_ms` and `horizontal_adapter_*` metrics; confirm the
  adapter backend (Redis, NATS, ...) is healthy via `/up`.
- **Push not delivered**: `sockudo_push_publish_status`, then `sockudo_list_push_dead_letters`;
  provider credentials are listed (redacted) with `sockudo_list_push_credentials`.

## Safety rules for agents
- Destructive tools (`reset_history`, `purge_history`, `terminate_user_connections`,
  `revoke_capability_tokens`) need the `admin` scope and a human-readable `reason`; state the
  reason before calling them.
- Never print app secrets. Signature tools return only the derived `auth` strings.
