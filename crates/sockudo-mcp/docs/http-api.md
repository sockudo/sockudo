# Sockudo HTTP API quick reference (for agents)

Every `/apps/{app_id}/...` route is signed with the app key/secret (Pusher-compatible
HMAC-SHA256 over method, path, and sorted query). MCP tools sign for you; you only pass
`app_id`.

## Publish
- `POST /apps/{app_id}/events` — one event to one or more channels. Body: `name`, `data`,
  `channel` or `channels`, optional `socket_id` (suppress echo), `info`
  (`subscription_count`, `user_count`), `idempotency_key`, `message_id`, `tags`, `delta`, `extras`.
- `POST /apps/{app_id}/batch_events` — `{"batch": [event, ...]}`.
- `POST /apps/{app_id}/users/{user_id}/terminate_connections` — disconnect a user.
- `POST /apps/{app_id}/users/{user_id}/force_reconnect` — close with 4200 so clients reconnect.
- `POST /apps/{app_id}/revocations` — revoke Protocol V2 capability tokens (`jti`, `client_id`).

## Channel state (needs live socket state; 404 in `server_role = "api"`)
- `GET /apps/{app_id}/channels?filter_by_prefix=&info=subscription_count,user_count`
- `GET /apps/{app_id}/channels/{channel}?info=...`
- `GET /apps/{app_id}/channels/{channel}/users` — presence members.

## Durable history
- `GET .../channels/{channel}/history` — `limit`, `direction` (`newest_first`|`oldest_first`),
  `cursor`, `start_serial`, `end_serial`, `start_time_ms`, `end_time_ms`. Returns `items`,
  `next_cursor`, `continuity`, `stream_state`.
- `GET .../history/state` — stream id, retained counts, degraded/reset-required flags.
- `POST .../history/reset` and `.../history/purge` — destructive; require `confirm_channel`,
  `confirm_operation`, `reason`.

## Versioned (mutable) messages
- `GET .../messages/{message_serial}` and `.../versions`.
- `POST .../messages/{message_serial}/update|delete|append` — server-privileged unless a
  `socket_id` actor is supplied, in which case that connection's identity and capabilities apply.

## Annotations
- `GET|POST .../messages/{message_serial}/annotations`, `DELETE .../annotations/{annotation_serial}`.
  Require versioned messages, annotations enabled globally, and channel policy opt-in.

## Presence history
- `GET .../presence/history`, `.../presence/history/state`, `.../presence/history/snapshot`
  (`at_time_ms` or `at_serial`), `POST .../presence/history/reset`.

## Push
- `POST /apps/{app_id}/push/publish`, `.../push/batch/publish`, `GET .../push/publish/{id}/status`,
  device registrations, channel subscriptions, dead letters, credentials, templates.

## Operational (unsigned)
- `GET /up`, `/up/{app_id}`, `/live`, `/accept-traffic`, `/usage`, `/operator/stats`.
- Prometheus metrics on the metrics port (`/metrics`, default 9601).

Errors are JSON: `{"error": "...", "code": "...", "status": 4xx}`. Codes such as
`feature_disabled`, `invalid_input`, `not_found`, `limit_exceeded`, `too_many_requests`.
