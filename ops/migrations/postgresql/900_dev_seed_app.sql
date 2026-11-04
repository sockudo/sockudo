-- Development/test seed ONLY. Do not apply to production databases.
--
-- Creates the well-known demo application (`app-id` / `app-key` /
-- `app-secret`) used by the local docker-compose dev/test stacks and
-- integration tests. Its credentials are public, so anyone who can reach a
-- server backed by this database could sign API requests for it.
--
-- Production: create applications with your own randomly generated key and
-- secret instead of applying this file.

INSERT INTO applications (
    id,
    key,
    secret,
    max_connections,
    enable_client_messages,
    enabled,
    max_client_events_per_second
) VALUES (
    'app-id',
    'app-key',
    'app-secret',
    100000,
    TRUE,
    TRUE,
    1000
)
ON CONFLICT (id) DO NOTHING;
