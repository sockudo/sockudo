# Sockudo Operator Dashboard

Separate admin UI for managing Sockudo apps and webhooks. Dashboard **users** are stored
in the dashboard database (migrations), not in environment variables.

## Structure

```text
dashboard/
├── api/     Bun + Hono admin API (auth, users, CRUD, metrics proxy)
└── web/     Vue 3 operator UI
```

## Prerequisites

1. Sockudo must use a **durable** app manager (not `memory`):

   ```bash
   APP_MANAGER_DRIVER=pgsql   # or mysql, dynamodb
   ```

2. Enable operational endpoints and metrics on Sockudo:

   ```bash
   HTTP_API_USAGE_ENABLED=true
   METRICS_ENABLED=true
   ```

## Quick start

```bash
# 1. Configure environment
cp dashboard/.env.example .env

# 2. Install API, run migrations, seed first admin
cd dashboard/api
bun install
bun run migrate
bun run seed:admin admin@sockudo.local 'change-me-now' 'Admin'

# 3. Start API
bun run dev

# 4. Start UI (separate terminal)
cd dashboard/web && bun install && bun run dev
```

- API: http://localhost:3460
- UI: http://localhost:5174

## Docker

`make up` (default `ENV=dev`) starts Sockudo **and** the operator dashboard:

```bash
make setup    # first time: creates .env
make build    # build images including dashboard
make up       # starts sockudo, redis, mysql, dashboard-api, dashboard-web
```

| Service | URL |
|---------|-----|
| Dashboard UI | http://localhost:5174 |
| Dashboard API | http://localhost:3460 |

Set `DASHBOARD_SESSION_SECRET` and optional `DASHBOARD_SEED_EMAIL` / `DASHBOARD_SEED_PASSWORD` in `.env` before first run.

Or manually with compose:

```bash
# Set a session secret and optional first admin seed in .env
export DASHBOARD_SESSION_SECRET=$(openssl rand -base64 32)
export DASHBOARD_SEED_EMAIL=admin@sockudo.local
export DASHBOARD_SEED_PASSWORD='change-me-now'

docker compose \
  -f docker-compose.yml \
  -f docker-compose.dev.yml \
  -f docker-compose.dashboard.yml \
  up -d --build
```

| Service | URL |
|---------|-----|
| Dashboard UI | http://localhost:5174 |
| Dashboard API | http://localhost:3460 |
| Sockudo | http://localhost:6001 |

Images are built from `dashboard/Dockerfile` (`api` and `web` targets). Migrations and
optional admin seed run automatically on API startup.

**Cargo / Rust:** the dashboard is not part of the Rust workspace; no `Cargo.toml` changes
are required.

## Dashboard database

Dashboard users live in separate tables (`dashboard_users`, `dashboard_migrations`):

| `APP_MANAGER_DRIVER` | Dashboard DB used |
|----------------------|-------------------|
| `pgsql` | Same PostgreSQL database as Sockudo apps |
| `mysql` | Same MySQL database as Sockudo apps |
| `dynamodb` | Local SQLite (`dashboard/api/data/dashboard.sqlite`) |

Override with `DASHBOARD_DATABASE_DRIVER=sqlite|mysql|pgsql`.

Migrations run automatically on API startup. To run manually:

```bash
cd dashboard/api && bun run migrate
```

Seed the first admin (only when no users exist):

```bash
bun run seed:admin <email> <password> [name]
```

## User management API

| Method | Path | Access | Description |
|--------|------|--------|-------------|
| POST | `/api/v1/auth/login` | Public | Login with email/password |
| GET | `/api/v1/auth/me` | Auth | Current user |
| GET | `/api/v1/users` | Admin | List users |
| POST | `/api/v1/users` | Admin | Create user |
| GET | `/api/v1/users/:id` | Admin or self | Get user |
| PUT | `/api/v1/users/:id` | Admin or self | Update user |
| DELETE | `/api/v1/users/:id` | Admin | Delete user |
| POST | `/api/v1/users/:id/change-password` | Admin or self | Change password |

Roles: `admin` (full access including user management), `operator` (apps/webhooks/metrics).

## Environment variables

| Variable | Purpose |
|----------|---------|
| `APP_MANAGER_DRIVER` | Sockudo app store: `mysql`, `pgsql`, `dynamodb` |
| `DASHBOARD_DATABASE_DRIVER` | Override dashboard DB driver |
| `DASHBOARD_SESSION_SECRET` | JWT session signing secret |
| `DASHBOARD_SQLITE_PATH` | SQLite path when using sqlite driver |
| `DATABASE_*` | Shared DB credentials (mysql/pgsql) |
| `SOCKUDO_HTTP_URL` | Sockudo main port for `/stats` |
| `SOCKUDO_METRICS_URL` | Metrics port |

**Do not** put operator credentials in `.env` — use `bun run seed:admin` or the Users UI.

## Cache note

Sockudo caches apps in memory. After dashboard app/webhook changes, nodes may take up to
`CACHE_TTL_SECONDS` to pick up updates.

## Security

- Use strong passwords (min 8 chars) for all dashboard users.
- Set a random `DASHBOARD_SESSION_SECRET` in production.
- Put the dashboard behind TLS and restrict network access.
