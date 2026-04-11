# Ops

This directory is the intended home for operational and deployment surfaces
that are currently spread across the repository root.

Current contents:

- `grafana/` for dashboards and Grafana provisioning
- `monitoring/` for Prometheus config and alert rules
- `nginx/` for reverse-proxy configs and SSL assets
- `migrations/` for canonical fresh-schema files and backend bootstrap notes

Possible future contents:

- `compose/` for tracked Docker Compose files

Why this exists:

- The repository root currently mixes product code with deployment and
  operational assets.
- `ops/` provides a stable landing zone for future moves without forcing a
  large risky relocation in one step.

Migration rule:

- Move tracked infra paths here only when all references are updated in the
  same change.
- Do not move generated files or local-only artifacts into `ops/`.
