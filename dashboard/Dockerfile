# =============================================================================
# Sockudo Operator Dashboard - Multi-stage Docker Build
# Targets: api, web
# =============================================================================

# -----------------------------------------------------------------------------
# API: install production dependencies
# -----------------------------------------------------------------------------
FROM oven/bun:1-debian AS api-deps

WORKDIR /app

COPY api/package.json api/bun.lock ./
RUN bun install --frozen-lockfile --production

# -----------------------------------------------------------------------------
# API: runtime image
# -----------------------------------------------------------------------------
FROM oven/bun:1-debian AS api

WORKDIR /app

COPY --from=api-deps /app/node_modules ./node_modules
COPY api/package.json api/bun.lock ./
COPY api/src ./src

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app/data \
    && chown -R bun:bun /app/data /app

USER bun

ENV NODE_ENV=production
ENV DASHBOARD_API_PORT=3460
ENV DASHBOARD_SQLITE_PATH=/app/data/dashboard.sqlite

EXPOSE 3460

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -f http://127.0.0.1:${DASHBOARD_API_PORT}/health || exit 1

CMD ["bun", "run", "start"]

# -----------------------------------------------------------------------------
# Web: build static assets
# -----------------------------------------------------------------------------
FROM oven/bun:1-debian AS web-build

WORKDIR /app

COPY web/package.json web/bun.lock ./
RUN bun install --frozen-lockfile

COPY web/ ./
RUN bun run build

# -----------------------------------------------------------------------------
# Web: nginx static + API reverse proxy
# -----------------------------------------------------------------------------
FROM nginx:alpine AS web

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=web-build /app/dist /usr/share/nginx/html

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
