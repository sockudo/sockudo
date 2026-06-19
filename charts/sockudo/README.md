# Sockudo Helm Chart

This chart deploys the Sockudo server and can optionally deploy the Sockudo
operator dashboard.

## Dashboard

The dashboard is disabled by default:

```yaml
dashboard:
  enabled: false
```

To enable it in production:

- use a durable app manager: `mysql`, `pgsql`/`postgres`, or `dynamodb`
- keep `HTTP_API_USAGE_ENABLED` and Prometheus metrics enabled
- provide a strong `DASHBOARD_SESSION_SECRET` through an existing Kubernetes Secret
- seed the first admin through an existing Secret or create users after install
- expose the dashboard through TLS and restrict network access

Example:

```yaml
config:
  appManagerDriver: pgsql
  httpApi:
    usageEnabled: true
  metrics:
    enabled: true

database:
  postgres:
    host: postgres.default.svc
    port: 5432
    username: sockudo
    database: sockudo
    tableName: applications
  existingSecret: sockudo-postgres

dashboard:
  enabled: true
  sessionSecret:
    existingSecret: sockudo-dashboard-session
  seedAdmin:
    enabled: true
    existingSecret: sockudo-dashboard-seed
  ingress:
    enabled: true
    hosts:
      - host: sockudo-admin.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: sockudo-admin-tls
        hosts:
          - sockudo-admin.example.com
```

Expected secret keys:

```bash
kubectl create secret generic sockudo-dashboard-session \
  --from-literal=dashboard-session-secret="$(openssl rand -base64 32)"

kubectl create secret generic sockudo-dashboard-seed \
  --from-literal=dashboard-seed-email="admin@example.com" \
  --from-literal=dashboard-seed-password="change-me-with-a-long-random-password" \
  --from-literal=dashboard-seed-name="Administrator"
```

`dashboard.seedAdmin` is only used when the dashboard user table is empty. After
bootstrap, manage operators from the Users page.

The chart creates:

- `Deployment` and `Service` for `dashboard-api`
- `Deployment` and `Service` for `dashboard-web`
- optional dashboard `Ingress`
- optional dashboard `HorizontalPodAutoscaler`, `PodDisruptionBudget`, and `NetworkPolicy`
- optional PVC for `/app/data`; required when `dashboard.databaseDriver=sqlite`
- Helm test hook that checks the dashboard API `/health` and web root

Dashboard images default to `sockudo/dashboard-api:<chart appVersion>` and
`sockudo/dashboard-web:<chart appVersion>`. Override
`dashboard.api.image.*` and `dashboard.web.image.*` if your registry publishes
different image names or tags.
