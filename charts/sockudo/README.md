# Sockudo Helm Chart

This chart deploys the Sockudo server and can optionally deploy the Sockudo
operator dashboard.

## Install

```bash
helm install sockudo oci://ghcr.io/sockudo/charts/sockudo --version 4.7.0
```

Always pass `--version`. Without it Helm resolves the newest release at install time,
which makes redeploys non-reproducible.

GitHub Packages renders a Helm chart as though it were a container image, so the package
page lists it under "Containers" and never displays the `oci://` address. The command above
is the address; it cannot be derived from that page.

To see every available option:

```bash
helm show values oci://ghcr.io/sockudo/charts/sockudo --version 4.7.0
```

For chart development, install from a checkout instead:

```bash
helm install sockudo ./charts/sockudo
```

## Versioning

The chart version always equals the Sockudo application version, and the chart is
republished with every application release. Chart `4.7.0` deploys application `4.7.0`;
there is no separate chart version to cross-reference.

One consequence worth knowing: consecutive chart versions may be identical charts. `4.7.1`
following `4.7.0` means a new application release, not necessarily a chart change.

## Configuration file

By default the chart generates a ConfigMap from the `config.*` values and mounts it at
`/app/config/config.json`. That is fine as long as the configuration holds no secrets.

It cannot hold secrets, though, for any deployment that needs per-app webhooks: those are
expressible only in the config file, not through environment variables, so the app secret
has to sit next to them - and a ConfigMap means plaintext in the values file, and in
practice in the repository holding it.

Point `config.existingSecret` at a Secret to supply the whole file instead:

```yaml
config:
  existingSecret: sockudo-config
  existingSecretKey: config.json
```

The ConfigMap is then not rendered at all, and the `config` volume is backed by the Secret.
`config.existingSecret` and `configJson` are mutually exclusive; setting both fails the
render.

Two things to know:

- The mount path is `/app/config/config.json` and Sockudo picks its parser from the file
  extension, so the Secret's content must be JSON even though the annotated reference
  configuration is TOML.
- Because Helm never sees the Secret's content, the `checksum/config` pod annotation is not
  emitted in this mode. Rotating the Secret does not restart the pods; use a reloader
  controller or restart the Deployment yourself.

The Secret is usually managed outside the chart. With External Secrets Operator:

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: sockudo-config
spec:
  secretStoreRef:
    kind: ClusterSecretStore
    name: my-secret-store
  target:
    name: sockudo-config
    template:
      engineVersion: v2
      data:
        config.json: "{{ .sockudoConfig }}"
  data:
    - secretKey: sockudoConfig
      remoteRef:
        key: SOCKUDO_CONFIG_FILE
```

## OpenTelemetry

OpenTelemetry export is disabled by default. Enable any combination of stable traces, metrics,
and logs with `config.openTelemetry`. Export is additive: the Prometheus service and local logs
continue to work as configured.

```yaml
config:
  openTelemetry:
    enabled: true
    tracesEnabled: true
    metricsEnabled: true
    logsEnabled: true
    serviceName: sockudo
    serviceNamespace: realtime
    deploymentEnvironment: production
    endpoint: http://otel-collector.observability.svc:4317
    resourceAttributes:
      k8s.cluster.name: production-eu

extraEnv:
  - name: OTEL_EXPORTER_OTLP_PROTOCOL
    value: grpc
  - name: OTEL_TRACES_SAMPLER
    value: parentbased_traceidratio
  - name: OTEL_TRACES_SAMPLER_ARG
    value: "0.10"
```

`OTEL_EXPORTER_OTLP_PROTOCOL` accepts `grpc`, `http/protobuf`, or `http/json`. For HTTP,
configure the collector's port (normally `4318`) instead of the gRPC port (`4317`). A common
`OTEL_EXPORTER_OTLP_ENDPOINT` is treated as a base endpoint; signal-specific
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, and
`OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` are also supported.

Do not place exporter headers or API keys in chart values.
Expose them from a Secret with `extraEnvFrom`:

```yaml
extraEnvFrom:
  - secretRef:
      name: sockudo-otel-exporter
```

For example, that Secret can contain `OTEL_EXPORTER_OTLP_HEADERS`. HTTPS exporters use the bundled
platform/web PKI roots; custom CA bundles and mTLS exporter configuration are not supported by the
current Rust SDK integration. If a NetworkPolicy restricts egress, explicitly allow the collector
address and port. Sockudo does not receive OTLP, so no collector port belongs in the Sockudo
Service.

Collector failures are fail-open: they do not change `/live`, `/up`, or request handling. Export
queues are bounded, and graceful shutdown flushes them within the configured timeout. W3C
`traceparent`/`tracestate` and baggage propagation are enabled by default when OpenTelemetry is
enabled. OpenTelemetry profiles are not supported.

## Rollouts and autoscaling

`strategy` is passed straight through to the Deployment. Left unset, Kubernetes applies its
default of `maxUnavailable: 25%` / `maxSurge: 25%`, which drops a quarter of the connection
capacity mid-rollout - precisely while the clients from those pods are all reconnecting:

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 0
    maxSurge: 25%
```

`autoscaling.metrics` is passed straight through to the HorizontalPodAutoscaler and accepts
any `autoscaling/v2` metric type. CPU utilisation tracks message throughput rather than
connection headroom - a pod can sit near its connection ceiling at low CPU - so the metric
that reflects capacity is `sockudo_connected`:

```yaml
autoscaling:
  enabled: true
  metrics:
    - type: External
      external:
        metric:
          name: sockudo_connected
        target:
          type: AverageValue
          averageValue: "4000"
```

Setting `autoscaling.metrics` **replaces** the CPU and memory metrics generated from
`targetCPUUtilizationPercentage` and `targetMemoryUtilizationPercentage` rather than adding
to them: an HPA scales on the highest recommendation from any metric, so leaving CPU in
place would keep overriding a connection-count metric.

`dashboard.api.strategy`, `dashboard.web.strategy`,
`dashboard.autoscaling.api.metrics` and `dashboard.autoscaling.web.metrics` behave the same
way for the dashboard deployments.

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
