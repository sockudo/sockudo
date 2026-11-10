import { describe, expect, test } from "bun:test";
import { createHmac } from "node:crypto";
import type { AppRecord } from "../types/app.ts";
import {
  createSignedSockudoStatsUrl,
  discoverPrometheusPrefix,
  mergeSockudoStats,
  parsePrometheusMetrics,
  parsePrometheusText,
  summarizeMetrics,
} from "./sockudo.ts";

describe("Prometheus text parsing", () => {
  test("parses metadata, standard labels, escapes, and optional timestamps", () => {
    const text = String.raw`
# HELP custom_connected Active\nconnections
# TYPE custom_connected gauge
custom_connected{app_id="app-a",port="6001",detail="west, \"primary\" \\ route\nnext"} 3 1710000000000
# TYPE custom_requests_total counter
custom_requests_total{app_id="app-a"} 1.25e+03
`;

    const parsed = parsePrometheusMetrics(text);

    expect(parsed.samples).toEqual([
      {
        name: "custom_connected",
        labels: {
          app_id: "app-a",
          port: "6001",
          detail: 'west, "primary" \\ route\nnext',
        },
        value: 3,
        timestamp: 1_710_000_000_000,
      },
      {
        name: "custom_requests_total",
        labels: { app_id: "app-a" },
        value: 1_250,
      },
    ]);
    expect(parsed.families).toEqual([
      {
        name: "custom_connected",
        type: "gauge",
        help: "Active\nconnections",
        samples: [parsed.samples[0]],
      },
      {
        name: "custom_requests_total",
        type: "counter",
        samples: [parsed.samples[1]],
      },
    ]);
    expect(Number.isNaN(Date.parse(parsed.scraped_at))).toBeFalse();
  });

  test("groups histogram exposition samples into one metric family", () => {
    const text = `
# HELP request_latency_seconds Request latency.
# TYPE request_latency_seconds histogram
request_latency_seconds_bucket{le="0.5"} 2
request_latency_seconds_bucket{le="+Inf"} 3
request_latency_seconds_sum 1.25
request_latency_seconds_count 3
`;

    const parsed = parsePrometheusMetrics(text);

    expect(parsed.families).toHaveLength(1);
    expect(parsed.families[0].name).toBe("request_latency_seconds");
    expect(parsed.families[0].type).toBe("histogram");
    expect(parsed.families[0].help).toBe("Request latency.");
    expect(parsed.families[0].samples).toHaveLength(4);
    expect(parsed.families[0].samples[1].labels.le).toBe("+Inf");
  });

  test("skips malformed or non-finite samples without losing valid samples", () => {
    const text = String.raw`
valid_metric{label="ok"} 4
broken_label{label="unterminated} 1
broken_escape{label="\t"} 2
broken_timestamp 3 1.5
not_finite NaN
too_many_fields 4 1710000000000 extra
`;

    expect(parsePrometheusText(text)).toEqual([
      { name: "valid_metric", labels: { label: "ok" }, value: 4 },
    ]);
  });

  test("keeps the legacy flat sample parser contract", () => {
    const samples = parsePrometheusText(
      'sockudo_connected{app_id="app-a",port="6001"} 2\n',
    );

    expect(Array.isArray(samples)).toBeTrue();
    expect(samples).toEqual([
      {
        name: "sockudo_connected",
        labels: { app_id: "app-a", port: "6001" },
        value: 2,
      },
    ]);
  });
});

describe("Prometheus summaries", () => {
  test("uses the actual connected metric and aggregates ports by app", () => {
    const samples = parsePrometheusText(`
sockudo_connected{app_id="app-b",port="6001"} 2
sockudo_connected{app_id="app-a",port="6001"} 3
sockudo_connected{app_id="app-a",port="6002"} 1
sockudo_new_connections_total{app_id="app-a",port="6001"} 8
`);

    const summary = summarizeMetrics(samples);

    expect(summary.metrics_prefix).toBe("sockudo_");
    expect(summary.connected_sockets).toBe(6);
    expect(summary.new_connections_total).toBe(8);
    expect(summary.by_app).toEqual([
      { app_id: "app-a", connected_sockets: 4 },
      { app_id: "app-b", connected_sockets: 2 },
    ]);
  });

  test("discovers a custom prefix and applies it to all summary metrics", () => {
    const samples = parsePrometheusText(`
tenant_tokio_workers_count 4
tenant_connected{app_id="app-a",port="6001"} 5
tenant_ws_messages_received_total{app_id="app-a",port="6001"} 12
sockudo_push_ws_messages_received_total{app="other"} 99
tenant_history_write_failures_total{app_id="app-a",port="6001"} 2
`);

    expect(discoverPrometheusPrefix(samples)).toBe("tenant_");
    expect(summarizeMetrics(samples)).toMatchObject({
      metrics_prefix: "tenant_",
      connected_sockets: 5,
      ws_messages_received_total: 12,
      history_write_failures_total: 2,
    });
  });
});

const statsApp: AppRecord = {
  id: "app-1",
  key: "public-key",
  secret: "top-secret",
  enabled: true,
  policy: {
    limits: { max_connections: 100, max_client_events_per_second: 10 },
    features: { enable_client_messages: false },
    channels: {},
  },
};

describe("Sockudo /stats signing", () => {
  test("signs GET /stats with the Pusher canonical form", () => {
    const url = new URL(
      createSignedSockudoStatsUrl(statsApp, 1_785_312_000, "http://sockudo:6001"),
    );
    expect(url.pathname).toBe("/stats");
    expect(url.searchParams.get("auth_key")).toBe("public-key");
    expect(url.searchParams.get("auth_timestamp")).toBe("1785312000");
    expect(url.searchParams.get("auth_version")).toBe("1.0");
    const expected = createHmac("sha256", "top-secret")
      .update(
        "GET\n/stats\nauth_key=public-key&auth_timestamp=1785312000&auth_version=1.0",
      )
      .digest("hex");
    expect(url.searchParams.get("auth_signature")).toBe(expected);
    expect(url.toString()).not.toContain(statsApp.secret);
  });

  test("merges per-app responses into one operator view", () => {
    const merged = mergeSockudoStats([
      {
        memory: { used: 1 },
        totals: { apps: 1, connections: 2, occupancy: { channels: 3 } },
        apps: [{ app_id: "b", connections: 2 }],
      },
      {
        memory: { used: 9 },
        totals: { apps: 1, connections: 5, occupancy: { channels: 1 } },
        apps: [{ app_id: "a", connections: 5 }],
      },
    ]);
    expect(merged.memory).toEqual({ used: 1 });
    expect(merged.totals).toEqual({
      apps: 2,
      connections: 7,
      occupancy: { channels: 4 },
    });
    expect(merged.apps.map((app) => app.app_id)).toEqual(["a", "b"]);
  });
});
