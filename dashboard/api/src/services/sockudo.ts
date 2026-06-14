import { config } from "../config.ts";

export async function fetchSockudoStats() {
  const res = await fetch(`${config.sockudoHttpUrl}/stats`, {
    signal: AbortSignal.timeout(5_000),
  });
  if (!res.ok) {
    throw new Error(`Sockudo /stats returned ${res.status}`);
  }
  return res.json();
}

export async function fetchSockudoUsage() {
  const res = await fetch(`${config.sockudoHttpUrl}/usage`, {
    signal: AbortSignal.timeout(5_000),
  });
  if (!res.ok) {
    throw new Error(`Sockudo /usage returned ${res.status}`);
  }
  return res.json();
}

export async function fetchSockudoHealth(appId?: string) {
  const path = appId ? `/up/${encodeURIComponent(appId)}` : "/up";
  const res = await fetch(`${config.sockudoHttpUrl}${path}`, {
    signal: AbortSignal.timeout(5_000),
  });
  return {
    status: res.status,
    ok: res.ok,
    body: await res.text(),
  };
}

export async function fetchPrometheusMetrics(): Promise<string> {
  const res = await fetch(`${config.sockudoMetricsUrl}/metrics`, {
    signal: AbortSignal.timeout(10_000),
  });
  if (!res.ok) {
    throw new Error(`Sockudo /metrics returned ${res.status}`);
  }
  return res.text();
}

export interface MetricSample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

export function parsePrometheusText(text: string): MetricSample[] {
  const samples: MetricSample[] = [];
  for (const line of text.split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const space = line.lastIndexOf(" ");
    if (space <= 0) continue;
    const value = Number(line.slice(space + 1));
    if (!Number.isFinite(value)) continue;
    const metricPart = line.slice(0, space);
    const braceStart = metricPart.indexOf("{");
    if (braceStart === -1) {
      samples.push({ name: metricPart, labels: {}, value });
      continue;
    }
    const name = metricPart.slice(0, braceStart);
    const labelsRaw = metricPart.slice(braceStart + 1, -1);
    const labels: Record<string, string> = {};
    for (const pair of labelsRaw.match(/(?:[^,"\\]|\\.)+/g) ?? []) {
      const eq = pair.indexOf("=");
      if (eq === -1) continue;
      const key = pair.slice(0, eq).trim();
      const raw = pair.slice(eq + 1).trim();
      labels[key] = raw.replace(/^"|"$/g, "").replace(/\\"/g, '"');
    }
    samples.push({ name, labels, value });
  }
  return samples;
}

export function summarizeMetrics(samples: MetricSample[]) {
  const sumByName = (name: string) =>
    samples
      .filter((s) => s.name === name)
      .reduce((acc, s) => acc + s.value, 0);

  const gaugeByName = (name: string) => {
    const matches = samples.filter((s) => s.name === name);
    return matches.reduce((acc, s) => acc + s.value, 0);
  };

  return {
    connected_sockets: gaugeByName("sockudo_connected_sockets"),
    new_connections_total: sumByName("sockudo_new_connections_total"),
    new_disconnections_total: sumByName("sockudo_new_disconnections_total"),
    ws_messages_received_total: sumByName("sockudo_ws_messages_received_total"),
    ws_messages_sent_total: sumByName("sockudo_ws_messages_sent_total"),
    http_calls_received_total: sumByName("sockudo_http_calls_received_total"),
    channel_subscriptions_total: sumByName(
      "sockudo_channel_subscriptions_total",
    ),
    channel_unsubscriptions_total: sumByName(
      "sockudo_channel_unsubscriptions_total",
    ),
    rate_limit_triggered_total: sumByName("sockudo_rate_limit_triggered_total"),
    connection_errors_total: sumByName("sockudo_connection_errors_total"),
    history_writes_total: sumByName("sockudo_history_writes_total"),
    history_write_failures_total: sumByName(
      "sockudo_history_write_failures_total",
    ),
    by_app: samples
      .filter((s) => s.name === "sockudo_connected_sockets" && s.labels.app_id)
      .map((s) => ({
        app_id: s.labels.app_id,
        connected_sockets: s.value,
      })),
  };
}
