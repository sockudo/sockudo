import { config } from "../config.ts";

/**
 * The cross-tenant operator stats are served on Sockudo's internal metrics
 * listener (the same one as `/metrics`). The client-facing `/stats` only
 * reports the app that signed the request (GHSA-xfrj-v26g-242j).
 */
export async function fetchSockudoStats() {
  const res = await fetch(`${config.sockudoMetricsUrl}/stats`, {
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
  // App-specific readiness (404 unknown, 503 disabled) is only answered on the
  // internal metrics listener; the public `/up/{appId}` mirrors `/up`.
  const url = appId
    ? `${config.sockudoMetricsUrl}/up/${encodeURIComponent(appId)}`
    : `${config.sockudoHttpUrl}/up`;
  const res = await fetch(url, {
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
  timestamp?: number;
}

export type PrometheusMetricType =
  | "counter"
  | "gauge"
  | "histogram"
  | "summary"
  | "untyped";

export interface MetricFamily {
  name: string;
  type: PrometheusMetricType;
  help?: string;
  samples: MetricSample[];
}

export interface ParsedPrometheusMetrics {
  samples: MetricSample[];
  families: MetricFamily[];
  scraped_at: string;
}

interface MetricMetadata {
  name: string;
  type?: PrometheusMetricType;
  help?: string;
}

const PROMETHEUS_TYPES = new Set<PrometheusMetricType>([
  "counter",
  "gauge",
  "histogram",
  "summary",
  "untyped",
]);

const PREFIX_DISCOVERY_SUFFIXES = [
  "tokio_workers_count",
  "tokio_active_tasks",
  "connected",
  "new_connections_total",
  "new_disconnections_total",
  "ws_messages_received_total",
  "ws_messages_sent_total",
  "http_calls_received_total",
] as const;

function unescapePrometheusText(value: string): string | null {
  let result = "";
  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];
    if (char !== "\\") {
      result += char;
      continue;
    }

    const escaped = value[index + 1];
    if (escaped === undefined) return null;
    if (escaped === "n") result += "\n";
    else if (escaped === "\\" || escaped === '"') result += escaped;
    else return null;
    index += 1;
  }
  return result;
}

function parsePrometheusLabels(raw: string): Record<string, string> | null {
  const labels: Record<string, string> = {};
  let index = 0;

  const skipWhitespace = () => {
    while (index < raw.length && /\s/.test(raw[index])) index += 1;
  };

  while (index < raw.length) {
    skipWhitespace();
    if (index >= raw.length) break;

    const nameStart = index;
    if (!/[a-zA-Z_]/.test(raw[index])) return null;
    index += 1;
    while (index < raw.length && /[a-zA-Z0-9_]/.test(raw[index])) {
      index += 1;
    }
    const name = raw.slice(nameStart, index);

    skipWhitespace();
    if (raw[index] !== "=") return null;
    index += 1;
    skipWhitespace();
    if (raw[index] !== '"') return null;
    index += 1;

    let encodedValue = "";
    let closed = false;
    while (index < raw.length) {
      const char = raw[index];
      if (char === '"') {
        closed = true;
        index += 1;
        break;
      }
      if (char === "\\") {
        const escaped = raw[index + 1];
        if (escaped === undefined) return null;
        encodedValue += `\\${escaped}`;
        index += 2;
        continue;
      }
      encodedValue += char;
      index += 1;
    }
    if (!closed || Object.hasOwn(labels, name)) return null;

    const value = unescapePrometheusText(encodedValue);
    if (value === null) return null;
    labels[name] = value;

    skipWhitespace();
    if (index >= raw.length) break;
    if (raw[index] !== ",") return null;
    index += 1;
    skipWhitespace();
    if (index >= raw.length) return null;
  }

  return labels;
}

function splitPrometheusSampleLine(
  line: string,
): { metric: string; value: string; timestamp?: string } | null {
  let inQuotes = false;
  let escaped = false;
  let separator = -1;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (inQuotes) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') inQuotes = false;
      continue;
    }
    if (char === '"') {
      inQuotes = true;
      continue;
    }
    if (/\s/.test(char)) {
      separator = index;
      break;
    }
  }

  if (separator <= 0 || inQuotes || escaped) return null;
  const metric = line.slice(0, separator);
  const fields = line.slice(separator).trim().split(/\s+/);
  if (fields.length < 1 || fields.length > 2) return null;
  return {
    metric,
    value: fields[0],
    ...(fields[1] === undefined ? {} : { timestamp: fields[1] }),
  };
}

function parsePrometheusSample(line: string): MetricSample | null {
  const split = splitPrometheusSampleLine(line);
  if (!split) return null;

  const value = Number(split.value);
  if (!Number.isFinite(value)) return null;

  let timestamp: number | undefined;
  if (split.timestamp !== undefined) {
    if (!/^-?\d+$/.test(split.timestamp)) return null;
    timestamp = Number(split.timestamp);
    if (!Number.isFinite(timestamp)) return null;
  }

  const braceStart = split.metric.indexOf("{");
  let name = split.metric;
  let labels: Record<string, string> = {};
  if (braceStart !== -1) {
    if (!split.metric.endsWith("}")) return null;
    name = split.metric.slice(0, braceStart);
    const parsedLabels = parsePrometheusLabels(
      split.metric.slice(braceStart + 1, -1),
    );
    if (!parsedLabels) return null;
    labels = parsedLabels;
  }

  if (!/^[a-zA-Z_:][a-zA-Z0-9_:]*$/.test(name)) return null;
  return {
    name,
    labels,
    value,
    ...(timestamp === undefined ? {} : { timestamp }),
  };
}

function metricFamilyName(
  sampleName: string,
  metadata: Map<string, MetricMetadata>,
): string {
  if (metadata.has(sampleName)) return sampleName;

  for (const entry of metadata.values()) {
    if (
      (entry.type === "histogram" || entry.type === "summary") &&
      (sampleName === `${entry.name}_sum` ||
        sampleName === `${entry.name}_count` ||
        (entry.type === "histogram" &&
          sampleName === `${entry.name}_bucket`))
    ) {
      return entry.name;
    }
    if (
      entry.type === "counter" &&
      !entry.name.endsWith("_total") &&
      sampleName === `${entry.name}_total`
    ) {
      return entry.name;
    }
  }

  return sampleName;
}

export function parsePrometheusMetrics(text: string): ParsedPrometheusMetrics {
  const samples: MetricSample[] = [];
  const metadata = new Map<string, MetricMetadata>();

  for (const rawLine of text.split("\n")) {
    const line = rawLine.trim();
    if (!line) continue;

    const helpMatch = line.match(/^#\s+HELP\s+(\S+)\s+(.*)$/);
    if (helpMatch) {
      const help = unescapePrometheusText(helpMatch[2]);
      if (help !== null) {
        const current = metadata.get(helpMatch[1]);
        metadata.set(helpMatch[1], {
          name: helpMatch[1],
          ...current,
          help,
        });
      }
      continue;
    }

    const typeMatch = line.match(/^#\s+TYPE\s+(\S+)\s+(\S+)\s*$/);
    if (typeMatch && PROMETHEUS_TYPES.has(typeMatch[2] as PrometheusMetricType)) {
      const current = metadata.get(typeMatch[1]);
      metadata.set(typeMatch[1], {
        name: typeMatch[1],
        ...current,
        type: typeMatch[2] as PrometheusMetricType,
      });
      continue;
    }

    if (line.startsWith("#")) continue;
    const sample = parsePrometheusSample(line);
    if (sample) samples.push(sample);
  }

  const families = new Map<string, MetricFamily>();
  for (const entry of metadata.values()) {
    families.set(entry.name, {
      name: entry.name,
      type: entry.type ?? "untyped",
      ...(entry.help === undefined ? {} : { help: entry.help }),
      samples: [],
    });
  }
  for (const sample of samples) {
    const familyName = metricFamilyName(sample.name, metadata);
    const entry = metadata.get(familyName);
    let family = families.get(familyName);
    if (!family) {
      family = {
        name: familyName,
        type: entry?.type ?? "untyped",
        ...(entry?.help === undefined ? {} : { help: entry.help }),
        samples: [],
      };
      families.set(familyName, family);
    }
    family.samples.push(sample);
  }

  return {
    samples,
    families: [...families.values()],
    scraped_at: new Date().toISOString(),
  };
}

/**
 * Parses Prometheus text into the legacy flat sample array.
 *
 * Keep this wrapper for the existing `/metrics` response shape. New callers
 * that need HELP/TYPE metadata should use `parsePrometheusMetrics`.
 */
export function parsePrometheusText(text: string): MetricSample[] {
  return parsePrometheusMetrics(text).samples;
}

export function discoverPrometheusPrefix(
  samples: MetricSample[],
): string | null {
  for (const suffix of PREFIX_DISCOVERY_SUFFIXES) {
    const sample = samples.find(
      (candidate) => candidate.name.endsWith(suffix),
    );
    if (sample) return sample.name.slice(0, -suffix.length);
  }
  return null;
}

export function summarizeMetrics(samples: MetricSample[]) {
  const metricsPrefix = discoverPrometheusPrefix(samples);
  const samplesBySuffix = (suffix: string) => {
    if (metricsPrefix !== null) {
      const exact = samples.filter(
        (sample) => sample.name === `${metricsPrefix}${suffix}`,
      );
      if (exact.length > 0) return exact;
    }
    return samples.filter((sample) => sample.name.endsWith(suffix));
  };
  const sumBySuffix = (suffix: string) =>
    samplesBySuffix(suffix)
      .reduce((acc, sample) => acc + sample.value, 0);

  const connectedSamples = samplesBySuffix("connected");
  const connectedByApp = new Map<string, number>();
  for (const sample of connectedSamples) {
    const appId = sample.labels.app_id;
    if (!appId) continue;
    connectedByApp.set(
      appId,
      (connectedByApp.get(appId) ?? 0) + sample.value,
    );
  }

  return {
    metrics_prefix: metricsPrefix,
    connected_sockets: connectedSamples.reduce(
      (acc, sample) => acc + sample.value,
      0,
    ),
    new_connections_total: sumBySuffix("new_connections_total"),
    new_disconnections_total: sumBySuffix("new_disconnections_total"),
    ws_messages_received_total: sumBySuffix("ws_messages_received_total"),
    ws_messages_sent_total: sumBySuffix("ws_messages_sent_total"),
    http_calls_received_total: sumBySuffix("http_calls_received_total"),
    channel_subscriptions_total: sumBySuffix("channel_subscriptions_total"),
    channel_unsubscriptions_total: sumBySuffix(
      "channel_unsubscriptions_total",
    ),
    rate_limit_triggered_total: sumBySuffix("rate_limit_triggered_total"),
    connection_errors_total: sumBySuffix("connection_errors_total"),
    history_writes_total: sumBySuffix("history_writes_total"),
    history_write_failures_total: sumBySuffix(
      "history_write_failures_total",
    ),
    by_app: [...connectedByApp.entries()]
      .map(([app_id, connected_sockets]) => ({
        app_id,
        connected_sockets,
      }))
      .sort((left, right) => left.app_id.localeCompare(right.app_id)),
  };
}
