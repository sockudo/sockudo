export interface AppPolicy {
  limits: {
    max_connections: number;
    max_client_events_per_second: number;
    max_backend_events_per_second?: number;
    max_read_requests_per_second?: number;
  };
  features: {
    enable_client_messages: boolean;
    enable_user_authentication?: boolean;
    enable_watchlist_events?: boolean;
  };
  channels: {
    allowed_origins?: string[];
  };
  webhooks?: Webhook[];
}

export interface Webhook {
  url?: string;
  event_types: string[];
  filter?: { channel_prefix?: string; channel_pattern?: string };
  headers?: Record<string, string>;
}

export interface App {
  id: string;
  key: string;
  secret: string;
  enabled: boolean;
  policy: AppPolicy;
  webhook_count?: number;
}

export interface MetricsSummary {
  connected_sockets: number;
  new_connections_total: number;
  new_disconnections_total: number;
  ws_messages_received_total: number;
  ws_messages_sent_total: number;
  http_calls_received_total: number;
  channel_subscriptions_total: number;
  rate_limit_triggered_total: number;
  connection_errors_total: number;
  by_app: Array<{ app_id: string; connected_sockets: number }>;
}

export interface StatsResponse {
  totals: {
    apps: number;
    connections: number;
    users: number;
  };
  apps: Array<{
    app_id: string;
    connections: number;
    users: number;
    occupancy: {
      channels: number;
      subscriptions: number;
    };
  }>;
  memory: { used: number; total: number; percent: number };
}

import type { DashboardUser, UserRole } from "@/types/user";

class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    credentials: "include",
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new ApiError(data.error ?? res.statusText, res.status);
  }
  return data as T;
}

export const api = {
  login: (email: string, password: string) =>
    request<DashboardUser>("/api/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    }),
  logout: () =>
    request<{ ok: boolean }>("/api/v1/auth/logout", { method: "POST" }),
  me: () => request<DashboardUser>("/api/v1/auth/me"),
  listUsers: () => request<DashboardUser[]>("/api/v1/users"),
  createUser: (body: {
    email: string;
    password: string;
    name?: string;
    role?: UserRole;
    active?: boolean;
  }) =>
    request<DashboardUser>("/api/v1/users", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  updateUser: (
    id: string,
    body: Partial<{
      email: string;
      name: string;
      role: UserRole;
      active: boolean;
      password: string;
    }>,
  ) =>
    request<DashboardUser>(`/api/v1/users/${id}`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),
  deleteUser: (id: string) =>
    request<{ ok: boolean }>(`/api/v1/users/${id}`, { method: "DELETE" }),
  changePassword: (
    id: string,
    body: { current_password?: string; new_password: string },
  ) =>
    request<DashboardUser>(`/api/v1/users/${id}/change-password`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  listApps: () => request<App[]>("/api/v1/apps"),
  getApp: (id: string, revealSecret = false) =>
    request<App>(
      `/api/v1/apps/${id}${revealSecret ? "?reveal_secret=true" : ""}`,
    ),
  createApp: (body: {
    id: string;
    key: string;
    secret?: string;
    enabled?: boolean;
    policy?: Partial<AppPolicy>;
  }) =>
    request<App>("/api/v1/apps", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  updateApp: (id: string, body: Partial<App>) =>
    request<App>(`/api/v1/apps/${id}`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),
  deleteApp: (id: string) =>
    request<{ ok: boolean }>(`/api/v1/apps/${id}`, { method: "DELETE" }),
  rotateSecret: (id: string) =>
    request<App>(`/api/v1/apps/${id}/rotate-secret`, { method: "POST" }),
  listWebhooks: (appId: string) =>
    request<Webhook[]>(`/api/v1/apps/${appId}/webhooks`),
  createWebhook: (appId: string, webhook: Webhook) =>
    request<App>(`/api/v1/apps/${appId}/webhooks`, {
      method: "POST",
      body: JSON.stringify(webhook),
    }),
  updateWebhook: (appId: string, index: number, webhook: Webhook) =>
    request<App>(`/api/v1/apps/${appId}/webhooks/${index}`, {
      method: "PUT",
      body: JSON.stringify(webhook),
    }),
  deleteWebhook: (appId: string, index: number) =>
    request<App>(`/api/v1/apps/${appId}/webhooks/${index}`, {
      method: "DELETE",
    }),
  metricsSummary: () =>
    request<MetricsSummary>("/api/v1/ops/metrics/summary"),
  stats: () => request<StatsResponse>("/api/v1/ops/stats"),
  opsConfig: () =>
    request<{
      app_manager_driver: string;
      sockudo_http_url: string;
      sockudo_metrics_url: string;
    }>("/api/v1/ops/config"),
};

export { ApiError };
