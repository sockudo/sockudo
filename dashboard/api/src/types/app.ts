export interface WebhookFilter {
  channel_prefix?: string;
  channel_suffix?: string;
  channel_pattern?: string;
  channel_namespace?: string;
  channel_namespaces?: string[];
}

export interface WebhookRetryPolicy {
  enabled?: boolean;
  max_attempts?: number;
  max_elapsed_time_ms?: number;
  initial_backoff_ms?: number;
  max_backoff_ms?: number;
}

export interface Webhook {
  url?: string;
  lambda_function?: string;
  lambda?: { function_name: string; region: string };
  event_types: string[];
  filter?: WebhookFilter;
  headers?: Record<string, string>;
  retry?: WebhookRetryPolicy;
  request_timeout_ms?: number;
}

export interface AppLimitsPolicy {
  max_connections: number;
  max_backend_events_per_second?: number;
  max_client_events_per_second: number;
  max_read_requests_per_second?: number;
  max_presence_members_per_channel?: number;
  max_presence_member_size_in_kb?: number;
  max_channel_name_length?: number;
  max_event_channels_at_once?: number;
  max_event_name_length?: number;
  max_event_payload_in_kb?: number;
  max_event_batch_size?: number;
  decay_seconds?: number;
  terminate_on_limit?: boolean;
}

export interface AppFeaturesPolicy {
  enable_client_messages: boolean;
  enable_user_authentication?: boolean;
  enable_watchlist_events?: boolean;
}

export interface AppChannelsPolicy {
  allowed_origins?: string[];
  annotations_enabled?: boolean;
  channel_namespaces?: Array<{
    name: string;
    channel_name_pattern: string;
    max_channel_name_length?: number;
    allow_user_limited_channels?: boolean;
    annotations_enabled?: boolean;
  }>;
}

export interface AppPolicy {
  limits: AppLimitsPolicy;
  features: AppFeaturesPolicy;
  channels: AppChannelsPolicy;
  webhooks?: Webhook[];
  idempotency?: { enabled?: boolean; ttl_seconds?: number };
  connection_recovery?: {
    enabled?: boolean;
    buffer_ttl_seconds?: number;
    max_buffer_size?: number;
  };
  history?: {
    enabled?: boolean;
    rewind_enabled?: boolean;
    retention_window_seconds?: number;
    max_messages_per_channel?: number;
    max_bytes_per_channel?: number;
  };
  presence_history?: {
    enabled?: boolean;
    retention_window_seconds?: number;
    max_events_per_channel?: number;
    max_bytes_per_channel?: number;
  };
}

export interface AppRecord {
  id: string;
  key: string;
  secret: string;
  enabled: boolean;
  policy: AppPolicy;
}

export interface AppCreateInput {
  id: string;
  key: string;
  secret: string;
  enabled?: boolean;
  policy?: Partial<AppPolicy>;
}

export interface AppUpdateInput {
  key?: string;
  secret?: string;
  enabled?: boolean;
  policy?: Partial<AppPolicy>;
}

export const DEFAULT_POLICY: AppPolicy = {
  limits: {
    max_connections: 10_000,
    max_client_events_per_second: 1_000,
    max_backend_events_per_second: 1_000,
    max_read_requests_per_second: 100,
  },
  features: {
    enable_client_messages: false,
    enable_user_authentication: false,
    enable_watchlist_events: false,
  },
  channels: {
    allowed_origins: ["*"],
  },
  webhooks: [],
};

export const WEBHOOK_EVENT_TYPES = [
  "channel_occupied",
  "channel_vacated",
  "subscription_count",
  "member_added",
  "member_removed",
  "member_updated",
  "client_event",
  "cache_miss",
  "ai_turn_started",
  "ai_turn_ended",
  "ai_cancel_requested",
  "ai_stream_orphaned",
  "message_version_created",
  "annotation_created",
  "annotation_deleted",
] as const;

export function mergePolicy(partial?: Partial<AppPolicy>): AppPolicy {
  if (!partial) return structuredClone(DEFAULT_POLICY);
  return {
    limits: { ...DEFAULT_POLICY.limits, ...partial.limits },
    features: { ...DEFAULT_POLICY.features, ...partial.features },
    channels: { ...DEFAULT_POLICY.channels, ...partial.channels },
    webhooks: partial.webhooks ?? DEFAULT_POLICY.webhooks,
    idempotency: partial.idempotency,
    connection_recovery: partial.connection_recovery,
    history: partial.history,
    presence_history: partial.presence_history,
  };
}

export function maskSecret(secret: string): string {
  if (secret.length <= 8) return "********";
  return `${secret.slice(0, 4)}${"*".repeat(Math.min(secret.length - 8, 12))}${secret.slice(-4)}`;
}

export function toPublicApp(app: AppRecord, revealSecret = false) {
  return {
    id: app.id,
    key: app.key,
    secret: revealSecret ? app.secret : maskSecret(app.secret),
    enabled: app.enabled,
    policy: app.policy,
    webhook_count: app.policy.webhooks?.length ?? 0,
  };
}
