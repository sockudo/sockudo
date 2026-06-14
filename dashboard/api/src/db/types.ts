import type {
  AppCreateInput,
  AppPolicy,
  AppRecord,
  AppUpdateInput,
  Webhook,
} from "../types/app.ts";

export interface AppsRepository {
  list(): Promise<AppRecord[]>;
  findById(id: string): Promise<AppRecord | null>;
  create(input: AppCreateInput): Promise<AppRecord>;
  update(id: string, input: AppUpdateInput): Promise<AppRecord>;
  delete(id: string): Promise<void>;
  close(): Promise<void>;
}

export interface FlatColumns {
  max_connections: number;
  enable_client_messages: boolean;
  max_backend_events_per_second: number | null;
  max_client_events_per_second: number;
  max_read_requests_per_second: number | null;
  max_presence_members_per_channel: number | null;
  max_presence_member_size_in_kb: number | null;
  max_channel_name_length: number | null;
  max_event_channels_at_once: number | null;
  max_event_name_length: number | null;
  max_event_payload_in_kb: number | null;
  max_event_batch_size: number | null;
  enable_user_authentication: boolean | null;
  enable_watchlist_events: boolean | null;
}

export function policyToFlat(policy: AppPolicy): FlatColumns {
  return {
    max_connections: policy.limits.max_connections,
    enable_client_messages: policy.features.enable_client_messages,
    max_backend_events_per_second:
      policy.limits.max_backend_events_per_second ?? null,
    max_client_events_per_second: policy.limits.max_client_events_per_second,
    max_read_requests_per_second:
      policy.limits.max_read_requests_per_second ?? null,
    max_presence_members_per_channel:
      policy.limits.max_presence_members_per_channel ?? null,
    max_presence_member_size_in_kb:
      policy.limits.max_presence_member_size_in_kb ?? null,
    max_channel_name_length: policy.limits.max_channel_name_length ?? null,
    max_event_channels_at_once: policy.limits.max_event_channels_at_once ?? null,
    max_event_name_length: policy.limits.max_event_name_length ?? null,
    max_event_payload_in_kb: policy.limits.max_event_payload_in_kb ?? null,
    max_event_batch_size: policy.limits.max_event_batch_size ?? null,
    enable_user_authentication:
      policy.features.enable_user_authentication ?? null,
    enable_watchlist_events: policy.features.enable_watchlist_events ?? null,
  };
}

export function rowToApp(row: {
  id: string;
  key: string;
  secret: string;
  enabled: boolean;
  policy: AppPolicy | null;
  webhooks?: Webhook[] | null;
  allowed_origins?: string[] | null;
  enable_client_messages?: boolean;
  max_connections?: number;
  max_client_events_per_second?: number;
  max_backend_events_per_second?: number | null;
  max_read_requests_per_second?: number | null;
  enable_user_authentication?: boolean | null;
  enable_watchlist_events?: boolean | null;
}): AppRecord {
  if (row.policy) {
    return {
      id: row.id,
      key: row.key,
      secret: row.secret,
      enabled: row.enabled,
      policy: row.policy,
    };
  }

  return {
    id: row.id,
    key: row.key,
    secret: row.secret,
    enabled: row.enabled,
    policy: {
      limits: {
        max_connections: row.max_connections ?? 10_000,
        max_client_events_per_second: row.max_client_events_per_second ?? 1_000,
        max_backend_events_per_second:
          row.max_backend_events_per_second ?? undefined,
        max_read_requests_per_second:
          row.max_read_requests_per_second ?? undefined,
      },
      features: {
        enable_client_messages: row.enable_client_messages ?? false,
        enable_user_authentication: row.enable_user_authentication ?? undefined,
        enable_watchlist_events: row.enable_watchlist_events ?? undefined,
      },
      channels: {
        allowed_origins: row.allowed_origins ?? ["*"],
      },
      webhooks: row.webhooks ?? [],
    },
  };
}
