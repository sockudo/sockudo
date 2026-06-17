import postgres from "postgres";
import { config } from "../config.ts";
import {
  mergePolicy,
  type AppCreateInput,
  type AppPolicy,
  type AppRecord,
  type AppUpdateInput,
} from "../types/app.ts";
import {
  type AppsRepository,
  policyToFlat,
  rowToApp,
} from "./types.ts";

export class PostgresAppsRepository implements AppsRepository {
  private sql: ReturnType<typeof postgres>;
  private table: string;

  constructor() {
    const pg = config.database.postgres;
    this.table = pg.table;
    this.sql = postgres({
      host: pg.host,
      port: pg.port,
      user: pg.user,
      password: pg.password,
      database: pg.database,
      max: 10,
    });
  }

  async list(): Promise<AppRecord[]> {
    const rows = await this.sql.unsafe(`
      SELECT id, key, secret, enabled, policy, webhooks, allowed_origins,
             max_connections, enable_client_messages,
             max_backend_events_per_second, max_client_events_per_second,
             max_read_requests_per_second, enable_user_authentication,
             enable_watchlist_events
      FROM ${this.table}
      ORDER BY id ASC
    `);
    return rows.map((row) =>
      rowToApp({
        id: row.id as string,
        key: row.key as string,
        secret: row.secret as string,
        enabled: row.enabled as boolean,
        policy: (row.policy as AppPolicy | null) ?? null,
        webhooks: (row.webhooks as AppPolicy["webhooks"]) ?? null,
        allowed_origins: (row.allowed_origins as string[] | null) ?? null,
        max_connections: row.max_connections as number,
        enable_client_messages: row.enable_client_messages as boolean,
        max_client_events_per_second: row.max_client_events_per_second as number,
        max_backend_events_per_second:
          row.max_backend_events_per_second as number | null,
        max_read_requests_per_second:
          row.max_read_requests_per_second as number | null,
        enable_user_authentication:
          row.enable_user_authentication as boolean | null,
        enable_watchlist_events: row.enable_watchlist_events as boolean | null,
      }),
    );
  }

  async findById(id: string): Promise<AppRecord | null> {
    const rows = await this.sql.unsafe(
      `
      SELECT id, key, secret, enabled, policy, webhooks, allowed_origins,
             max_connections, enable_client_messages,
             max_backend_events_per_second, max_client_events_per_second,
             max_read_requests_per_second, enable_user_authentication,
             enable_watchlist_events
      FROM ${this.table}
      WHERE id = $1
    `,
      [id],
    );
    if (rows.length === 0) return null;
    const row = rows[0];
    return rowToApp({
      id: row.id as string,
      key: row.key as string,
      secret: row.secret as string,
      enabled: row.enabled as boolean,
      policy: (row.policy as AppPolicy | null) ?? null,
      webhooks: (row.webhooks as AppPolicy["webhooks"]) ?? null,
      allowed_origins: (row.allowed_origins as string[] | null) ?? null,
      max_connections: row.max_connections as number,
      enable_client_messages: row.enable_client_messages as boolean,
      max_client_events_per_second: row.max_client_events_per_second as number,
      max_backend_events_per_second:
        row.max_backend_events_per_second as number | null,
      max_read_requests_per_second:
        row.max_read_requests_per_second as number | null,
      enable_user_authentication:
        row.enable_user_authentication as boolean | null,
      enable_watchlist_events: row.enable_watchlist_events as boolean | null,
    });
  }

  async create(input: AppCreateInput): Promise<AppRecord> {
    const policy = mergePolicy(input.policy);
    const flat = policyToFlat(policy);
    const enabled = input.enabled ?? true;

    await this.sql.unsafe(
      `
      INSERT INTO ${this.table} (
        id, key, secret, max_connections, enable_client_messages, enabled,
        max_backend_events_per_second, max_client_events_per_second,
        max_read_requests_per_second, max_presence_members_per_channel,
        max_presence_member_size_in_kb, max_channel_name_length,
        max_event_channels_at_once, max_event_name_length,
        max_event_payload_in_kb, max_event_batch_size,
        enable_user_authentication, enable_watchlist_events,
        policy, webhooks, allowed_origins
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        $17, $18, $19::jsonb, $20::jsonb, $21::jsonb
      )
    `,
      [
        input.id,
        input.key,
        input.secret,
        flat.max_connections,
        flat.enable_client_messages,
        enabled,
        flat.max_backend_events_per_second,
        flat.max_client_events_per_second,
        flat.max_read_requests_per_second,
        flat.max_presence_members_per_channel,
        flat.max_presence_member_size_in_kb,
        flat.max_channel_name_length,
        flat.max_event_channels_at_once,
        flat.max_event_name_length,
        flat.max_event_payload_in_kb,
        flat.max_event_batch_size,
        flat.enable_user_authentication,
        flat.enable_watchlist_events,
        JSON.stringify(policy),
        JSON.stringify(policy.webhooks ?? []),
        JSON.stringify(policy.channels.allowed_origins ?? ["*"]),
      ],
    );

    const created = await this.findById(input.id);
    if (!created) throw new Error("Failed to create app");
    return created;
  }

  async update(id: string, input: AppUpdateInput): Promise<AppRecord> {
    const existing = await this.findById(id);
    if (!existing) throw new Error("App not found");

    const policy = mergePolicy({
      ...existing.policy,
      ...input.policy,
      limits: { ...existing.policy.limits, ...input.policy?.limits },
      features: { ...existing.policy.features, ...input.policy?.features },
      channels: { ...existing.policy.channels, ...input.policy?.channels },
      webhooks: input.policy?.webhooks ?? existing.policy.webhooks,
    });
    const flat = policyToFlat(policy);
    const key = input.key ?? existing.key;
    const secret = input.secret ?? existing.secret;
    const enabled = input.enabled ?? existing.enabled;

    const result = await this.sql.unsafe(
      `
      UPDATE ${this.table} SET
        key = $1, secret = $2, max_connections = $3, enable_client_messages = $4,
        enabled = $5, max_backend_events_per_second = $6,
        max_client_events_per_second = $7, max_read_requests_per_second = $8,
        max_presence_members_per_channel = $9, max_presence_member_size_in_kb = $10,
        max_channel_name_length = $11, max_event_channels_at_once = $12,
        max_event_name_length = $13, max_event_payload_in_kb = $14,
        max_event_batch_size = $15, enable_user_authentication = $16,
        enable_watchlist_events = $17, policy = $18::jsonb, webhooks = $19::jsonb,
        allowed_origins = $20::jsonb, updated_at = CURRENT_TIMESTAMP
      WHERE id = $21
    `,
      [
        key,
        secret,
        flat.max_connections,
        flat.enable_client_messages,
        enabled,
        flat.max_backend_events_per_second,
        flat.max_client_events_per_second,
        flat.max_read_requests_per_second,
        flat.max_presence_members_per_channel,
        flat.max_presence_member_size_in_kb,
        flat.max_channel_name_length,
        flat.max_event_channels_at_once,
        flat.max_event_name_length,
        flat.max_event_payload_in_kb,
        flat.max_event_batch_size,
        flat.enable_user_authentication,
        flat.enable_watchlist_events,
        JSON.stringify(policy),
        JSON.stringify(policy.webhooks ?? []),
        JSON.stringify(policy.channels.allowed_origins ?? ["*"]),
        id,
      ],
    );

    if (result.count === 0) throw new Error("App not found");
    const updated = await this.findById(id);
    if (!updated) throw new Error("Failed to update app");
    return updated;
  }

  async delete(id: string): Promise<void> {
    const result = await this.sql.unsafe(
      `DELETE FROM ${this.table} WHERE id = $1`,
      [id],
    );
    if (result.count === 0) throw new Error("App not found");
  }

  async close(): Promise<void> {
    await this.sql.end();
  }
}
