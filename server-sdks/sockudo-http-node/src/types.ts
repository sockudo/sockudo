import type { Agent } from "http";
import type { Response } from "node-fetch";

export interface BaseOptions {
  appId: string | number;
  key: string;
  secret: string;
  port?: string | number;
  useTLS?: boolean;
  encrypted?: boolean;
  timeout?: number;
  agent?: Agent;
  encryptionMasterKey?: string;
  encryptionMasterKeyBase64?: string;
  autoIdempotencyKey?: boolean;
  maxRetries?: number;
  scheme?: string;
}

export interface ClusterOptions extends BaseOptions {
  cluster: string;
}

export interface HostOptions extends BaseOptions {
  host: string;
  port?: string | number;
}

export type Options = ClusterOptions | HostOptions;

export type IdempotencyKey = string | true;

export interface TriggerParams {
  socket_id?: string;
  info?: string;
  idempotency_key?: IdempotencyKey;
}

export interface BatchEvent extends TriggerParams {
  channel: string;
  name: string;
  data: unknown;
}

export interface RequestParams {
  [key: string]: unknown;
}

export interface RequestOptions {
  path: string;
  params?: RequestParams;
  headers?: Record<string, string>;
}

export interface GetOptions extends RequestOptions {}

export type PushCapability = "push-admin" | "push-subscribe";

export interface PushCursorParams extends RequestParams {
  limit?: number;
  cursor?: string;
}

export interface PushListResponse<T> {
  items: T[];
  next_cursor?: string | null;
  has_more: boolean;
}

export type PushProviderKind = "fcm" | "apns" | "webPush" | "hms" | "wns";

export type PushRecipient =
  | { transportType: "gcm"; registrationToken: string }
  | { transportType: "apns"; deviceToken: string }
  | { transportType: "web"; endpoint: string; p256dh: string; auth: string }
  | { transportType: "hms"; registrationToken: string }
  | { transportType: "wns"; channelUri: string };

export interface PushDeviceDetails {
  appId?: string;
  id: string;
  clientId?: string;
  formFactor: string;
  platform: string;
  metadata?: Record<string, unknown>;
  deviceSecret?: string;
  timezone: string;
  locale: string;
  lastActiveAtMs?: number;
  push: {
    recipient: PushRecipient;
    state?: string;
    failureCount?: number;
    errorReason?: string;
  };
  pushRatePolicy?: {
    capacity: number;
    refillPerSecond: number;
  };
}

export interface PushDeviceRegistrationResponse {
  change: string;
  tokenHash: string;
  device: Record<string, unknown>;
  deviceIdentityToken?: string;
}

export interface PushChannelSubscription {
  appId?: string;
  channel: string;
  deviceId: string;
  clientId?: string;
  provider: PushProviderKind;
  tokenHash: string;
  credentialVersion: number;
}

export interface PushPayload {
  templateId?: string;
  templateData?: Record<string, unknown>;
  title?: string;
  body?: string;
  icon?: string;
  sound?: string;
  collapseKey?: string;
}

export type PushPublishTarget =
  | { type: "device"; device_id?: string; deviceId?: string }
  | { type: "client"; client_id?: string; clientId?: string }
  | { type: "channel"; channel: string }
  | { type: "registeredTopic"; topic: string }
  | { type: "userTopic"; topic: string }
  | { type: "recipient"; recipient: PushRecipient }
  | { type: "providerTopic"; provider: PushProviderKind; topic: string }
  | { type: "providerCondition"; provider: PushProviderKind; condition: string }
  | { type: "indexedFilter"; filter: Record<string, unknown> };

export interface PushProviderOverridePayload {
  provider: PushProviderKind;
  payload: Record<string, unknown>;
}

export interface PushPublishRequest {
  publishId?: string;
  recipients: PushPublishTarget[];
  payload: PushPayload;
  providerOverrides?: PushProviderOverridePayload[];
  sync?: false;
  notBeforeMs?: number;
  expiresAtMs?: number;
}

export interface PushPublishAcceptedResponse {
  publish_id?: string;
  publishId?: string;
  status: string;
  expectedRecipients: number;
  fanoutRegime: string;
  renderedPayloads?: Array<Record<string, unknown>>;
}

export interface PushDeliveryStatusEvent {
  appId?: string;
  publishId: string;
  eventId: string;
  occurredAtMs: number;
  result: Record<string, unknown>;
}

export interface HistoryParams extends RequestParams {
  limit?: number;
  direction?: string;
  cursor?: string;
  start_serial?: number;
  end_serial?: number;
  start_time_ms?: number;
  end_time_ms?: number;
}

export interface HistoryItem {
  stream_id?: string | null;
  serial?: number | null;
  published_at_ms?: number | null;
  message_id?: string | null;
  event_name?: string | null;
  operation_kind?: string | null;
  payload_size_bytes?: number | null;
  message?: Record<string, unknown> | null;
}

export interface HistoryBounds {
  start_serial?: number | null;
  end_serial?: number | null;
  start_time_ms?: number | null;
  end_time_ms?: number | null;
}

export interface HistoryContinuity {
  stream_id?: string | null;
  oldest_available_serial?: number | null;
  newest_available_serial?: number | null;
  oldest_available_published_at_ms?: number | null;
  newest_available_published_at_ms?: number | null;
  retained_messages: number;
  retained_bytes: number;
  complete: boolean;
  truncated_by_retention: boolean;
}

export interface HistoryPage {
  items: HistoryItem[];
  direction: string;
  limit: number;
  has_more: boolean;
  next_cursor?: string | null;
  bounds: HistoryBounds;
  continuity: HistoryContinuity;
  stream_state?: Record<string, unknown>;
}

export type HistoryDirection = "newest_first" | "oldest_first";

export interface PresenceHistoryParams extends HistoryParams {
  direction?: HistoryDirection;
  /** Ably-compatible alias for start_time_ms */
  start?: number;
  /** Ably-compatible alias for end_time_ms */
  end?: number;
}

export type PresenceHistoryEventKind = "member_added" | "member_removed";
export type PresenceHistoryEventCause =
  | "join"
  | "disconnect"
  | "orphan_cleanup"
  | "timeout"
  | "forced_disconnect";

export interface PresenceHistoryEventPayload {
  stream_id: string;
  serial: number;
  published_at_ms: number;
  event: PresenceHistoryEventKind;
  cause: PresenceHistoryEventCause;
  user_id: string;
  connection_id?: string | null;
  user_info?: Record<string, unknown> | null;
  dead_node_id?: string | null;
}

export interface PresenceHistoryItem {
  stream_id: string;
  serial: number;
  published_at_ms: number;
  event: PresenceHistoryEventKind;
  cause: PresenceHistoryEventCause;
  user_id: string;
  connection_id?: string | null;
  dead_node_id?: string | null;
  payload_size_bytes: number;
  presence_event: PresenceHistoryEventPayload;
}

export interface PresenceHistoryBounds {
  start_serial?: number | null;
  end_serial?: number | null;
  start_time_ms?: number | null;
  end_time_ms?: number | null;
}

export interface PresenceHistoryContinuity {
  stream_id?: string | null;
  oldest_available_serial?: number | null;
  newest_available_serial?: number | null;
  oldest_available_published_at_ms?: number | null;
  newest_available_published_at_ms?: number | null;
  retained_events: number;
  retained_bytes: number;
  degraded: boolean;
  complete: boolean;
  truncated_by_retention: boolean;
}

export interface PresenceHistoryPage {
  items: PresenceHistoryItem[];
  direction: HistoryDirection;
  limit: number;
  has_more: boolean;
  next_cursor?: string | null;
  bounds: PresenceHistoryBounds;
  continuity: PresenceHistoryContinuity;
}

export interface PresenceSnapshotParams extends RequestParams {
  at_time_ms?: number;
  /** Ably-compatible alias for at_time_ms */
  at?: number;
  at_serial?: number;
}

export interface PresenceSnapshotMember {
  user_id: string;
  last_event: PresenceHistoryEventKind;
  last_event_serial: number;
  last_event_at_ms: number;
}

export interface PresenceSnapshot {
  channel: string;
  members: PresenceSnapshotMember[];
  member_count: number;
  events_replayed: number;
  snapshot_serial?: number | null;
  snapshot_time_ms?: number | null;
  continuity: PresenceHistoryContinuity;
}

export interface MessageVersionInfo {
  serial: string;
  timestamp_ms: number;
  client_id?: string | null;
  description?: string | null;
  metadata?: Record<string, unknown> | null;
}

export interface VersionedRealtimeMessage {
  event?: string;
  channel?: string;
  data?: unknown;
  name?: string;
  serial?: number;
  action: "create" | "update" | "delete" | "append" | "summary";
  message_serial: string;
  history_serial?: number | null;
  delivery_serial?: number | null;
  version?: MessageVersionInfo | null;
  extras?: Record<string, unknown>;
}

export interface GetMessageResponse {
  channel: string;
  item: VersionedRealtimeMessage;
}

export interface ListMessageVersionsResponse {
  channel: string;
  direction: string;
  limit: number;
  has_more: boolean;
  next_cursor?: string | null;
  items: VersionedRealtimeMessage[];
}

export interface MutationResponse {
  channel: string;
  message_serial: string;
  action: "create" | "update" | "delete" | "append" | "summary";
  accepted: boolean;
  version_serial?: string | null;
  history_serial?: number | string | null;
  delivery_serial?: number | string | null;
  status: string;
}

export interface MessageVersionsParams extends RequestParams {
  limit?: number;
  direction?: string;
  cursor?: string;
}

export interface PublishAnnotationBody {
  type: string;
  name?: string;
  clientId?: string;
  socketId?: string;
  count?: number;
  data?: unknown;
  encoding?: string;
}

export interface PublishAnnotationResponse {
  annotationSerial: string;
}

export interface DeleteAnnotationResponse {
  annotationSerial: string;
  deletedAnnotationSerial: string;
}

export interface AnnotationEventsParams extends RequestParams {
  type?: string;
  from_serial?: string;
  limit?: number;
  socket_id?: string;
}

export interface AnnotationEvent {
  action: "annotation.create" | "annotation.delete";
  id?: string | null;
  serial: string;
  messageSerial: string;
  type: string;
  name?: string | null;
  clientId?: string | null;
  count?: number | null;
  data?: unknown;
  encoding?: string | null;
  timestamp?: number | null;
}

export interface AnnotationEventsResponse {
  channel: string;
  messageSerial: string;
  limit: number;
  hasMore: boolean;
  nextCursor?: string | null;
  items: AnnotationEvent[];
}

export interface PostOptions extends RequestOptions {
  body: unknown;
}

export interface SignedQueryStringOptions {
  method: string;
  path: string;
  body?: string;
  params?: RequestParams;
}

export interface ChannelAuthResponse {
  auth: string;
  channel_data?: string;
  shared_secret?: string;
}

export interface UserAuthResponse {
  auth: string;
  user_data: string;
}

export interface PresenceChannelData {
  user_id: string;
  user_info?: Record<string, unknown>;
}

export interface UserChannelData {
  id: string;
  [key: string]: unknown;
}

export interface WebHookRequest {
  headers: Record<string, string | undefined>;
  rawBody: string;
}

export interface WebHookEvent {
  name: string;
  channel?: string;
  event?: string;
  data?: unknown;
  socket_id?: string;
  user_id?: string;
  [key: string]: unknown;
}

export interface WebHookData {
  time_ms: number;
  events: WebHookEvent[];
}

export interface TokenShape {
  key: string;
  secret: string;
}

export type ResponseWithIdempotency = Response & {
  idempotencyKey?: IdempotencyKey;
  idempotencyKeys?: IdempotencyKey[];
};

export interface SignedRequest {
  method: string;
  path: string;
  params?: RequestParams;
  body?: string;
}
