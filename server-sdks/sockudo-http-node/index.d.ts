import { Agent } from "http";
import { Response } from "node-fetch";

export = Sockudo;

declare class Sockudo {
  constructor(opts: Sockudo.Options);

  trigger(
    channel: string | Array<string>,
    event: string,
    data: any,
    params?: Sockudo.TriggerParams,
  ): Promise<Response>;

  trigger(channel: string | Array<string>, event: string, data: any): Promise<Response>;

  triggerBatch(events: Array<Sockudo.BatchEvent>): Promise<Response>;

  get(opts: Sockudo.GetOptions): Promise<Response>;
  delete(opts: Sockudo.GetOptions): Promise<Response>;
  channelHistory(channel: string, params?: Sockudo.HistoryParams): Promise<Sockudo.HistoryPage>;
  getMessage(channel: string, messageSerial: string): Promise<Sockudo.GetMessageResponse>;
  getMessageVersions(
    channel: string,
    messageSerial: string,
    params?: Sockudo.MessageVersionsParams,
  ): Promise<Sockudo.ListMessageVersionsResponse>;
  updateMessage(
    channel: string,
    messageSerial: string,
    body: Record<string, unknown>,
  ): Promise<Sockudo.MutationResponse>;
  deleteMessage(
    channel: string,
    messageSerial: string,
    body?: Record<string, unknown>,
  ): Promise<Sockudo.MutationResponse>;
  appendMessage(
    channel: string,
    messageSerial: string,
    body: { data: string; [key: string]: unknown },
  ): Promise<Sockudo.MutationResponse>;
  publishAnnotation(
    channel: string,
    messageSerial: string,
    body: Sockudo.PublishAnnotationBody,
  ): Promise<Sockudo.PublishAnnotationResponse>;
  deleteAnnotation(
    channel: string,
    messageSerial: string,
    annotationSerial: string,
    params?: { socket_id?: string },
  ): Promise<Sockudo.DeleteAnnotationResponse>;
  listAnnotations(
    channel: string,
    messageSerial: string,
    params?: Sockudo.AnnotationEventsParams,
  ): Promise<Sockudo.AnnotationEventsResponse>;
  channelPresenceHistory(
    channel: string,
    params?: Sockudo.PresenceHistoryParams,
  ): Promise<Sockudo.PresenceHistoryPage>;
  post(opts: Sockudo.PostOptions): Promise<Response>;

  /**
   * @deprecated Use authorizeChannel
   */
  authenticate(
    socketId: string,
    channel: string,
    data?: Sockudo.PresenceChannelData,
  ): Sockudo.ChannelAuthResponse;

  authorizeChannel(
    socketId: string,
    channel: string,
    data?: Sockudo.PresenceChannelData,
  ): Sockudo.ChannelAuthResponse;

  authenticateUser(socketId: string, userData: Sockudo.UserChannelData): Sockudo.UserAuthResponse;

  sendToUser(userId: string, event: string, data: any): Promise<Response>;

  terminateUserConnections(userId: string): Promise<Response>;

  activateDevice(
    device: Sockudo.PushDeviceDetails,
    options?: { rotateDeviceIdentityToken?: boolean },
  ): Promise<Sockudo.PushDeviceRegistrationResponse>;
  createDeviceActivation(
    device: Sockudo.PushDeviceDetails,
  ): Promise<Sockudo.PushDeviceRegistrationResponse>;
  updateDeviceRegistration(
    device: Sockudo.PushDeviceDetails,
    deviceIdentityToken: string,
  ): Promise<Sockudo.PushDeviceRegistrationResponse>;
  listDeviceRegistrations(
    params?: Sockudo.PushCursorParams,
  ): Promise<Sockudo.PushListResponse<Record<string, unknown>>>;
  getDeviceRegistration(
    deviceId: string,
    deviceIdentityToken?: string,
  ): Promise<Record<string, unknown>>;
  deleteDeviceRegistration(deviceId: string, deviceIdentityToken?: string): Promise<Response>;
  removeDeviceRegistrationsByClient(clientId: string): Promise<Record<string, unknown>>;
  upsertChannelPushSubscription(
    subscription: Sockudo.PushChannelSubscription,
    deviceIdentityToken?: string,
  ): Promise<Sockudo.PushChannelSubscription>;
  listChannelPushSubscriptions(
    params?: Sockudo.PushSubscriptionParams,
    deviceIdentityToken?: string,
  ): Promise<Sockudo.PushListResponse<Sockudo.PushChannelSubscription>>;
  deleteChannelPushSubscriptions(
    params: { channel?: string; deviceId?: string },
    deviceIdentityToken?: string,
  ): Promise<Record<string, unknown>>;
  listChannelPushSubscriptionChannels(
    params?: Sockudo.PushCursorParams,
  ): Promise<Sockudo.PushListResponse<string>>;
  listPushCredentials(
    params?: Sockudo.PushCursorParams,
  ): Promise<Sockudo.PushListResponse<Record<string, unknown>>>;
  putPushCredential(
    provider: "fcm" | "apns" | "webpush" | "hms" | "wns",
    credential: Record<string, unknown>,
  ): Promise<Record<string, unknown>>;
  publishPush(request: Sockudo.PushPublishRequest): Promise<Sockudo.PushPublishAcceptedResponse>;
  publishPushDirect(
    request: Sockudo.PushPublishRequest,
  ): Promise<Sockudo.PushPublishAcceptedResponse>;
  publishPushBatch(
    requests: Sockudo.PushPublishRequest[],
  ): Promise<{ items: Sockudo.PushPublishAcceptedResponse[] }>;
  schedulePush(
    request: Sockudo.PushPublishRequest & { notBeforeMs: number },
  ): Promise<Sockudo.PushPublishAcceptedResponse>;
  getPublishStatus(publishId: string): Promise<Record<string, unknown>>;
  cancelScheduledPush(publishId: string): Promise<Response>;
  postPushDeliveryStatus(event: Sockudo.PushDeliveryStatusEvent): Promise<Record<string, unknown>>;

  webhook(request: Sockudo.WebHookRequest): Sockudo.WebHook;
  createSignedQueryString(opts: Sockudo.SignedQueryStringOptions): string;
}

declare namespace Sockudo {
  export function forCluster(cluster: string, opts: BaseOptions): Sockudo;
  export function forURL(connectionString: string, opts?: Partial<Options>): Sockudo;

  export interface BaseOptions {
    appId: string;
    key: string;
    secret: string;
    useTLS?: boolean;
    encrypted?: boolean;
    timeout?: number;
    agent?: Agent;
    encryptionMasterKeyBase64?: string;
    autoIdempotencyKey?: boolean;
  }
  interface ClusterOptions extends BaseOptions {
    cluster: string;
  }
  interface HostOptions extends BaseOptions {
    host: string;
    port?: string;
  }

  export type Options = ClusterOptions | HostOptions;

  export interface TriggerParams {
    socket_id?: string;
    info?: string;
    idempotency_key?: string | true;
  }

  export interface BatchEvent {
    channel: string;
    name: string;
    data: any;
    socket_id?: string;
    info?: string;
    idempotency_key?: string | true;
  }

  type ReservedParams =
    | "auth_key"
    | "auth_timestamp"
    | "auth_version"
    | "auth_signature"
    | "body_md5";

  // I can't help but feel that this is a bit of a hack, but it seems to be the
  // best way of defining a type which allows any key except some known set.
  // Relies on the observation that if a reserved key is provided, it must fit
  // the RHS of the intersection, and have type `never`.
  //
  // https://stackoverflow.com/a/58594586
  export type Params = { [key: string]: any } & {
    [K in ReservedParams]?: never;
  };

  export interface RequestOptions {
    path: string;
    params?: Params;
    headers?: Record<string, string>;
  }
  export type GetOptions = RequestOptions;

  export interface PushCursorParams extends Params {
    limit?: number;
    cursor?: string;
  }
  export interface PushSubscriptionParams extends PushCursorParams {
    channel?: string;
    deviceId?: string;
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
    | { type: "device"; deviceId: string }
    | { type: "client"; clientId: string }
    | { type: "channel"; channel: string }
    | { type: "registeredTopic"; topic: string }
    | { type: "userTopic"; topic: string }
    | { type: "recipient"; recipient: PushRecipient }
    | { type: "providerTopic"; provider: PushProviderKind; topic: string }
    | {
        type: "providerCondition";
        provider: PushProviderKind;
        condition: string;
      };
  export interface PushPublishRequest {
    publishId?: string;
    recipients: PushPublishTarget[];
    payload: PushPayload;
    providerOverrides?: Array<{
      provider: PushProviderKind;
      payload: Record<string, unknown>;
    }>;
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

  export interface HistoryParams extends Params {
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
    message?: { [key: string]: unknown } | null;
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
    items: Array<HistoryItem>;
    direction: string;
    limit: number;
    has_more: boolean;
    next_cursor?: string | null;
    bounds: HistoryBounds;
    continuity: HistoryContinuity;
    stream_state?: { [key: string]: unknown };
  }
  export interface MessageVersionsParams extends Params {
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
    data?: any;
    encoding?: string;
  }

  export interface PublishAnnotationResponse {
    annotationSerial: string;
  }

  export interface DeleteAnnotationResponse {
    annotationSerial: string;
    deletedAnnotationSerial: string;
  }

  export interface AnnotationEventsParams extends Params {
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
    data?: any;
    encoding?: string | null;
    timestamp?: number | null;
  }

  export interface AnnotationEventsResponse {
    channel: string;
    messageSerial: string;
    limit: number;
    hasMore: boolean;
    nextCursor?: string | null;
    items: Array<AnnotationEvent>;
  }

  export type HistoryDirection = "newest_first" | "oldest_first";

  export interface PresenceHistoryParams extends HistoryParams {
    direction?: HistoryDirection;
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
    user_info?: { [key: string]: any } | null;
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
    complete: boolean;
    truncated_by_retention: boolean;
  }

  export interface PresenceHistoryPage {
    items: Array<PresenceHistoryItem>;
    direction: HistoryDirection;
    limit: number;
    has_more: boolean;
    next_cursor?: string | null;
    bounds: PresenceHistoryBounds;
    continuity: PresenceHistoryContinuity;
  }
  export interface MessageVersionInfo {
    serial: string;
    timestamp_ms: number;
    client_id?: string | null;
    description?: string | null;
    metadata?: { [key: string]: unknown } | null;
  }
  export interface VersionedRealtimeMessage {
    event?: string;
    channel?: string;
    data?: any;
    name?: string;
    serial?: number;
    action: "create" | "update" | "delete" | "append" | "summary";
    message_serial: string;
    history_serial?: number | null;
    delivery_serial?: number | null;
    version?: MessageVersionInfo | null;
    extras?: { [key: string]: unknown };
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
    items: Array<VersionedRealtimeMessage>;
  }
  export interface MutationResponse {
    channel: string;
    message_serial: string;
    action: "create" | "update" | "delete" | "append" | "summary";
    accepted: boolean;
    version_serial?: string | null;
    status: string;
  }
  export interface PostOptions extends RequestOptions {
    body: string;
  }
  export interface SignedQueryStringOptions {
    method: string;
    path: string;
    body?: string;
    params?: Params;
  }

  /**
   * @deprecated Use ChannelAuthResponse
   */
  export interface AuthResponse {
    auth: string;
    channel_data?: string;
    shared_secret?: string;
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
    user_info?: {
      [key: string]: any;
    };
  }

  export interface UserChannelData {
    id: string;
    [key: string]: any;
  }

  export interface WebHookRequest {
    headers: object;
    rawBody: string;
  }

  interface Event {
    name: string;
    channel: string;
    event: string;
    data: string;
    socket_id: string;
  }

  interface WebHookData {
    time_ms: number;
    events: Array<Event>;
  }

  export interface Token {
    key: string;
    secret: string;
  }

  export class WebHook {
    constructor(token: Token, request: WebHookRequest);

    isValid(extraTokens?: Token | Array<Token>): boolean;
    isContentTypeValid(): boolean;
    isBodyValid(): boolean;
    getData(): WebHookData;
    getEvents(): Array<Event>;
    getTime(): Date;
  }

  export class RequestError extends Error {
    constructor(message: string, url: string, error: Error, status?: number, body?: string);
    url: string;
    error: Error;
    status?: number;
    body?: string;
  }

  export class WebHookError extends Error {
    constructor(message: string, contentType: string, body: string, signature: string);
    contentType: string;
    body: string;
    signature: string;
  }

  export { Response };
}
