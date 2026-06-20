import crypto from "crypto";
import url from "url";
import * as auth from "./auth";
import { RequestError, WebHookError } from "./errors";
import * as events from "./events";
import * as requests from "./requests";
import SockudoConfig = require("./sockudo_config");
import Token = require("./token");
import WebHook = require("./webhook");
import type {
  BatchEvent,
  AnnotationEventsParams,
  AnnotationEventsResponse,
  ChannelAuthResponse,
  DeleteAnnotationResponse,
  GetMessageResponse,
  GetOptions,
  HistoryPage,
  ListMessageVersionsResponse,
  MessageVersionsParams,
  MutationResponse,
  PresenceHistoryPage,
  PresenceHistoryParams,
  PresenceSnapshot,
  PresenceSnapshotParams,
  Options,
  PostOptions,
  PushCapability,
  PushChannelSubscription,
  PushCursorParams,
  PushDeliveryStatusEvent,
  PushDeviceDetails,
  PushDeviceRegistrationResponse,
  PushListResponse,
  PushPublishAcceptedResponse,
  PushPublishRequest,
  PresenceChannelData,
  PublishAnnotationBody,
  PublishAnnotationResponse,
  ResponseWithIdempotency,
  SignedQueryStringOptions,
  TriggerParams,
  UserAuthResponse,
  UserChannelData,
  WebHookRequest,
} from "./types";

function validateChannel(channel: string): void {
  if (typeof channel !== "string" || channel === "" || channel.match(/[^A-Za-z0-9_\-=@,.;:]/)) {
    throw new Error(`Invalid channel name: '${channel}'`);
  }
  if (channel.length > 200) {
    throw new Error(`Channel name too long: '${channel}'`);
  }
}

function validatePresenceChannel(channel: string): void {
  validateChannel(channel);
  if (!channel.startsWith("presence-")) {
    throw new Error(`Presence history is only available for presence channels: '${channel}'`);
  }
}

function validateSocketId(socketId: string): void {
  if (typeof socketId !== "string" || socketId === "" || !socketId.match(/^\d+\.\d+$/)) {
    throw new Error(`Invalid socket id: '${socketId}'`);
  }
}

function validateUserId(userId: string): void {
  if (typeof userId !== "string" || userId === "") {
    throw new Error(`Invalid user id: '${userId}'`);
  }
}

function validateUserData(userData: UserChannelData): void {
  if (userData == null || typeof userData !== "object") {
    throw new Error(`Invalid user data: '${userData}'`);
  }
  validateUserId(userData.id);
}

function generateBase64UrlId(): string {
  return crypto
    .randomBytes(12)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

class Sockudo {
  config: SockudoConfig;
  idempotencyBaseId: string;
  publishSerial: number;
  autoIdempotencyKey: boolean;
  maxRetries: number;

  static Token = Token;
  static RequestError = RequestError;
  static WebHookError = WebHookError;
  authenticate!: (
    socketId: string,
    channel: string,
    data?: PresenceChannelData,
  ) => ChannelAuthResponse;

  constructor(options: Options) {
    this.config = new SockudoConfig(options);
    this.idempotencyBaseId = generateBase64UrlId();
    this.publishSerial = 0;
    this.autoIdempotencyKey =
      options.autoIdempotencyKey !== undefined ? options.autoIdempotencyKey : true;
    this.maxRetries = options.maxRetries !== undefined ? options.maxRetries : 3;
  }

  static forURL(sockudoUrl: string, options?: Partial<Options>): Sockudo {
    const apiUrl = url.parse(sockudoUrl);
    const apiPath = apiUrl.pathname?.split("/") ?? [];
    const apiAuth = apiUrl.auth?.split(":");

    if (!apiUrl.protocol || !apiUrl.hostname || !apiAuth || apiPath.length === 0) {
      throw new Error("Invalid Sockudo URL");
    }

    return new Sockudo({
      ...options,
      scheme: apiUrl.protocol.replace(/:$/, ""),
      host: apiUrl.hostname,
      port: apiUrl.port ? parseInt(apiUrl.port, 10) : undefined,
      appId: parseInt(apiPath[apiPath.length - 1], 10),
      key: apiAuth[0],
      secret: apiAuth[1],
    } as Options);
  }

  static forCluster(cluster: string, options: Omit<Options, "cluster" | "host">): Sockudo {
    return new Sockudo({
      ...options,
      host: `api-${cluster}.sockudo.com`,
    } as Options);
  }

  authorizeChannel(
    socketId: string,
    channel: string,
    data?: PresenceChannelData,
  ): ChannelAuthResponse {
    validateSocketId(socketId);
    validateChannel(channel);

    return auth.getSocketSignature(this, this.config.token, channel, socketId, data);
  }

  authenticateUser(socketId: string, userData: UserChannelData): UserAuthResponse {
    validateSocketId(socketId);
    validateUserData(userData);

    return auth.getSocketSignatureForUser(this.config.token, socketId, userData);
  }

  sendToUser(userId: string, event: string, data: unknown): Promise<ResponseWithIdempotency> {
    if (event.length > 200) {
      throw new Error(`Too long event name: '${event}'`);
    }
    validateUserId(userId);
    return events.trigger(this, [`#server-to-user-${userId}`], event, data);
  }

  terminateUserConnections(userId: string): Promise<ResponseWithIdempotency> {
    validateUserId(userId);
    return this.post({
      path: `/users/${userId}/terminate_connections`,
      body: {},
    });
  }

  trigger(
    channels: string | string[],
    event: string,
    data: unknown,
    params?: TriggerParams,
  ): Promise<ResponseWithIdempotency> {
    if (params?.socket_id) {
      validateSocketId(params.socket_id);
    }

    const normalizedChannels = Array.isArray(channels) ? channels : [channels];
    if (event.length > 200) {
      throw new Error(`Too long event name: '${event}'`);
    }
    if (normalizedChannels.length > 100) {
      throw new Error("Can't trigger a message to more than 100 channels");
    }
    for (const channel of normalizedChannels) {
      validateChannel(channel);
    }

    const serial = this.publishSerial++;
    let idempotencyKey = params?.idempotency_key;
    let resolvedParams = params;

    if (this.autoIdempotencyKey && (!params || params.idempotency_key === undefined)) {
      idempotencyKey = `${this.idempotencyBaseId}:${serial}`;
      resolvedParams = {
        ...params,
        idempotency_key: idempotencyKey,
      };
    }

    const attempt = (remaining: number): Promise<ResponseWithIdempotency> => {
      return events.trigger(this, normalizedChannels, event, data, resolvedParams).then(
        (response) => {
          response.idempotencyKey = idempotencyKey;
          return response;
        },
        (err: RequestError) => {
          const statusCode = err.statusCode !== undefined ? err.statusCode : err.status;
          const isAbortError =
            err.error &&
            ((err.error as any).name === "AbortError" || (err.error as any).type === "aborted");
          const isRetryable = !isAbortError && (statusCode === undefined || statusCode >= 500);
          if (isRetryable && remaining > 1) {
            return attempt(remaining - 1);
          }
          throw err;
        },
      );
    };

    return attempt(this.maxRetries);
  }

  triggerBatch(batch: BatchEvent[]): Promise<ResponseWithIdempotency> {
    const serial = this.publishSerial++;
    const batchKeys: Array<NonNullable<BatchEvent["idempotency_key"]>> = [];
    const normalizedBatch = [...batch];

    if (this.autoIdempotencyKey) {
      for (let i = 0; i < normalizedBatch.length; i++) {
        if (normalizedBatch[i].idempotency_key === undefined) {
          const key = `${this.idempotencyBaseId}:${serial}:${i}`;
          normalizedBatch[i] = {
            ...normalizedBatch[i],
            idempotency_key: key,
          };
          batchKeys.push(key);
        } else {
          const existingKey = normalizedBatch[i].idempotency_key;
          if (existingKey !== undefined) {
            batchKeys.push(existingKey);
          }
        }
      }
    }

    const attempt = (remaining: number): Promise<ResponseWithIdempotency> => {
      return events.triggerBatch(this, normalizedBatch).then(
        (response) => {
          response.idempotencyKeys = batchKeys;
          return response;
        },
        (err: RequestError) => {
          const statusCode = err.statusCode !== undefined ? err.statusCode : err.status;
          const isAbortError =
            err.error &&
            ((err.error as any).name === "AbortError" || (err.error as any).type === "aborted");
          const isRetryable = !isAbortError && (statusCode === undefined || statusCode >= 500);
          if (isRetryable && remaining > 1) {
            return attempt(remaining - 1);
          }
          throw err;
        },
      );
    };

    return attempt(this.maxRetries);
  }

  post(options: PostOptions): Promise<ResponseWithIdempotency> {
    return requests.send(this.config, {
      ...options,
      method: "POST",
    }) as Promise<ResponseWithIdempotency>;
  }

  get(options: GetOptions): Promise<ResponseWithIdempotency> {
    return requests.send(this.config, {
      ...options,
      method: "GET",
    }) as Promise<ResponseWithIdempotency>;
  }

  delete(options: GetOptions): Promise<ResponseWithIdempotency> {
    return requests.send(this.config, {
      ...options,
      method: "DELETE",
    }) as Promise<ResponseWithIdempotency>;
  }

  private pushHeaders(
    capability: PushCapability = "push-admin",
    deviceIdentityToken?: string,
  ): Record<string, string> {
    const headers: Record<string, string> = {
      "x-sockudo-push-capability": capability,
    };
    if (deviceIdentityToken) {
      headers["x-sockudo-device-identity-token"] = deviceIdentityToken;
    }
    return headers;
  }

  activateDevice(
    device: PushDeviceDetails,
    options: { rotateDeviceIdentityToken?: boolean } = {},
  ): Promise<PushDeviceRegistrationResponse> {
    const headers = this.pushHeaders("push-admin");
    if (options.rotateDeviceIdentityToken) {
      headers["x-sockudo-rotate-device-identity-token"] = "true";
    }
    return this.post({
      path: "/push/deviceRegistrations",
      body: device,
      headers,
    }).then((response) => response.json() as Promise<PushDeviceRegistrationResponse>);
  }

  createDeviceActivation(device: PushDeviceDetails): Promise<PushDeviceRegistrationResponse> {
    return this.activateDevice(device);
  }

  updateDeviceRegistration(
    device: PushDeviceDetails,
    deviceIdentityToken: string,
  ): Promise<PushDeviceRegistrationResponse> {
    return this.post({
      path: "/push/deviceRegistrations",
      body: device,
      headers: this.pushHeaders("push-subscribe", deviceIdentityToken),
    }).then((response) => response.json() as Promise<PushDeviceRegistrationResponse>);
  }

  listDeviceRegistrations(
    params: PushCursorParams = {},
  ): Promise<PushListResponse<Record<string, unknown>>> {
    return this.get({
      path: "/push/deviceRegistrations",
      params,
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<PushListResponse<Record<string, unknown>>>);
  }

  getDeviceRegistration(
    deviceId: string,
    deviceIdentityToken?: string,
  ): Promise<Record<string, unknown>> {
    return this.get({
      path: `/push/deviceRegistrations/${deviceId}`,
      headers: this.pushHeaders(
        deviceIdentityToken ? "push-subscribe" : "push-admin",
        deviceIdentityToken,
      ),
    }).then((response) => response.json() as Promise<Record<string, unknown>>);
  }

  deleteDeviceRegistration(
    deviceId: string,
    deviceIdentityToken?: string,
  ): Promise<ResponseWithIdempotency> {
    return this.delete({
      path: `/push/deviceRegistrations/${deviceId}`,
      headers: this.pushHeaders(
        deviceIdentityToken ? "push-subscribe" : "push-admin",
        deviceIdentityToken,
      ),
    });
  }

  removeDeviceRegistrationsByClient(clientId: string): Promise<Record<string, unknown>> {
    return this.delete({
      path: "/push/deviceRegistrations",
      params: { clientId },
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<Record<string, unknown>>);
  }

  upsertChannelPushSubscription(
    subscription: PushChannelSubscription,
    deviceIdentityToken?: string,
  ): Promise<PushChannelSubscription> {
    return this.post({
      path: "/push/channelSubscriptions",
      body: subscription,
      headers: this.pushHeaders(
        deviceIdentityToken ? "push-subscribe" : "push-admin",
        deviceIdentityToken,
      ),
    }).then((response) => response.json() as Promise<PushChannelSubscription>);
  }

  listChannelPushSubscriptions(
    params: PushCursorParams & { channel?: string; deviceId?: string } = {},
    deviceIdentityToken?: string,
  ): Promise<PushListResponse<PushChannelSubscription>> {
    return this.get({
      path: "/push/channelSubscriptions",
      params,
      headers: this.pushHeaders(
        deviceIdentityToken ? "push-subscribe" : "push-admin",
        deviceIdentityToken,
      ),
    }).then((response) => response.json() as Promise<PushListResponse<PushChannelSubscription>>);
  }

  deleteChannelPushSubscriptions(
    params: { channel?: string; deviceId?: string },
    deviceIdentityToken?: string,
  ): Promise<Record<string, unknown>> {
    return this.delete({
      path: "/push/channelSubscriptions",
      params,
      headers: this.pushHeaders(
        deviceIdentityToken ? "push-subscribe" : "push-admin",
        deviceIdentityToken,
      ),
    }).then((response) => response.json() as Promise<Record<string, unknown>>);
  }

  listChannelPushSubscriptionChannels(
    params: PushCursorParams = {},
  ): Promise<PushListResponse<string>> {
    return this.get({
      path: "/push/channelSubscriptions/channels",
      params,
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<PushListResponse<string>>);
  }

  listPushCredentials(
    params: PushCursorParams = {},
  ): Promise<PushListResponse<Record<string, unknown>>> {
    return this.get({
      path: "/push/credentials",
      params,
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<PushListResponse<Record<string, unknown>>>);
  }

  putPushCredential(
    provider: "fcm" | "apns" | "webpush" | "hms" | "wns",
    credential: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    return this.post({
      path: `/push/credentials/${provider}`,
      body: credential,
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<Record<string, unknown>>);
  }

  publishPush(request: PushPublishRequest): Promise<PushPublishAcceptedResponse> {
    return this.post({
      path: "/push/publish",
      body: { ...request, sync: false },
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<PushPublishAcceptedResponse>);
  }

  publishPushDirect(request: PushPublishRequest): Promise<PushPublishAcceptedResponse> {
    return this.publishPush(request);
  }

  publishPushBatch(
    requests: PushPublishRequest[],
  ): Promise<{ items: PushPublishAcceptedResponse[] }> {
    return this.post({
      path: "/push/batch/publish",
      body: requests.map((request) => ({ ...request, sync: false })),
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<{ items: PushPublishAcceptedResponse[] }>);
  }

  schedulePush(
    request: PushPublishRequest & { notBeforeMs: number },
  ): Promise<PushPublishAcceptedResponse> {
    return this.publishPush(request);
  }

  getPublishStatus(publishId: string): Promise<Record<string, unknown>> {
    return this.get({
      path: `/push/publish/${publishId}/status`,
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<Record<string, unknown>>);
  }

  cancelScheduledPush(publishId: string): Promise<ResponseWithIdempotency> {
    return this.delete({
      path: `/push/scheduled/${publishId}`,
      headers: this.pushHeaders("push-admin"),
    });
  }

  postPushDeliveryStatus(event: PushDeliveryStatusEvent): Promise<Record<string, unknown>> {
    return this.post({
      path: "/push/deliveryStatus",
      body: event,
      headers: this.pushHeaders("push-admin"),
    }).then((response) => response.json() as Promise<Record<string, unknown>>);
  }

  channelHistory(channel: string, params: GetOptions["params"] = {}): Promise<HistoryPage> {
    validateChannel(channel);
    return this.get({
      path: `/channels/${channel}/history`,
      params,
    }).then((response) => response.json() as Promise<HistoryPage>);
  }

  getMessage(channel: string, messageSerial: string): Promise<GetMessageResponse> {
    validateChannel(channel);
    return this.get({
      path: `/channels/${channel}/messages/${messageSerial}`,
    }).then((response) => response.json() as Promise<GetMessageResponse>);
  }

  getMessageVersions(
    channel: string,
    messageSerial: string,
    params: MessageVersionsParams = {},
  ): Promise<ListMessageVersionsResponse> {
    validateChannel(channel);
    return this.get({
      path: `/channels/${channel}/messages/${messageSerial}/versions`,
      params,
    }).then((response) => response.json() as Promise<ListMessageVersionsResponse>);
  }

  updateMessage(
    channel: string,
    messageSerial: string,
    body: Record<string, unknown>,
  ): Promise<MutationResponse> {
    validateChannel(channel);
    return this.post({
      path: `/channels/${channel}/messages/${messageSerial}/update`,
      body,
    }).then((response) => response.json() as Promise<MutationResponse>);
  }

  deleteMessage(
    channel: string,
    messageSerial: string,
    body: Record<string, unknown> = {},
  ): Promise<MutationResponse> {
    validateChannel(channel);
    return this.post({
      path: `/channels/${channel}/messages/${messageSerial}/delete`,
      body,
    }).then((response) => response.json() as Promise<MutationResponse>);
  }

  appendMessage(
    channel: string,
    messageSerial: string,
    body: { data: string; [key: string]: unknown },
  ): Promise<MutationResponse> {
    validateChannel(channel);
    return this.post({
      path: `/channels/${channel}/messages/${messageSerial}/append`,
      body,
    }).then((response) => response.json() as Promise<MutationResponse>);
  }

  publishAnnotation(
    channel: string,
    messageSerial: string,
    body: PublishAnnotationBody,
  ): Promise<PublishAnnotationResponse> {
    validateChannel(channel);
    return this.post({
      path: `/channels/${channel}/messages/${messageSerial}/annotations`,
      body,
    }).then((response) => response.json() as Promise<PublishAnnotationResponse>);
  }

  deleteAnnotation(
    channel: string,
    messageSerial: string,
    annotationSerial: string,
    params: { socket_id?: string } = {},
  ): Promise<DeleteAnnotationResponse> {
    validateChannel(channel);
    return this.delete({
      path: `/channels/${channel}/messages/${messageSerial}/annotations/${annotationSerial}`,
      params,
    }).then((response) => response.json() as Promise<DeleteAnnotationResponse>);
  }

  listAnnotations(
    channel: string,
    messageSerial: string,
    params: AnnotationEventsParams = {},
  ): Promise<AnnotationEventsResponse> {
    validateChannel(channel);
    return this.get({
      path: `/channels/${channel}/messages/${messageSerial}/annotations`,
      params,
    }).then((response) => response.json() as Promise<AnnotationEventsResponse>);
  }

  channelPresenceHistory(
    channel: string,
    params: PresenceHistoryParams = {},
  ): Promise<PresenceHistoryPage> {
    validatePresenceChannel(channel);
    return this.get({
      path: `/channels/${channel}/presence/history`,
      params,
    }).then((response) => response.json() as Promise<PresenceHistoryPage>);
  }

  channelPresenceSnapshot(
    channel: string,
    params: PresenceSnapshotParams = {},
  ): Promise<PresenceSnapshot> {
    validatePresenceChannel(channel);
    return this.get({
      path: `/channels/${channel}/presence/history/snapshot`,
      params,
    }).then((response) => response.json() as Promise<PresenceSnapshot>);
  }

  webhook(request: WebHookRequest): WebHook {
    return new WebHook(this.config.token, request);
  }

  createSignedQueryString(options: SignedQueryStringOptions): string {
    return requests.createSignedQueryString(this.config.token, options);
  }

  channelSharedSecret(channel: string): Buffer {
    return crypto
      .createHash("sha256")
      .update(Buffer.concat([Buffer.from(channel), this.config.encryptionMasterKey!]))
      .digest();
  }
}

Sockudo.prototype.authenticate = Sockudo.prototype.authorizeChannel;

export = Sockudo;
