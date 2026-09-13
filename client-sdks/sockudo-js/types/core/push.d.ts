export type PushHeadersProvider =
  | Record<string, string>
  | (() => Record<string, string> | Promise<Record<string, string>>);

export interface PushRegistrationOptions {
  endpoint: string;
  headers?: PushHeadersProvider;
  fetch?: typeof fetch;
}

export interface PushCursorParams {
  [key: string]: unknown;
  limit?: number;
  cursor?: string;
}

export interface PushSubscriptionParams extends PushCursorParams {
  channel?: string;
  deviceId?: string;
}

export type PushProviderKind = "fcm" | "apns" | "webPush" | "hms" | "wns";
export type ApnsChannelStoragePolicy = "noStorage" | "mostRecent";
export type ApnsLiveActivityEvent = "start" | "update" | "end";
export type ApnsLiveActivityPriority = "lowPower" | "conservePower" | "immediate";

export interface ApnsLiveActivityPayload {
  event: ApnsLiveActivityEvent;
  timestamp: number;
  contentState: Record<string, unknown>;
  attributesType?: string;
  attributes?: Record<string, unknown>;
  alert?: Record<string, unknown>;
  staleDate?: number;
  dismissalDate?: number;
  relevanceScore?: number;
  inputPushToken?: boolean;
  inputPushChannel?: string;
  priority?: ApnsLiveActivityPriority;
}

export type ApnsLiveActivityRecipient =
  | { transportType: "apnsLiveActivity"; activityToken: string }
  | {
      transportType: "apnsLiveActivityBroadcast";
      channelId: string;
      storagePolicy: ApnsChannelStoragePolicy;
    };

export type PushRecipient =
  | { transportType: "gcm"; registrationToken: string }
  | { transportType: "apns"; deviceToken: string }
  | ApnsLiveActivityRecipient
  | { transportType: "web"; endpoint: string; p256dh: string; auth: string }
  | { transportType: "hms"; registrationToken: string }
  | { transportType: "wns"; channelUri: string };

export interface PushDeviceDetails {
  id: string;
  clientId?: string;
  formFactor: string;
  platform: string;
  metadata?: Record<string, unknown>;
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

export interface PushChannelSubscription {
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
  | { type: "recipient"; recipient: PushRecipient };

export interface PushPublishRequest {
  publishId?: string;
  recipients: PushPublishTarget[];
  payload: PushPayload;
  providerOverrides?: Array<{
    provider: PushProviderKind;
    payload: Record<string, unknown>;
  }>;
  liveActivity?: ApnsLiveActivityPayload;
  notBeforeMs?: number;
  expiresAtMs?: number;
}

export interface ApnsLiveActivityPublishRequest {
  publishId?: string;
  recipients: Array<{ type: "recipient"; recipient: ApnsLiveActivityRecipient }>;
  payload?: PushPayload;
  liveActivity: ApnsLiveActivityPayload;
  notBeforeMs?: number;
  expiresAtMs?: number;
}

export declare class SockudoPushRegistration {
  constructor(options: PushRegistrationOptions);
  activateDevice(device: PushDeviceDetails): Promise<unknown>;
  updateDeviceRegistration(
    device: PushDeviceDetails,
    deviceIdentityToken: string,
  ): Promise<unknown>;
  listDeviceRegistrations(params?: PushCursorParams): Promise<unknown>;
  getDeviceRegistration(deviceId: string): Promise<unknown>;
  deleteDeviceRegistration(deviceId: string): Promise<unknown>;
  upsertChannelSubscription(subscription: PushChannelSubscription): Promise<unknown>;
  listChannelSubscriptions(params?: PushSubscriptionParams): Promise<unknown>;
  deleteChannelSubscriptions(params: PushSubscriptionParams): Promise<unknown>;
  publish(request: PushPublishRequest): Promise<unknown>;
  publishLiveActivity(request: ApnsLiveActivityPublishRequest): Promise<unknown>;
  publishBatch(requests: PushPublishRequest[]): Promise<unknown>;
  schedulePublish(request: PushPublishRequest & { notBeforeMs: number }): Promise<unknown>;
  getPublishStatus(publishId: string): Promise<unknown>;
  cancelScheduledPublish(publishId: string): Promise<unknown>;
  postDeliveryStatus(event: Record<string, unknown>): Promise<unknown>;
}
