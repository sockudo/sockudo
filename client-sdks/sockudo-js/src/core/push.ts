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

export type PushRecipient =
  | { transportType: "gcm"; registrationToken: string }
  | { transportType: "apns"; deviceToken: string }
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
  | { type: "userTopic"; topic: string };

export interface PushPublishRequest {
  publishId?: string;
  recipients: PushPublishTarget[];
  payload: PushPayload;
  providerOverrides?: Array<{
    provider: PushProviderKind;
    payload: Record<string, unknown>;
  }>;
  notBeforeMs?: number;
  expiresAtMs?: number;
}

export class SockudoPushRegistration {
  private endpoint: string;
  private headers?: PushHeadersProvider;
  private fetchImpl: typeof fetch;

  constructor(options: PushRegistrationOptions) {
    this.endpoint = options.endpoint.replace(/\/+$/, "");
    this.headers = options.headers;
    this.fetchImpl =
      options.fetch ||
      (typeof window !== "undefined" && typeof window.fetch === "function"
        ? window.fetch.bind(window)
        : fetch);
  }

  activateDevice(device: PushDeviceDetails): Promise<unknown> {
    return this.request("POST", "/deviceRegistrations", device);
  }

  updateDeviceRegistration(
    device: PushDeviceDetails,
    deviceIdentityToken: string,
  ): Promise<unknown> {
    return this.request("POST", "/deviceRegistrations", device, undefined, {
      "X-Sockudo-Device-Identity-Token": deviceIdentityToken,
    });
  }

  listDeviceRegistrations(params: PushCursorParams = {}): Promise<unknown> {
    return this.request("GET", "/deviceRegistrations", undefined, params);
  }

  getDeviceRegistration(deviceId: string): Promise<unknown> {
    return this.request(
      "GET",
      `/deviceRegistrations/${encodeURIComponent(deviceId)}`,
    );
  }

  deleteDeviceRegistration(deviceId: string): Promise<unknown> {
    return this.request(
      "DELETE",
      `/deviceRegistrations/${encodeURIComponent(deviceId)}`,
    );
  }

  upsertChannelSubscription(
    subscription: PushChannelSubscription,
  ): Promise<unknown> {
    return this.request("POST", "/channelSubscriptions", subscription);
  }

  listChannelSubscriptions(
    params: PushSubscriptionParams = {},
  ): Promise<unknown> {
    return this.request("GET", "/channelSubscriptions", undefined, params);
  }

  deleteChannelSubscriptions(params: PushSubscriptionParams): Promise<unknown> {
    return this.request("DELETE", "/channelSubscriptions", undefined, params);
  }

  publish(request: PushPublishRequest): Promise<unknown> {
    return this.request("POST", "/publish", { ...request, sync: false });
  }

  publishBatch(requests: PushPublishRequest[]): Promise<unknown> {
    return this.request(
      "POST",
      "/batch/publish",
      requests.map((request) => ({ ...request, sync: false })),
    );
  }

  schedulePublish(
    request: PushPublishRequest & { notBeforeMs: number },
  ): Promise<unknown> {
    return this.publish(request);
  }

  getPublishStatus(publishId: string): Promise<unknown> {
    return this.request(
      "GET",
      `/publish/${encodeURIComponent(publishId)}/status`,
    );
  }

  cancelScheduledPublish(publishId: string): Promise<unknown> {
    return this.request(
      "DELETE",
      `/scheduled/${encodeURIComponent(publishId)}`,
    );
  }

  postDeliveryStatus(event: Record<string, unknown>): Promise<unknown> {
    return this.request("POST", "/deliveryStatus", event);
  }

  private async request(
    method: string,
    path: string,
    body?: unknown,
    params?: Record<string, unknown>,
    headers?: Record<string, string>,
  ): Promise<unknown> {
    const resolvedHeaders = await this.resolveHeaders(headers);
    const url = new URL(`${this.endpoint}${path}`);
    for (const [key, value] of Object.entries(params || {})) {
      if (value !== undefined && value !== null) {
        url.searchParams.set(key, String(value));
      }
    }

    const response = await this.fetchImpl(url.toString(), {
      method,
      headers: resolvedHeaders,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(
        `Sockudo push request failed with HTTP ${response.status}`,
      );
    }
    if (response.status === 204) {
      return undefined;
    }
    return response.json();
  }

  private async resolveHeaders(
    headers?: Record<string, string>,
  ): Promise<Record<string, string>> {
    const base =
      typeof this.headers === "function"
        ? await this.headers()
        : this.headers || {};
    return {
      ...base,
      ...headers,
      "Content-Type": "application/json",
    };
  }
}
