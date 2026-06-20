import { default as EventsDispatcher } from "../events/dispatcher";
import * as Errors from "../errors";
import Logger from "../logger";
import Sockudo from "../sockudo";
import { SockudoEvent } from "../connection/protocol/message-types";
import Metadata from "./metadata";
import UrlStore from "../utils/url_store";
import { ChannelAuthorizationData, ChannelAuthorizationCallback } from "../auth/options";
import { HTTPAuthError } from "../errors";
import { FilterNode } from "./filter";
import { DeltaAlgorithm } from "../delta/types";
import { prefixedEvent, prefixedInternal, isInternalEvent } from "../protocol_prefix";
import type { VersionedMessagesOptions } from "../options";

export type SubscriptionRewind =
  | number
  | {
      count?: number;
      seconds?: number;
    };

export interface ChannelSubscriptionOptions {
  filter?: any;
  delta?: { enabled?: boolean; algorithm?: "fossil" | "xdelta3" };
  events?: string[];
  rewind?: SubscriptionRewind;
  annotationSubscribe?: boolean;
}

export interface ChannelHistoryParams {
  direction?: "newest_first" | "oldest_first";
  limit?: number;
  cursor?: string;
  start_serial?: number;
  end_serial?: number;
  start_time_ms?: number;
  end_time_ms?: number;
}

export interface ChannelHistoryPage {
  items: Array<Record<string, unknown>>;
  direction: string;
  limit: number;
  has_more: boolean;
  next_cursor?: string | null;
  bounds: Record<string, unknown>;
  continuity: Record<string, unknown>;
  stream_state?: Record<string, unknown>;
  hasNext(): boolean;
  next(): Promise<ChannelHistoryPage>;
}

export interface MessageVersionsParams {
  direction?: "newest_first" | "oldest_first";
  limit?: number;
  cursor?: string;
}

export interface GetMessageResponse {
  channel: string;
  item: Record<string, unknown>;
}

export interface ListMessageVersionsResponse {
  channel: string;
  direction: string;
  limit: number;
  has_more: boolean;
  next_cursor?: string | null;
  items: Array<Record<string, unknown>>;
  hasNext(): boolean;
  next(): Promise<ListMessageVersionsResponse>;
}

export interface PublishAnnotationRequest {
  type: string;
  name?: string;
  clientId?: string;
  socketId?: string;
  count?: number;
  data?: unknown;
  encoding?: string | null;
}

export interface PublishAnnotationResponse {
  annotationSerial: string;
}

export interface DeleteAnnotationResponse {
  annotationSerial: string;
  deletedAnnotationSerial: string;
}

export interface AnnotationEventsParams {
  type?: string;
  limit?: number;
  fromSerial?: string;
  socketId?: string;
}

export interface AnnotationEvent {
  action: "annotation.create" | "annotation.delete";
  id?: string;
  serial: string;
  messageSerial: string;
  type: string;
  name?: string;
  clientId?: string;
  count?: number;
  data?: unknown;
  encoding?: string;
  timestamp: number;
}

export interface AnnotationEventsResponse {
  channel: string;
  messageSerial: string;
  limit: number;
  hasMore: boolean;
  nextCursor?: string | null;
  items: AnnotationEvent[];
  hasNext(): boolean;
  next(): Promise<AnnotationEventsResponse>;
}

/**
 * Per-subscription delta compression settings
 * Allows clients to negotiate delta compression on a per-channel basis
 */
export interface ChannelDeltaSettings {
  /**
   * Enable/disable delta compression for this channel subscription
   * - true: Enable delta compression
   * - false: Disable delta compression
   * - undefined: Use server default (global enable_delta_compression)
   */
  enabled?: boolean;
  /**
   * Preferred algorithm for this subscription
   * - 'fossil': Use Fossil delta algorithm
   * - 'xdelta3': Use Xdelta3/VCDIFF algorithm
   * - undefined: Use server default algorithm
   */
  algorithm?: DeltaAlgorithm;
}

/**
 * Serialize delta settings for the subscription message
 * Supports multiple formats for server compatibility:
 * - Simple string: "fossil", "xdelta3", "disabled"
 * - Boolean: true/false
 * - Object: { enabled: boolean, algorithm: string }
 */
function serializeDeltaSettings(
  settings: ChannelDeltaSettings,
): string | boolean | { enabled?: boolean; algorithm?: string } {
  // If only algorithm is specified, use simple string format
  if (settings.enabled === undefined && settings.algorithm) {
    return settings.algorithm;
  }
  // If only enabled is specified as false, use "disabled" string
  if (settings.enabled === false && settings.algorithm === undefined) {
    return false;
  }
  // If only enabled is specified as true, use boolean
  if (settings.enabled === true && settings.algorithm === undefined) {
    return true;
  }
  // Otherwise use full object format
  return {
    enabled: settings.enabled,
    algorithm: settings.algorithm,
  };
}

/** Provides base public channel interface with an event emitter.
 *
 * Emits:
 * - subscription_succeeded - after subscribing successfully
 * - other non-internal events
 *
 * @param {String} name
 * @param {Sockudo} sockudo
 */
export default class Channel extends EventsDispatcher {
  name: string;
  sockudo: Sockudo;
  subscribed: boolean;
  subscriptionPending: boolean;
  subscriptionCancelled: boolean;
  subscriptionCount: null;
  tagsFilter: FilterNode | null;
  eventsFilter: string[] | null;
  deltaSettings: ChannelDeltaSettings | null;
  rewind: SubscriptionRewind | null;
  annotationSubscribe: boolean;

  constructor(name: string, sockudo: Sockudo) {
    super(function (event, _data) {
      Logger.debug("No callbacks on " + name + " for " + event);
    });

    this.name = name;
    this.sockudo = sockudo;
    this.subscribed = false;
    this.subscriptionPending = false;
    this.subscriptionCancelled = false;
    this.tagsFilter = null;
    this.eventsFilter = null;
    this.deltaSettings = null;
    this.rewind = null;
    this.annotationSubscribe = false;
  }

  /**
   * Set per-subscription delta compression settings
   *
   * Call this before subscribing to negotiate delta compression for this channel.
   * Alternatively, pass delta settings to the subscribe() method.
   *
   * @param settings Delta compression settings for this channel
   *
   * @example
   * // Enable delta compression with Fossil algorithm
   * channel.setDeltaSettings({ enabled: true, algorithm: 'fossil' });
   *
   * @example
   * // Disable delta compression for this channel
   * channel.setDeltaSettings({ enabled: false });
   *
   * @example
   * // Use server default but prefer xdelta3
   * channel.setDeltaSettings({ algorithm: 'xdelta3' });
   */
  setDeltaSettings(settings: ChannelDeltaSettings | null): void {
    this.deltaSettings = settings;
    Logger.debug(`Delta settings for channel ${this.name}: ${JSON.stringify(settings)}`);
  }

  /**
   * Get current delta compression settings for this channel
   */
  getDeltaSettings(): ChannelDeltaSettings | null {
    return this.deltaSettings;
  }

  async channelHistory(params: ChannelHistoryParams = {}): Promise<ChannelHistoryPage> {
    const config = this.sockudo.config.versionedMessages;
    if (!config?.endpoint) {
      throw new Error(
        "versionedMessages.endpoint must be configured to use channelHistory(). " +
          "This endpoint should proxy requests to the Sockudo server REST API.",
      );
    }
    return this.fetchChannelHistoryPage(config, params);
  }

  async getMessage(messageSerial: string): Promise<GetMessageResponse> {
    const config = this.sockudo.config.versionedMessages;
    if (!config?.endpoint) {
      throw new Error(
        "versionedMessages.endpoint must be configured to use getMessage(). " +
          "This endpoint should proxy requests to the Sockudo server REST API.",
      );
    }
    const response = await this.fetchVersioned(config, {
      channel: this.name,
      messageSerial,
      action: "get_message",
    });
    return response as unknown as GetMessageResponse;
  }

  async getMessageVersions(
    messageSerial: string,
    params: MessageVersionsParams = {},
  ): Promise<ListMessageVersionsResponse> {
    const config = this.sockudo.config.versionedMessages;
    if (!config?.endpoint) {
      throw new Error(
        "versionedMessages.endpoint must be configured to use getMessageVersions(). " +
          "This endpoint should proxy requests to the Sockudo server REST API.",
      );
    }
    return this.fetchMessageVersionsPage(config, messageSerial, params);
  }

  async publishAnnotation(
    messageSerial: string,
    annotation: PublishAnnotationRequest,
  ): Promise<PublishAnnotationResponse> {
    const config = this.sockudo.config.versionedMessages;
    if (!config?.endpoint) {
      throw new Error(
        "versionedMessages.endpoint must be configured to use publishAnnotation(). " +
          "This endpoint should proxy requests to the Sockudo server REST API.",
      );
    }
    const response = await this.fetchVersioned(config, {
      channel: this.name,
      messageSerial,
      annotation,
      action: "publish_annotation",
    });
    return response as unknown as PublishAnnotationResponse;
  }

  async deleteAnnotation(
    messageSerial: string,
    annotationSerial: string,
    socketId?: string,
  ): Promise<DeleteAnnotationResponse> {
    const config = this.sockudo.config.versionedMessages;
    if (!config?.endpoint) {
      throw new Error(
        "versionedMessages.endpoint must be configured to use deleteAnnotation(). " +
          "This endpoint should proxy requests to the Sockudo server REST API.",
      );
    }
    const response = await this.fetchVersioned(config, {
      channel: this.name,
      messageSerial,
      annotationSerial,
      socketId,
      action: "delete_annotation",
    });
    return response as unknown as DeleteAnnotationResponse;
  }

  async listAnnotations(
    messageSerial: string,
    params: AnnotationEventsParams = {},
  ): Promise<AnnotationEventsResponse> {
    const config = this.sockudo.config.versionedMessages;
    if (!config?.endpoint) {
      throw new Error(
        "versionedMessages.endpoint must be configured to use listAnnotations(). " +
          "This endpoint should proxy requests to the Sockudo server REST API.",
      );
    }
    return this.fetchAnnotationEventsPage(config, messageSerial, params);
  }

  /** Skips authorization, since public channels don't require it.
   *
   * @param {(...args: any[]) => any} callback
   */
  authorize(socketId: string, callback: ChannelAuthorizationCallback) {
    return callback(null, { auth: "" });
  }

  /** Triggers an event */
  trigger(event: string, data: any) {
    if (event.indexOf("client-") !== 0) {
      throw new Errors.BadEventName("Event '" + event + "' does not start with 'client-'");
    }
    if (!this.subscribed) {
      const suffix = UrlStore.buildLogSuffix("triggeringClientEvents");
      Logger.warn(
        `Client event triggered before channel 'subscription_succeeded' event . ${suffix}`,
      );
    }
    return this.sockudo.send_event(event, data, this.name);
  }

  /** Signals disconnection to the channel. For internal use only. */
  disconnect() {
    this.subscribed = false;
    this.subscriptionPending = false;
  }

  /** Handles a SockudoEvent. For internal use only.
   *
   * @param {SockudoEvent} event
   */
  handleEvent(event: SockudoEvent) {
    const eventName = event.event;
    const data = event.data;
    if (eventName === prefixedInternal("subscription_succeeded")) {
      this.handleSubscriptionSucceededEvent(event);
    } else if (eventName === prefixedInternal("subscription_count")) {
      this.handleSubscriptionCountEvent(event);
    } else if (eventName === prefixedInternal("message") && data?.action === "message.summary") {
      const metadata: Metadata = {};
      this.emit("message.summary", data, metadata);
    } else if (eventName === prefixedInternal("annotation") && data?.action) {
      const metadata: Metadata = {};
      this.emit(data.action, data, metadata);
    } else if (!isInternalEvent(eventName)) {
      const metadata: Metadata = {};
      this.emit(eventName, data, metadata);
    }
  }

  handleSubscriptionSucceededEvent(event: SockudoEvent) {
    this.subscriptionPending = false;
    this.subscribed = true;
    if (this.subscriptionCancelled) {
      this.sockudo.unsubscribe(this.name);
    } else {
      this.emit(prefixedEvent("subscription_succeeded"), event.data);
    }
  }

  handleSubscriptionCountEvent(event: SockudoEvent) {
    if (event.data.subscription_count) {
      this.subscriptionCount = event.data.subscription_count;
    }

    this.emit(prefixedEvent("subscription_count"), event.data);
  }

  /** Sends a subscription request. For internal use only. */
  subscribe() {
    if (this.subscribed) {
      return;
    }
    this.subscriptionPending = true;
    this.subscriptionCancelled = false;
    this.authorize(
      this.sockudo.connection.socket_id,
      (error: Error | null, data: ChannelAuthorizationData) => {
        if (error) {
          this.subscriptionPending = false;
          // Why not bind to 'subscription_error' a level up, and log there?
          // Binding to this event would cause the warning about no callbacks being
          // bound (see constructor) to be suppressed, that's not what we want.
          Logger.error(error.toString());
          this.emit(
            prefixedEvent("subscription_error"),
            Object.assign(
              {},
              {
                type: "AuthError",
                error: error.message,
              },
              error instanceof HTTPAuthError ? { status: error.status } : {},
            ),
          );
        } else {
          const subscribeData: any = {
            auth: data.auth,
            channel_data: data.channel_data,
            channel: this.name,
          };

          if (this.tagsFilter) {
            subscribeData.tags_filter = this.tagsFilter;
          }

          if (this.eventsFilter) {
            subscribeData.events = this.eventsFilter;
          }

          if (this.rewind !== null) {
            subscribeData.rewind = this.rewind;
          }

          if (this.annotationSubscribe) {
            subscribeData.modes = ["SUBSCRIBE", "ANNOTATION_SUBSCRIBE"];
          }

          // Add per-subscription delta settings if present
          // This enables per-channel delta negotiation
          if (this.deltaSettings) {
            subscribeData.delta = serializeDeltaSettings(this.deltaSettings);
            Logger.debug(
              `Subscribing to ${this.name} with delta settings: ${JSON.stringify(subscribeData.delta)}`,
            );
          }

          this.sockudo.send_event(prefixedEvent("subscribe"), subscribeData);
        }
      },
    );
  }

  private async fetchVersioned(
    config: VersionedMessagesOptions,
    body: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...config.headers,
      ...config.headersProvider?.(),
    };

    const response = await fetch(config.endpoint, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      credentials: "same-origin",
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Versioned message request failed (${response.status}): ${text}`);
    }

    return response.json() as Promise<Record<string, unknown>>;
  }

  private async fetchChannelHistoryPage(
    config: VersionedMessagesOptions,
    params: ChannelHistoryParams,
  ): Promise<ChannelHistoryPage> {
    const data = await this.fetchVersioned(config, {
      channel: this.name,
      params,
      action: "channel_history",
    });
    return {
      ...(data as Omit<ChannelHistoryPage, "hasNext" | "next">),
      hasNext(): boolean {
        return !!data.has_more && !!data.next_cursor;
      },
      next: (): Promise<ChannelHistoryPage> => {
        if (!data.has_more || !data.next_cursor) {
          return Promise.reject(new Error("No more pages available"));
        }
        return this.fetchChannelHistoryPage(config, {
          ...params,
          cursor: data.next_cursor as string,
        });
      },
    };
  }

  private async fetchMessageVersionsPage(
    config: VersionedMessagesOptions,
    messageSerial: string,
    params: MessageVersionsParams,
  ): Promise<ListMessageVersionsResponse> {
    const data = await this.fetchVersioned(config, {
      channel: this.name,
      messageSerial,
      params,
      action: "get_message_versions",
    });
    return {
      ...(data as Omit<ListMessageVersionsResponse, "hasNext" | "next">),
      hasNext(): boolean {
        return !!data.has_more && !!data.next_cursor;
      },
      next: (): Promise<ListMessageVersionsResponse> => {
        if (!data.has_more || !data.next_cursor) {
          return Promise.reject(new Error("No more pages available"));
        }
        return this.fetchMessageVersionsPage(config, messageSerial, {
          ...params,
          cursor: data.next_cursor as string,
        });
      },
    };
  }

  private async fetchAnnotationEventsPage(
    config: VersionedMessagesOptions,
    messageSerial: string,
    params: AnnotationEventsParams,
  ): Promise<AnnotationEventsResponse> {
    const data = await this.fetchVersioned(config, {
      channel: this.name,
      messageSerial,
      params,
      action: "list_annotations",
    });
    return {
      ...(data as Omit<AnnotationEventsResponse, "hasNext" | "next">),
      hasNext(): boolean {
        return !!data.hasMore && !!data.nextCursor;
      },
      next: (): Promise<AnnotationEventsResponse> => {
        if (!data.hasMore || !data.nextCursor) {
          return Promise.reject(new Error("No more pages available"));
        }
        return this.fetchAnnotationEventsPage(config, messageSerial, {
          ...params,
          fromSerial: data.nextCursor as string,
        });
      },
    };
  }

  /** Sends an unsubscription request. For internal use only. */
  unsubscribe() {
    this.subscribed = false;
    this.sockudo.send_event(prefixedEvent("unsubscribe"), {
      channel: this.name,
    });
  }

  /** Cancels an in progress subscription. For internal use only. */
  cancelSubscription() {
    this.subscriptionCancelled = true;
  }

  /** Reinstates an in progress subscripiton. For internal use only. */
  reinstateSubscription() {
    this.subscriptionCancelled = false;
  }
}
