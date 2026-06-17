import PrivateChannel from "./private_channel";
import Logger from "../logger";
import Members from "./members";
import Sockudo from "../sockudo";
import UrlStore from "core/utils/url_store";
import { SockudoEvent } from "../connection/protocol/message-types";
import Metadata from "./metadata";
import { ChannelAuthorizationData } from "../auth/options";
import {
  prefixedEvent,
  prefixedInternal,
  isInternalEvent,
} from "../protocol_prefix";
import type { PresenceHistoryOptions } from "../options";

export type PresenceHistoryDirection = "newest_first" | "oldest_first";

export interface PresenceHistoryParams {
  direction?: PresenceHistoryDirection;
  limit?: number;
  cursor?: string;
  start_serial?: number;
  end_serial?: number;
  start_time_ms?: number;
  end_time_ms?: number;
  /** Ably-compatible alias for start_time_ms */
  start?: number;
  /** Ably-compatible alias for end_time_ms */
  end?: number;
}

export interface PresenceHistoryItem {
  stream_id: string;
  serial: number;
  published_at_ms: number;
  event: "member_added" | "member_removed";
  cause: string;
  user_id: string;
  connection_id?: string | null;
  dead_node_id?: string | null;
  payload_size_bytes: number;
  presence_event: Record<string, unknown>;
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
  direction: PresenceHistoryDirection;
  limit: number;
  has_more: boolean;
  next_cursor?: string | null;
  bounds: PresenceHistoryBounds;
  continuity: PresenceHistoryContinuity;

  hasNext(): boolean;
  next(): Promise<PresenceHistoryPage>;
}

export interface PresenceSnapshotParams {
  at_time_ms?: number;
  /** Ably-compatible alias for at_time_ms */
  at?: number;
  at_serial?: number;
}

export interface PresenceSnapshotMember {
  user_id: string;
  last_event: "member_added" | "member_removed";
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

export default class PresenceChannel extends PrivateChannel {
  members: Members;

  /** Adds presence channel functionality to private channels.
   *
   * @param {String} name
   * @param {Sockudo} sockudo
   */
  constructor(name: string, sockudo: Sockudo) {
    super(name, sockudo);
    this.members = new Members();
  }

  /** Authorizes the connection as a member of the channel.
   *
   * @param  {String} socketId
   * @param  {(...args: any[]) => any} callback
   */
  authorize(socketId: string, callback: (...args: any[]) => any) {
    super.authorize(socketId, async (error, authData) => {
      if (!error) {
        authData = authData as ChannelAuthorizationData;
        if (authData.channel_data != null) {
          const channelData = JSON.parse(authData.channel_data);
          this.members.setMyID(channelData.user_id);
        } else {
          await this.sockudo.user.signinDonePromise;
          if (this.sockudo.user.user_data != null) {
            // If the user is signed in, get the id of the authenticated user
            // and allow the presence authorization to continue.
            this.members.setMyID(this.sockudo.user.user_data.id);
          } else {
            let suffix = UrlStore.buildLogSuffix("authorizationEndpoint");
            Logger.error(
              `Invalid auth response for channel '${this.name}', ` +
                `expected 'channel_data' field. ${suffix}, ` +
                `or the user should be signed in.`,
            );
            callback("Invalid auth response");
            return;
          }
        }
      }
      callback(error, authData);
    });
  }

  /** Handles presence and subscription events. For internal use only.
   *
   * @param {SockudoEvent} event
   */
  handleEvent(event: SockudoEvent) {
    const eventName = event.event;
    if (isInternalEvent(eventName)) {
      this.handleInternalEvent(event);
    } else {
      const data = event.data;
      const metadata: Metadata = {};
      if (event.user_id) {
        metadata.user_id = event.user_id;
      }
      this.emit(eventName, data, metadata);
    }
  }
  handleInternalEvent(event: SockudoEvent) {
    const eventName = event.event;
    const data = event.data;
    switch (eventName) {
      case prefixedInternal("subscription_succeeded"):
        this.handleSubscriptionSucceededEvent(event);
        break;
      case prefixedInternal("subscription_count"):
        this.handleSubscriptionCountEvent(event);
        break;
      case prefixedInternal("member_added"):
        const addedMember = this.members.addMember(data);
        this.emit(prefixedEvent("member_added"), addedMember);
        break;
      case prefixedInternal("member_removed"):
        const removedMember = this.members.removeMember(data);
        if (removedMember) {
          this.emit(prefixedEvent("member_removed"), removedMember);
        }
        break;
    }
  }

  handleSubscriptionSucceededEvent(event: SockudoEvent) {
    this.subscriptionPending = false;
    this.subscribed = true;
    if (this.subscriptionCancelled) {
      this.sockudo.unsubscribe(this.name);
    } else {
      this.members.onSubscription(event.data);
      this.emit(prefixedEvent("subscription_succeeded"), this.members);
    }
  }

  /** Resets the channel state, including members map. For internal use only. */
  disconnect() {
    this.members.reset();
    super.disconnect();
  }

  /**
   * Fetches presence history for this channel via the configured backend proxy.
   *
   * Requires `presenceHistory.endpoint` to be set in the Sockudo client options.
   * The endpoint receives a POST with `{ channel, params, action: "history" }` and
   * must proxy the request to the Sockudo server's REST API using server-side HMAC auth.
   *
   * @param params - Query parameters: direction, limit, cursor, serial/time bounds
   * @returns Paged result with items, bounds, continuity, hasNext(), and next()
   */
  async history(
    params: PresenceHistoryParams = {},
  ): Promise<PresenceHistoryPage> {
    const historyConfig = this.sockudo.config.presenceHistory;
    if (!historyConfig?.endpoint) {
      throw new Error(
        "presenceHistory.endpoint must be configured to use presence.history(). " +
          "This endpoint should proxy requests to the Sockudo server REST API.",
      );
    }

    return this.fetchHistoryPage(historyConfig, params);
  }

  /**
   * Fetches a point-in-time snapshot of effective membership for this channel.
   *
   * Requires `presenceHistory.endpoint` to be set in the Sockudo client options.
   * The endpoint receives a POST with `{ channel, params, action: "snapshot" }` and
   * must proxy the request to the Sockudo server's snapshot REST API.
   *
   * @param params - Optional bounds: at_time_ms or at_serial
   * @returns Snapshot with members and continuity metadata
   */
  async snapshot(
    params: PresenceSnapshotParams = {},
  ): Promise<PresenceSnapshot> {
    const historyConfig = this.sockudo.config.presenceHistory;
    if (!historyConfig?.endpoint) {
      throw new Error(
        "presenceHistory.endpoint must be configured to use presence.snapshot(). " +
          "This endpoint should proxy requests to the Sockudo server REST API.",
      );
    }

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...historyConfig.headers,
      ...historyConfig.headersProvider?.(),
    };

    const response = await fetch(historyConfig.endpoint, {
      method: "POST",
      headers,
      body: JSON.stringify({
        channel: this.name,
        params,
        action: "snapshot",
      }),
      credentials: "same-origin",
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `Presence snapshot request failed (${response.status}): ${body}`,
      );
    }

    return response.json() as Promise<PresenceSnapshot>;
  }

  private async fetchHistoryPage(
    config: PresenceHistoryOptions,
    params: PresenceHistoryParams,
  ): Promise<PresenceHistoryPage> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...config.headers,
      ...config.headersProvider?.(),
    };

    const response = await fetch(config.endpoint, {
      method: "POST",
      headers,
      body: JSON.stringify({
        channel: this.name,
        params,
        action: "history",
      }),
      credentials: "same-origin",
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `Presence history request failed (${response.status}): ${body}`,
      );
    }

    const data = await response.json();
    return {
      ...data,
      hasNext(): boolean {
        return !!data.has_more && !!data.next_cursor;
      },
      next: (): Promise<PresenceHistoryPage> => {
        if (!data.has_more || !data.next_cursor) {
          return Promise.reject(new Error("No more pages available"));
        }
        return this.fetchHistoryPage(config, {
          ...params,
          cursor: data.next_cursor,
        });
      },
    };
  }
}
