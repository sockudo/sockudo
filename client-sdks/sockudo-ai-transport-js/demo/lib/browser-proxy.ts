import Sockudo from "@sockudo/client";
import {
  adaptSockudoClient,
  getCodecHeaders,
  getTransportHeaders,
  type ChannelLike,
  type ClientLike,
  type HistoryOptions,
  type InboundMessage,
  type InboundMessageAction,
  type MessageAck,
  type MessageMutation,
  type PaginatedResult,
  type SockudoClientPeer,
} from "@sockudo/ai-transport";

export interface BrowserSockudoConfig {
  appKey: string;
  host: string;
  port: number;
}

export function createBrowserSockudoClient(
  config: BrowserSockudoConfig,
  clientId: string,
): { raw: Sockudo; realtime: ClientLike } {
  const options = {
    cluster: "local",
    forceTLS: false,
    enabledTransports: ["ws"],
    wsHost: config.host,
    wsPort: config.port,
    wssPort: config.port,
    protocolVersion: 2,
    clientId,
    channelAuthorization: { transport: "ajax", endpoint: "/api/channel-auth" },
    versionedMessages: { endpoint: "/api/versioned-messages" },
  } as unknown as ConstructorParameters<typeof Sockudo>[1];
  const raw = new Sockudo(config.appKey, options);
  return {
    raw,
    realtime: createBrowserProxyClient(raw, clientId),
  };
}

function createBrowserProxyClient(raw: Sockudo, clientId: string): ClientLike {
  const realtime = adaptSockudoClient(raw as unknown as SockudoClientPeer);
  return {
    channels: {
      get(name, options) {
        const live = realtime.channels.get(name, options);
        return {
          ...live,
          publish(message) {
            return versionedProxy<MessageAck>({
              action: "publish_create",
              channel: name,
              name: message.name,
              data: message.data,
              extras: message.extras,
              messageId: message.messageId,
              messageSerial: message.messageSerial,
              clientId: message.clientId ?? clientId,
              socketId: message.socketId,
              opId: message.opId,
            });
          },
          appendMessage(messageSerial, data, mutation = {}) {
            return versionedProxy<MessageAck>({
              action: "message_append",
              channel: name,
              messageSerial,
              data,
              ...mutationPayload(mutation, clientId),
            });
          },
          updateMessage(messageSerial, mutation = {}) {
            return versionedProxy<MessageAck>({
              action: "message_update",
              channel: name,
              messageSerial,
              ...mutationPayload(mutation, clientId),
            });
          },
          deleteMessage(messageSerial, mutation = {}) {
            return versionedProxy<MessageAck>({
              action: "message_delete",
              channel: name,
              messageSerial,
              ...mutationPayload(mutation, clientId),
            });
          },
          history(historyOptions = {}) {
            return readProxyHistory(name, historyOptions);
          },
        } satisfies ChannelLike;
      },
    },
    connection: realtime.connection,
    close: realtime.close,
  };
}

async function readProxyHistory(
  channel: string,
  options: HistoryOptions,
): Promise<PaginatedResult<InboundMessage>> {
  const payload = await versionedProxy<{
    items?: unknown;
    next?: string;
    has_more?: boolean;
  }>({
    action: "channel_history",
    channel,
    params: historyParams(options),
  });
  const rawItems = Array.isArray(payload.items) ? [...payload.items] : [];
  if (options.direction === "newest_first") {
    rawItems.reverse();
  }
  const items = rawItems.map((item) => normalizeProxyHistoryItem(item, Number(Date.now())));
  const cursor = typeof payload.next === "string" ? payload.next : undefined;
  return {
    items,
    hasNext() {
      return Boolean(cursor ?? payload.has_more);
    },
    next() {
      return readProxyHistory(channel, cursor === undefined ? options : { ...options, cursor });
    },
  };
}

function normalizeProxyHistoryItem(item: unknown, fallbackSerial: number): InboundMessage {
  const wrapper = record(item);
  const message = record(wrapper.message ?? item);
  const serial = stringValue(message.message_serial ?? message.messageSerial) ?? "";
  const historySerial = serialValue(wrapper.serial ?? message.serial) ?? fallbackSerial;
  const extras = message.extras;
  const transport = getTransportHeaders(extras);
  return {
    name: stringValue(message.name) ?? stringValue(message.event) ?? "",
    data: parseJsonish(message.data),
    action: historyAction(message.action, message.data, transport),
    messageSerial: serial,
    historySerial,
    timestamp: Number(message.time_ms ?? message.timestamp ?? Date.now()),
    raw: item,
    ...optional("messageId", stringValue(message.message_id ?? message.messageId)),
    ...optional("extras", extras),
    getTransportHeaders() {
      return transport;
    },
    getCodecHeaders() {
      return getCodecHeaders(extras);
    },
  };
}

function historyAction(
  action: unknown,
  data: unknown,
  transport: Record<string, string>,
): InboundMessageAction {
  if (
    action === "create" ||
    action === "append" ||
    action === "update" ||
    action === "delete" ||
    action === "summary"
  ) {
    return action;
  }
  return transport.stream === "true" && typeof data === "string" ? "update" : "create";
}

function mutationPayload(
  mutation: Omit<MessageMutation, "data"> | MessageMutation,
  clientId: string,
): Record<string, unknown> {
  return {
    name: mutation.name,
    data: "data" in mutation ? mutation.data : undefined,
    extras: mutation.extras,
    clearFields: mutation.clearFields,
    opId: mutation.opId,
    clientId: mutation.clientId ?? clientId,
    socketId: mutation.socketId,
    description: mutation.description,
    metadata: mutation.metadata,
  };
}

function historyParams(options: HistoryOptions): Record<string, string> {
  return Object.fromEntries(
    Object.entries({
      limit: options.limit,
      direction: options.direction,
      cursor: options.cursor,
      start: options.start,
      end: options.end,
      startSerial: options.startSerial,
      endSerial: options.endSerial,
      startTimeMs: options.startTimeMs,
      endTimeMs: options.endTimeMs,
    })
      .filter(([, value]) => value !== undefined)
      .map(([key, value]) => [key, String(value)]),
  );
}

async function versionedProxy<T>(body: Record<string, unknown>): Promise<T> {
  const response = await fetch("/api/versioned-messages", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(stripUndefined(body)),
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`versioned proxy failed ${String(response.status)}: ${text}`);
  }
  return (text.length > 0 ? JSON.parse(text) : {}) as T;
}

function stripUndefined(body: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(body).filter(([, value]) => value !== undefined));
}

function optional<K extends string, V>(
  key: K,
  value: V | undefined,
): Record<K, V> | Record<string, never> {
  return value === undefined ? {} : ({ [key]: value } as Record<K, V>);
}

function parseJsonish(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }
  const trimmed = value.trimStart();
  if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
    return value;
  }
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function serialValue(value: unknown): string | number | undefined {
  return typeof value === "string" || typeof value === "number" ? value : undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}
