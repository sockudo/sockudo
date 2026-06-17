import { createHash, createHmac } from "node:crypto";
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
  type PublishMessage,
  type SockudoClientPeer,
} from "@sockudo/ai-transport";
import { demoConfig } from "./config";

export function isAllowedDemoChannel(channelName: string): boolean {
  const config = demoConfig();
  return channelName === config.channelName || channelName.startsWith("private-ai-e2e-");
}

export function realtimeClient(): ClientLike {
  const config = demoConfig();
  const options = {
    cluster: "local",
    forceTLS: false,
    enabledTransports: ["ws"],
    wsHost: config.host,
    wsPort: config.port,
    wssPort: config.port,
    protocolVersion: 2,
    channelAuthorization: {
      transport: "ajax",
      endpoint: `${config.appBaseUrl}/api/channel-auth`,
    },
  } as unknown as ConstructorParameters<typeof Sockudo>[1];
  return createServerProxyClient(new Sockudo(config.appKey, options));
}

export function channelAuth(socketId: string, channelName: string): { auth: string } {
  const config = demoConfig();
  const signature = createHmac("sha256", config.appSecret)
    .update(`${socketId}:${channelName}`)
    .digest("hex");
  return { auth: `${config.appKey}:${signature}` };
}

export function capabilityJwt(clientId: string): {
  token: string;
  expiresAt: number;
} {
  const config = demoConfig();
  const now = Math.floor(Date.now() / 1000);
  const expiresAt = now + 15 * 60;
  const header = { alg: "HS256", kid: config.appKey, typ: "JWT" };
  const payload = {
    exp: expiresAt,
    iat: now,
    jti: `demo-${clientId}-${String(now)}`,
    "x-sockudo-client-id": clientId,
    "x-sockudo-capability": {
      [config.channelName]: ["publish", "subscribe", "history", "presence"],
      "private-ai-e2e-*": ["publish", "subscribe", "history", "presence"],
    },
  };
  const unsigned = `${base64Url(JSON.stringify(header))}.${base64Url(JSON.stringify(payload))}`;
  const signature = createHmac("sha256", config.appSecret).update(unsigned).digest("base64url");
  return { token: `${unsigned}.${signature}`, expiresAt };
}

export async function proxyVersionedMessage(body: Record<string, unknown>): Promise<unknown> {
  const channel = requireString(body.channel, "channel");
  if (!isAllowedDemoChannel(channel)) {
    throw createError({ statusCode: 400, statusMessage: "unexpected channel" });
  }
  if (body.action === "publish_create") {
    const payload = await signedSockudoRequest("POST", "/apps/{appId}/events", {
      name: stringOr(body.name, "sockudo:message.create"),
      channel,
      data: encodeData(body.data),
      extras: body.extras,
      message_id: stringOr(body.messageId, crypto.randomUUID()),
      client_id: optionalString(body.clientId),
      socket_id: optionalString(body.socketId),
      op_id: optionalString(body.opId),
    });
    return normalizeAck(payload, optionalString(body.messageSerial));
  }
  if (
    body.action === "message_append" ||
    body.action === "message_update" ||
    body.action === "message_delete"
  ) {
    const messageSerial = requireString(body.messageSerial, "messageSerial");
    const action =
      body.action === "message_append"
        ? "append"
        : body.action === "message_update"
          ? "update"
          : "delete";
    const payload = await signedSockudoRequest(
      "POST",
      `/apps/{appId}/channels/${encodeURIComponent(channel)}/messages/${encodeURIComponent(messageSerial)}/${action}`,
      {
        data: body.data ?? "",
        name: optionalString(body.name),
        extras: body.extras,
        client_id: optionalString(body.clientId),
        socket_id: optionalString(body.socketId),
        op_id: optionalString(body.opId),
      },
    );
    return normalizeAck(payload, messageSerial);
  }
  if (body.action === "channel_history") {
    const params = new URLSearchParams(
      Object.entries((body.params as Record<string, string> | undefined) ?? {}),
    );
    const query = params.toString();
    return signedSockudoRequest(
      "GET",
      `/apps/{appId}/channels/${encodeURIComponent(channel)}/history${query ? `?${query}` : ""}`,
    );
  }
  throw createError({ statusCode: 400, statusMessage: "unsupported action" });
}

function createServerProxyClient(raw: Sockudo): ClientLike {
  const realtime = adaptSockudoClient(raw as unknown as SockudoClientPeer);
  return {
    channels: {
      get(name, options) {
        const live = realtime.channels.get(name, options);
        return {
          ...live,
          publish(message) {
            return publishCreate(name, message);
          },
          appendMessage(messageSerial, data, mutation = {}) {
            return mutateMessage("append", name, messageSerial, {
              ...mutation,
              data,
            });
          },
          updateMessage(messageSerial, mutation = {}) {
            return mutateMessage("update", name, messageSerial, mutation);
          },
          deleteMessage(messageSerial, mutation = {}) {
            return mutateMessage("delete", name, messageSerial, mutation);
          },
          history(historyOptions = {}) {
            return readChannelHistory(name, historyOptions);
          },
        } satisfies ChannelLike;
      },
    },
    connection: realtime.connection,
    close: realtime.close,
  };
}

async function publishCreate(channel: string, message: PublishMessage): Promise<MessageAck> {
  const payload = await signedSockudoRequest("POST", "/apps/{appId}/events", {
    name: message.name ?? "sockudo:message.create",
    channel,
    data: encodeData(message.data),
    extras: message.extras,
    message_id: message.messageId ?? crypto.randomUUID(),
    client_id: message.clientId,
    socket_id: message.socketId,
    op_id: message.opId,
  });
  return normalizeAck(payload, message.messageSerial);
}

async function mutateMessage(
  action: "append" | "update" | "delete",
  channel: string,
  messageSerial: string,
  mutation: MessageMutation,
): Promise<MessageAck> {
  const payload = await signedSockudoRequest(
    "POST",
    `/apps/{appId}/channels/${encodeURIComponent(channel)}/messages/${encodeURIComponent(messageSerial)}/${action}`,
    {
      data: mutation.data ?? "",
      name: mutation.name,
      extras: mutation.extras,
      client_id: mutation.clientId,
      socket_id: mutation.socketId,
      op_id: mutation.opId,
    },
  );
  return normalizeAck(payload, messageSerial);
}

async function readChannelHistory(
  channel: string,
  options: HistoryOptions,
): Promise<PaginatedResult<InboundMessage>> {
  const params = new URLSearchParams(historyParams(options));
  const query = params.toString();
  const payload = await signedSockudoRequest(
    "GET",
    `/apps/{appId}/channels/${encodeURIComponent(channel)}/history${query ? `?${query}` : ""}`,
  );
  const record = asRecord(payload);
  const rawItems = Array.isArray(record.items) ? [...record.items] : [];
  if (options.direction === "newest_first") {
    rawItems.reverse();
  }
  const items = rawItems.map((item) => normalizeHistoryItem(item, Number(Date.now())));
  const cursor = typeof record.next === "string" ? record.next : undefined;
  return {
    items,
    hasNext() {
      return Boolean(cursor ?? record.has_more);
    },
    next() {
      return readChannelHistory(channel, cursor === undefined ? options : { ...options, cursor });
    },
  };
}

async function signedSockudoRequest(
  method: "GET" | "POST",
  rawPathWithQuery: string,
  body?: Record<string, unknown>,
): Promise<unknown> {
  const config = demoConfig();
  const pathWithQuery = rawPathWithQuery.replace("{appId}", config.appId);
  const rawBody = body === undefined ? undefined : JSON.stringify(stripUndefined(body));
  const url = new URL(`http://${config.host}:${String(config.port)}${pathWithQuery}`);
  const params = Object.fromEntries(url.searchParams.entries());
  params.auth_key = config.appKey;
  params.auth_timestamp = String(Math.floor(Date.now() / 1000));
  params.auth_version = "1.0";
  if (rawBody !== undefined) {
    params.body_md5 = createHash("md5").update(rawBody).digest("hex");
  }
  const sorted = new URLSearchParams(
    Object.entries(params).sort(([left], [right]) => left.localeCompare(right)),
  ).toString();
  const signature = createHmac("sha256", config.appSecret)
    .update([method, url.pathname, sorted].join("\n"))
    .digest("hex");
  url.search = `${sorted}&auth_signature=${signature}`;
  const init: RequestInit = { method };
  if (rawBody !== undefined) {
    init.headers = { "Content-Type": "application/json" };
    init.body = rawBody;
  }
  const response = await fetch(url, init);
  const text = await response.text();
  const payload = text ? (JSON.parse(text) as unknown) : {};
  if (!response.ok) {
    throw createError({
      statusCode: response.status,
      statusMessage: `Sockudo request failed: ${text}`,
    });
  }
  return payload;
}

function normalizeAck(payload: unknown, fallback?: string): MessageAck {
  const record = asRecord(payload);
  const channels = record.channels as Record<string, unknown> | undefined;
  const first = channels === undefined ? record : Object.values(channels)[0];
  const ack = asRecord(first) ?? record;
  const result: MessageAck = {
    messageSerial: stringOr(ack.message_serial ?? ack.messageSerial, fallback ?? "msg-demo"),
    historySerial: serialValue(ack.history_serial ?? ack.historySerial) ?? 1,
  };
  const deliverySerial = serialValue(ack.delivery_serial ?? ack.deliverySerial);
  if (deliverySerial !== undefined) {
    result.deliverySerial = deliverySerial;
  }
  const versionSerial = stringValue(ack.version_serial ?? ack.versionSerial);
  if (versionSerial !== undefined) {
    result.versionSerial = versionSerial;
  }
  return result;
}

function normalizeHistoryItem(item: unknown, fallbackSerial: number): InboundMessage {
  const wrapper = asRecord(item);
  const message = asRecord(wrapper.message ?? item);
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

function encodeData(value: unknown): string {
  return typeof value === "string" ? value : JSON.stringify(value ?? null);
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

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function serialValue(value: unknown): string | number | undefined {
  return typeof value === "string" || typeof value === "number" ? value : undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function stringOr(value: unknown, fallback: string): string {
  return typeof value === "string" && value.length > 0 ? value : fallback;
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw createError({ statusCode: 400, statusMessage: `missing ${field}` });
  }
  return value;
}

function stripUndefined(body: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(body).filter(([, value]) => value !== undefined));
}

function base64Url(value: string): string {
  return Buffer.from(value).toString("base64url");
}
