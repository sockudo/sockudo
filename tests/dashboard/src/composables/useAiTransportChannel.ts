import {
  normalizeInboundMessage,
  type ChannelEvents,
  type ChannelLike,
  type HistoryOptions,
  type InboundMessage,
  type MessageAck,
  type MessageListener,
  type MessageMutation,
  type PaginatedResult,
  type PresenceEventName,
  type PresenceLike,
  type PresenceMember,
  type PublishMessage,
  type Serial,
  type SubscribeOptions,
  type Unsubscribe,
} from '@sockudo/ai-transport'
import { useHttpApi, type ApiResponse } from './useHttpApi'
import { usePusher } from './usePusher'

const ACK_INFO = 'message_serial,history_serial,delivery_serial,version_serial'

function serial(value: unknown): Serial | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim()) return value
  return undefined
}

function text(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value : undefined
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' ? value as Record<string, unknown> : {}
}

function compact(source: Record<string, unknown>) {
  return Object.fromEntries(Object.entries(source).filter(([, value]) => value !== undefined))
}

function hasKeys(value: Record<string, unknown>) {
  return Object.keys(value).length > 0
}

function aiCodecType(extras: unknown) {
  return text(record(record(record(extras).ai).codec).type)
}

function decodeWireData(value: unknown, extras: unknown) {
  if (typeof value !== 'string') return value
  const codecType = aiCodecType(extras)
  if (codecType?.startsWith('text-') || codecType?.startsWith('reasoning-')) return value
  const trimmed = value.trim()
  if (!trimmed || !/^(?:[{[]|null$|true$|false$|-?\d)/.test(trimmed)) return value
  try {
    return JSON.parse(trimmed)
  } catch {
    return value
  }
}

function apiError(res: ApiResponse, label: string) {
  if (res.status >= 200 && res.status < 300) return ''
  const data = res.data as any
  return data?.error ?? data?.message ?? `${label} failed with HTTP ${res.status}`
}

function normalizeAck(res: ApiResponse, channelName: string, fallbackSerial?: string): MessageAck {
  const data = record(res.data)
  const channels = record(data.channels)
  const channelAck = record(channels[channelName] ?? channels[decodeURIComponent(channelName)])
  const source = Object.keys(channelAck).length ? channelAck : data
  const messageSerial =
    source.messageSerial ?? source.message_serial ?? data.messageSerial ?? data.message_serial ?? fallbackSerial
  const historySerial = serial(source.historySerial ?? source.history_serial ?? data.historySerial ?? data.history_serial)

  if (typeof messageSerial !== 'string' || historySerial === undefined) {
    throw new Error(`Sockudo did not return a complete acknowledgement for ${channelName}.`)
  }

  return {
    messageSerial,
    historySerial,
    deliverySerial: serial(source.deliverySerial ?? source.delivery_serial ?? data.deliverySerial ?? data.delivery_serial),
    versionSerial: typeof (source.versionSerial ?? source.version_serial) === 'string'
      ? String(source.versionSerial ?? source.version_serial)
      : undefined,
    status: typeof source.status === 'string' ? source.status : undefined,
  }
}

function mutationBody(mutation: MessageMutation | Omit<MessageMutation, 'data'> = {}, data?: unknown) {
  return compact({
    data,
    name: mutation.name,
    extras: mutation.extras,
    clear_fields: mutation.clearFields,
    client_id: mutation.clientId,
    socket_id: mutation.socketId,
    description: mutation.description,
    metadata: mutation.metadata,
    op_id: mutation.opId,
  })
}

function publishData(data: unknown) {
  if (data === undefined) return undefined
  return typeof data === 'string' ? data : JSON.stringify(data)
}

function flattenRaw(raw: any, channelName: string) {
  const rawRecord = record(raw)
  const historyMessage = record(rawRecord.message)
  const rawData = hasKeys(historyMessage) ? historyMessage : rawRecord.data
  const candidate = record(rawData)
  const versionEnvelope = hasKeys(historyMessage) || 'message' in candidate || 'message_serial' in candidate || 'action' in candidate
  const envelope = versionEnvelope ? candidate : rawRecord
  const message = record(versionEnvelope ? (candidate.message ?? candidate) : rawRecord)
  const extras = message.extras ?? envelope.extras ?? rawRecord.extras
  const extrasHeaders = record(record(extras).headers)
  const data = versionEnvelope ? (message.data ?? envelope.data) : rawData
  const codecType = aiCodecType(extras)
  const rawAction = extrasHeaders.sockudo_action ?? rawRecord.operation_kind ?? envelope.action ?? message.action
  const action =
    (rawAction === 'message.create' || rawAction === 'create') &&
    typeof data === 'string' &&
    (codecType === 'text-end' || codecType === 'reasoning-end')
      ? 'message.update'
      : rawAction
  const versionSerial = text(extrasHeaders.sockudo_version_serial)
  const versionTimestampMs = Number(extrasHeaders.sockudo_version_timestamp_ms)
  const version = envelope.version ?? message.version ?? (versionSerial
    ? compact({
        serial: versionSerial,
        timestamp_ms: Number.isFinite(versionTimestampMs) ? versionTimestampMs : undefined,
      })
    : undefined)

  return {
    event: message.event ?? envelope.event ?? rawRecord.event ?? rawRecord.event_name ?? '',
    channel: message.channel ?? envelope.channel ?? rawRecord.channel ?? channelName,
    data: decodeWireData(data, extras),
    name: message.name ?? envelope.name ?? rawRecord.event_name,
    user_id: message.user_id ?? envelope.user_id ?? rawRecord.user_id,
    stream_id: message.stream_id ?? envelope.stream_id ?? rawRecord.stream_id,
    message_id: message.message_id ?? envelope.message_id ?? rawRecord.message_id ?? rawRecord.messageId,
    serial: serial(envelope.delivery_serial ?? envelope.deliverySerial ?? message.serial ?? rawRecord.serial),
    extras,
    action,
    message_serial:
      extrasHeaders.sockudo_message_serial ??
      envelope.message_serial ??
      envelope.messageSerial ??
      message.message_serial ??
      message.messageSerial,
    history_serial:
      extrasHeaders.sockudo_history_serial ??
      envelope.history_serial ??
      envelope.historySerial ??
      message.history_serial ??
      message.historySerial ??
      rawRecord.serial,
    delivery_serial: envelope.delivery_serial ?? envelope.deliverySerial,
    version_serial: versionSerial,
    version,
  }
}

function inboundFromRaw(raw: unknown, channelName: string): InboundMessage {
  return normalizeInboundMessage(flattenRaw(raw, channelName) as any)
}

function inboundDedupeKey(message: InboundMessage): string {
  const versionSerial = message.version?.serial
  if (versionSerial) return `${message.name}:${message.action}:${message.messageSerial}:${versionSerial}`
  return `${message.name}:${message.action}:${message.messageSerial}:${message.historySerial}:${JSON.stringify(message.data)}`
}

function historyQuery(options: HistoryOptions) {
  return compact({
    limit: options.limit === undefined ? undefined : String(options.limit),
    direction: options.direction,
    cursor: options.cursor,
    start: options.start === undefined ? undefined : String(options.start),
    end: options.end === undefined ? undefined : String(options.end),
    start_serial: options.startSerial === undefined ? undefined : String(options.startSerial),
    end_serial: options.endSerial === undefined ? undefined : String(options.endSerial),
    start_time_ms: options.startTimeMs === undefined ? undefined : String(options.startTimeMs),
    end_time_ms: options.endTimeMs === undefined ? undefined : String(options.endTimeMs),
  }) as Record<string, string>
}

const emptyPresence: PresenceLike = {
  enter: async () => undefined,
  update: async () => undefined,
  leave: async () => undefined,
  get: async (): Promise<readonly PresenceMember[]> => [],
  subscribe: (_listener: (event: PresenceEventName, member: PresenceMember) => void) => () => undefined,
}

function noChannelEvent<K extends keyof ChannelEvents>(
  _event: K,
  _listener: (payload: ChannelEvents[K]) => void,
): Unsubscribe {
  return () => undefined
}

export function createDashboardAiChannel(channelName: string): ChannelLike {
  const http = useHttpApi()
  const pusher = usePusher()

  async function history(options: HistoryOptions = {}): Promise<PaginatedResult<InboundMessage>> {
    const res = await http.getChannelHistory(channelName, historyQuery(options))
    const error = apiError(res, 'AI history')
    if (error) throw new Error(error)

    const data = record(res.data)
    const rawItems = Array.isArray(data.items) ? data.items : []
    const direction = options.direction ?? 'newest_first'
    const orderedItems = direction === 'newest_first' || direction === 'backwards' || direction === 'reverse'
      ? [...rawItems].reverse()
      : rawItems
    const items = orderedItems.map((item) => inboundFromRaw(item, channelName))
    const nextCursor = typeof data.next_cursor === 'string' ? data.next_cursor : undefined
    const hasMore = Boolean(data.has_more && nextCursor)

    return {
      items,
      hasNext: () => hasMore,
      next: () => history({ ...options, cursor: nextCursor }),
    }
  }

  return {
    name: channelName,
    attachSerial: undefined,
    presence: emptyPresence,
    async publish(message: PublishMessage): Promise<MessageAck> {
      const res = await http.publishAdvancedEvent(compact({
        name: message.name,
        channel: channelName,
        data: publishData(message.data),
        info: ACK_INFO,
        message_id: message.messageId ?? message.messageSerial,
        idempotency_key: message.opId,
        socket_id: message.socketId,
        extras: message.extras,
      }))
      const error = apiError(res, message.name ?? 'AI publish')
      if (error) {
        console.warn('[ai-channel] publish failed', message.name, res.status, res.data)
        throw new Error(error)
      }
      return normalizeAck(res, channelName, message.messageSerial)
    },
    async appendMessage(messageSerial: string, data: string, mutation: Omit<MessageMutation, 'data'> = {}) {
      const res = await http.appendMessage(channelName, messageSerial, mutationBody(mutation, data))
      const error = apiError(res, 'AI append')
      if (error) throw new Error(error)
      return normalizeAck(res, channelName, messageSerial)
    },
    async updateMessage(messageSerial: string, mutation: MessageMutation = {}) {
      const res = await http.updateMessage(channelName, messageSerial, mutationBody(mutation, mutation.data))
      const error = apiError(res, 'AI update')
      if (error) throw new Error(error)
      return normalizeAck(res, channelName, messageSerial)
    },
    async deleteMessage(messageSerial: string, mutation: MessageMutation = {}) {
      const res = await http.deleteMessage(channelName, messageSerial, mutationBody(mutation, mutation.data))
      const error = apiError(res, 'AI delete')
      if (error) throw new Error(error)
      return normalizeAck(res, channelName, messageSerial)
    },
    subscribe(listener: MessageListener, options?: SubscribeOptions): Unsubscribe {
      const names = options?.names ? new Set(options.names) : undefined
      const seen = new Set<string>()
      const seenOrder: string[] = []
      return pusher.bindRawEvent(channelName, (raw) => {
        const message = inboundFromRaw(raw, channelName)
        const key = inboundDedupeKey(message)
        if (seen.has(key)) return
        seen.add(key)
        seenOrder.push(key)
        if (seenOrder.length > 512) {
          const oldest = seenOrder.shift()
          if (oldest) seen.delete(oldest)
        }
        if (!names || names.has(message.name)) listener(message)
      })
    },
    history,
    on: noChannelEvent,
  }
}
