import { ref } from 'vue'
import { useDashboardStore } from '../stores/dashboard'
import { useHttpApi, type ApiResponse } from './useHttpApi'
import { usePusher } from './usePusher'

export type DemoSenderId = 'realtime' | 'history' | 'mutable' | 'push' | 'ai'

export interface DemoSenderStatus {
  state: 'idle' | 'running' | 'ok' | 'error'
  label: string
  detail: string
  updatedAtMs?: number
}

const senderLabels: Record<DemoSenderId, string> = {
  realtime: 'Realtime',
  history: 'History',
  mutable: 'Mutable',
  push: 'Push',
  ai: 'AI Transport',
}

function jsonData(value: unknown) {
  return JSON.stringify(value)
}

function emptyStatuses(): Record<DemoSenderId, DemoSenderStatus> {
  return {
    realtime: { state: 'idle', label: senderLabels.realtime, detail: 'Subscribe and publish live events.' },
    history: { state: 'idle', label: senderLabels.history, detail: 'Seed durable channel history.' },
    mutable: { state: 'idle', label: senderLabels.mutable, detail: 'Create, edit, append, annotate.' },
    push: { state: 'idle', label: senderLabels.push, detail: 'Publish a notification payload.' },
    ai: { state: 'idle', label: senderLabels.ai, detail: 'Run AI input, appends, completion, push.' },
  }
}

const runningSender = ref<DemoSenderId | null>(null)
const senderStatuses = ref<Record<DemoSenderId, DemoSenderStatus>>(emptyStatuses())

function apiOk(res: ApiResponse | null | undefined) {
  return Boolean(res && res.status >= 200 && res.status < 300)
}

function responseText(res: ApiResponse | null | undefined, fallback: string) {
  const data = res?.data as any
  return data?.error ?? data?.message ?? fallback
}

function ackSerial(res: ApiResponse | null | undefined, channel: string) {
  const data = res?.data as any
  const channels = data?.channels ?? {}
  const ack = channels[channel] ?? channels[decodeURIComponent(channel)]
  return ack?.message_serial ?? ack?.messageSerial ?? ''
}

function setStatus(id: DemoSenderId, status: Omit<DemoSenderStatus, 'label' | 'updatedAtMs'>) {
  senderStatuses.value = {
    ...senderStatuses.value,
    [id]: {
      ...status,
      label: senderLabels[id],
      updatedAtMs: Date.now(),
    },
  }
}

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms))
}

export function useDemoSenders() {
  const store = useDashboardStore()
  const pusher = usePusher()

  function isConnected() {
    return store.connectionState === 'connected'
  }

  async function ensureConnected() {
    if (isConnected()) return
    pusher.connect()
    const start = Date.now()
    while (!isConnected() && Date.now() - start < 5000) {
      await sleep(100)
    }
    if (!isConnected()) {
      throw new Error('Connect to Sockudo first or start the local server.')
    }
  }

  function subscribeIfMissing(channel: string) {
    if (store.connectionState !== 'connected' || store.channels.has(channel)) return
    pusher.subscribe(channel)
  }

  async function publishRealtime() {
    await ensureConnected()
    const api = useHttpApi()
    for (const channel of ['chat:demo', 'ticker:btc', 'presence-room']) {
      subscribeIfMissing(channel)
    }

    const sent = [
      await api.publishEvent(
        'chat:demo',
        'demo.message',
        JSON.stringify({ from: 'dashboard', text: 'Hello from the demo sender', sent_at_ms: Date.now() }),
      ),
      await api.publishEvent(
        'ticker:btc',
        'price.tick',
        JSON.stringify({ symbol: 'BTC', price: 68250 + Math.round(Math.random() * 500), sent_at_ms: Date.now() }),
        { symbol: 'BTC', source: 'demo' },
      ),
    ]
    if (sent.some((res) => !apiOk(res))) {
      throw new Error(responseText(sent.find((res) => !apiOk(res)), 'Realtime publish failed.'))
    }
    return 'Subscribed demo channels and sent chat plus ticker events.'
  }

  async function publishHistory() {
    await ensureConnected()
    const api = useHttpApi()
    const channel = 'history-room'
    subscribeIfMissing(channel)
    const count = 5
    for (let i = 0; i < count; i++) {
      const messageId = `story-history-${Date.now()}-${i}`
      const res = await api.publishAdvancedEvent({
        name: 'demo.history',
        channel,
        data: JSON.stringify({
          step: i + 1,
          body: `durable history demo event ${i + 1}`,
          sent_at_ms: Date.now(),
        }),
        message_id: messageId,
        info: 'message_serial,history_serial,delivery_serial',
      })
      if (!apiOk(res)) throw new Error(responseText(res, 'History publish failed.'))
    }
    await api.getChannelHistory(channel, { limit: '10', direction: 'newest_first' })
    return `Published ${count} retained events and fetched recent history.`
  }

  async function publishMutable() {
    await ensureConnected()
    const api = useHttpApi()
    const channel = 'chat:mutable'
    subscribeIfMissing(channel)
    const messageId = `story-versioned-${Date.now()}`
    const create = await api.publishAdvancedEvent({
      name: 'demo.message',
      channel,
      data: jsonData({ text: 'Draft order note', status: 'draft' }),
      message_id: messageId,
      info: 'message_serial,history_serial,delivery_serial,version_serial',
      extras: { demo: { sender: 'showcase' } },
    })
    if (!apiOk(create)) throw new Error(responseText(create, 'Mutable create failed.'))
    const serial = String(ackSerial(create, channel))
    if (!serial) throw new Error('Sockudo did not return a message serial.')

    const update = await api.updateMessage(channel, serial, {
      data: jsonData({ text: 'Published order note', status: 'published' }),
      description: 'showcase update',
      op_id: `${messageId}-update`,
    })
    if (!apiOk(update)) throw new Error(responseText(update, 'Mutable update failed.'))

    const append = await api.appendMessage(channel, serial, {
      data: ' with an appended audit trail',
      description: 'showcase append',
      op_id: `${messageId}-append`,
    })
    if (!apiOk(append)) throw new Error(responseText(append, 'Mutable append failed.'))

    await api.publishAnnotation(channel, serial, {
      type: 'reactions:distinct.v1',
      name: 'thumbsup',
      clientId: 'demo-user',
      count: 1,
    })
    await api.getMessageVersions(channel, serial, { limit: '10', direction: 'oldest_first' })
    return `Created serial ${serial}, updated it, appended text, and added an annotation.`
  }

  async function publishPush() {
    const api = useHttpApi()
    const publishId = `story-push-${Date.now()}`
    const res = await api.publishPush({
      publishId,
      recipients: [{ type: 'channel', channel: 'private-ai-demo' }],
      payload: {
        title: 'Sockudo demo',
        body: 'Push payload published from the one-click sender.',
        templateData: {
          channel: 'private-ai-demo',
          source: 'showcase',
        },
        collapseKey: 'dashboard-demo',
      },
      sync: true,
    }, 'sync')
    if (!apiOk(res)) throw new Error(responseText(res, 'Push publish failed.'))
    await api.getPushPublishStatus(publishId)
    return `Published push payload ${publishId} to private-ai-demo.`
  }

  async function publishAiTransport() {
    await ensureConnected()
    const api = useHttpApi()
    const channel = 'private-ai-demo'
    const turnId = `story-turn-${Date.now()}`
    subscribeIfMissing(channel)

    const extras = (status: 'streaming' | 'complete', role: 'user' | 'assistant') => ({
      ai: {
        transport: {
          'run-id': turnId,
          status,
          role,
        },
        codec: {
          'content-type': 'application/json',
        },
      },
    })

    const input = await api.publishAdvancedEvent({
      name: 'ai-input',
      channel,
      data: JSON.stringify({
        role: 'user',
        content: 'Demo the AI Transport lifecycle.',
        turn_id: turnId,
        client_id: 'demo-user',
      }),
      message_id: `ai-input-${turnId}`,
      info: 'message_serial,history_serial,delivery_serial,version_serial',
      extras: extras('complete', 'user'),
    })
    if (!apiOk(input)) throw new Error(responseText(input, 'AI input failed.'))

    const output = await api.publishAdvancedEvent({
      name: 'ai-output',
      channel,
      data: '',
      message_id: `ai-output-${turnId}`,
      info: 'message_serial,history_serial,delivery_serial,version_serial',
      extras: extras('streaming', 'assistant'),
    })
    if (!apiOk(output)) throw new Error(responseText(output, 'AI output create failed.'))
    const serial = String(ackSerial(output, channel))
    if (!serial) throw new Error('Sockudo did not return an AI output serial.')

    const chunks = [
      'Sockudo records each AI turn as durable input and versioned output.',
      ' Streaming chunks arrive as appends, while rollup keeps live delivery efficient.',
      ' The final aggregate can be rewound, recovered, and paired with push.',
    ]
    for (let i = 0; i < chunks.length; i++) {
      const append = await api.appendMessage(channel, serial, {
        data: chunks[i],
        extras: extras('streaming', 'assistant'),
        op_id: `${turnId}-append-${i + 1}`,
        description: `showcase AI chunk ${i + 1}`,
      })
      if (!apiOk(append)) throw new Error(responseText(append, 'AI append failed.'))
    }

    const finish = await api.updateMessage(channel, serial, {
      extras: extras('complete', 'assistant'),
      op_id: `${turnId}-complete`,
      description: 'showcase AI complete',
    })
    if (!apiOk(finish)) throw new Error(responseText(finish, 'AI completion failed.'))

    await api.publishPush({
      publishId: `story-ai-push-${Date.now()}`,
      recipients: [{ type: 'channel', channel }],
      payload: {
        title: 'AI turn complete',
        body: 'Sockudo finished an AI Transport stream.',
        templateData: { turn_id: turnId, channel },
        collapseKey: 'ai-transport',
      },
      sync: true,
    }, 'sync')

    await api.getChannelHistory(channel, { limit: '10', direction: 'newest_first' })
    return `Ran AI turn ${turnId}, appended ${chunks.length} chunks, completed it, and published push.`
  }

  async function runSender(id: DemoSenderId) {
    if (runningSender.value) return
    runningSender.value = id
    setStatus(id, { state: 'running', detail: 'Running...' })
    try {
      const detail = id === 'realtime'
        ? await publishRealtime()
        : id === 'history'
          ? await publishHistory()
          : id === 'mutable'
            ? await publishMutable()
            : id === 'push'
              ? await publishPush()
              : await publishAiTransport()
      setStatus(id, { state: 'ok', detail })
    } catch (error: any) {
      setStatus(id, { state: 'error', detail: error?.message ?? 'Sender failed.' })
    } finally {
      runningSender.value = null
    }
  }

  return {
    runningSender,
    senderStatuses,
    runSender,
  }
}
