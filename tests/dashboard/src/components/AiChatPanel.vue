<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from 'vue'
import {
  Bot,
  CheckCircle2,
  Loader2,
  MessageSquarePlus,
  RefreshCcw,
  RotateCcw,
  Send,
  Settings2,
  Square,
  Trash2,
  Wifi,
} from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { useHttpApi, type ApiResponse } from '../composables/useHttpApi'
import { usePusher } from '../composables/usePusher'

type ChatRole = 'user' | 'assistant' | 'system'
type ChatStatus = 'pending' | 'streaming' | 'complete' | 'cancelled' | 'error'
type AgentMode = 'demo' | 'external'

interface ChatMessage {
  id: string
  role: ChatRole
  content: string
  status: ChatStatus
  turnId?: string
  messageId?: string
  messageSerial?: string
  replyToId?: string
  error?: string
  createdAtMs: number
  updatedAtMs: number
}

const store = useDashboardStore()
const { connect, subscribe, bindEvent } = usePusher()

const channel = ref('private-ai-demo')
const clientId = ref('demo-user')
const agentMode = ref<AgentMode>('demo')
const composer = ref('')
const messages = ref<ChatMessage[]>([])
const showSettings = ref(false)
const loadingHistory = ref(false)
const sending = ref(false)
const cancelling = ref(false)
const lastNotice = ref('')
const lastError = ref('')
const scrollEl = ref<HTMLElement | null>(null)
const activeRun = ref<{ turnId: string; assistantId: string; cancelled: boolean } | null>(null)
const boundChannels = new Set<string>()

const isConnected = computed(() => store.connectionState === 'connected')
const isBusy = computed(() => sending.value || messages.value.some((message) => message.status === 'streaming' || message.status === 'pending'))
const activeAssistant = computed(() => {
  const run = activeRun.value
  return run ? messages.value.find((message) => message.id === run.assistantId) : undefined
})

const emptyPrompts = [
  'Show how Sockudo recovers an interrupted AI stream.',
  'Explain how push notifications can follow an AI turn.',
  'Describe why append rollup is safe for durable history.',
]

function now() {
  return Date.now()
}

function randomSuffix() {
  return Math.random().toString(36).slice(2, 8)
}

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms))
}

function statusLabel(status: ChatStatus) {
  if (status === 'pending') return 'pending'
  if (status === 'streaming') return 'streaming'
  if (status === 'cancelled') return 'stopped'
  if (status === 'error') return 'failed'
  return 'done'
}

function chatMessage(role: ChatRole, content: string, status: ChatStatus, extra: Partial<ChatMessage> = {}): ChatMessage {
  const timestamp = now()
  return {
    id: `${role}-${timestamp}-${randomSuffix()}`,
    role,
    content,
    status,
    createdAtMs: timestamp,
    updatedAtMs: timestamp,
    ...extra,
  }
}

function aiExtras(turnId: string, status: 'streaming' | 'complete' | 'cancelled', role: 'user' | 'assistant') {
  return {
    ai: {
      transport: {
        'turn-id': turnId,
        status,
        role,
      },
      codec: {
        'content-type': 'application/json',
      },
    },
  }
}

function getTransport(extras: any) {
  return extras?.ai?.transport ?? extras?.transport ?? {}
}

function getTurnId(extras: any, fallback?: string) {
  return getTransport(extras)?.['turn-id'] ?? getTransport(extras)?.turn_id ?? getTransport(extras)?.turnId ?? fallback
}

function getTransportStatus(extras: any): ChatStatus {
  const status = getTransport(extras)?.status
  if (status === 'complete' || status === 'cancelled') return status
  if (status === 'streaming') return 'streaming'
  return 'complete'
}

function textFromData(data: unknown): string {
  if (typeof data === 'string') {
    try {
      const parsed = JSON.parse(data)
      return textFromData(parsed)
    } catch {
      return data
    }
  }
  if (data && typeof data === 'object') {
    const record = data as Record<string, unknown>
    if (typeof record.content === 'string') return record.content
    if (typeof record.text === 'string') return record.text
    if (typeof record.body === 'string') return record.body
    if (typeof record.message === 'string') return record.message
    return JSON.stringify(data, null, 2)
  }
  return data == null ? '' : String(data)
}

function responseError(res: ApiResponse, label: string) {
  const data = res.data as any
  if (res.status >= 200 && res.status < 300) return ''
  return data?.error ?? data?.message ?? `${label} failed with HTTP ${res.status}`
}

function extractAckSerial(res: ApiResponse, targetChannel: string) {
  const data = res.data as any
  const channels = data?.channels ?? {}
  const ack = channels[targetChannel] ?? channels[decodeURIComponent(targetChannel)]
  return ack?.message_serial ?? ack?.messageSerial ?? ''
}

function upsertMessage(message: ChatMessage) {
  const bySerial = message.messageSerial
    ? messages.value.findIndex((existing) => existing.messageSerial === message.messageSerial)
    : -1
  const byId = messages.value.findIndex((existing) => existing.id === message.id)
  const index = bySerial >= 0 ? bySerial : byId
  if (index >= 0) {
    messages.value[index] = {
      ...messages.value[index],
      ...message,
      id: messages.value[index].id,
      createdAtMs: messages.value[index].createdAtMs,
      updatedAtMs: now(),
    }
  } else {
    messages.value.push(message)
  }
  sortMessages()
}

function updateMessage(id: string, patch: Partial<ChatMessage>) {
  const index = messages.value.findIndex((message) => message.id === id)
  if (index < 0) return
  messages.value[index] = {
    ...messages.value[index],
    ...patch,
    updatedAtMs: now(),
  }
}

function sortMessages() {
  messages.value = [...messages.value].sort((left, right) => left.createdAtMs - right.createdAtMs)
}

async function scrollToBottom() {
  await nextTick()
  if (scrollEl.value) scrollEl.value.scrollTop = scrollEl.value.scrollHeight
}

async function ensureConnection() {
  if (isConnected.value) return
  connect()
  const start = now()
  while (!isConnected.value && now() - start < 5000) {
    await sleep(100)
  }
  if (!isConnected.value) {
    throw new Error('Could not connect to Sockudo.')
  }
}

function bindChatEvents(targetChannel: string) {
  if (boundChannels.has(targetChannel)) return
  boundChannels.add(targetChannel)

  bindEvent(targetChannel, 'ai-input', (data: any) => {
    const turnId = data?.turn_id ?? data?.turnId ?? `turn-${now()}`
    upsertMessage(chatMessage('user', textFromData(data), 'complete', {
      id: `user-${turnId}`,
      turnId,
      messageId: `ai-input-${turnId}`,
    }))
  })

  for (const event of ['sockudo:message.create', 'sockudo:message.append', 'sockudo:message.update', 'sockudo:message.delete', 'ai-output']) {
    bindEvent(targetChannel, event, (data: any) => {
      mergeAssistantFrame(data, event)
    })
  }
}

async function ensureSubscribed() {
  const targetChannel = channel.value.trim()
  if (!targetChannel) throw new Error('Choose a channel first.')
  await ensureConnection()
  subscribe(targetChannel)
  bindChatEvents(targetChannel)
}

function mergeAssistantFrame(data: any, eventName = '') {
  const envelope = data?.message ? data : data?.item ?? data
  const message = envelope?.message ?? envelope
  const extras = message?.extras ?? envelope?.extras
  const name = message?.name ?? envelope?.name
  const serial = envelope?.message_serial ?? envelope?.messageSerial ?? message?.message_serial ?? message?.messageSerial
  const run = activeRun.value
  const turnId = getTurnId(extras, run?.turnId)
  const isAiOutput = name === 'ai-output' || eventName === 'ai-output' || Boolean(extras?.ai)
  if (!isAiOutput && !serial) return

  const activeAssistantId = run && turnId && run.turnId === turnId ? run.assistantId : undefined
  const fallbackId = `assistant-${turnId ?? now()}`
  const existing = messages.value.find((entry) =>
    (serial && entry.messageSerial === serial) ||
    (activeAssistantId && entry.id === activeAssistantId) ||
    entry.id === fallbackId,
  )
  const id = serial ? `assistant-${serial}` : (existing?.id ?? activeAssistantId ?? fallbackId)
  const incomingText = textFromData(message?.data ?? envelope?.data ?? data)
  const status = eventName.includes('delete') ? 'cancelled' : getTransportStatus(extras)
  const nextStatus = existing?.status === 'complete' && status === 'streaming' ? existing.status : status
  const content = eventName === 'ai-output' && !incomingText && existing
    ? existing.content
    : eventName.includes('append') && existing && incomingText.length < existing.content.length
      ? `${existing.content}${incomingText}`
      : incomingText

  upsertMessage(chatMessage('assistant', content, nextStatus, {
    id,
    turnId,
    messageSerial: serial ? String(serial) : existing?.messageSerial,
    replyToId: turnId ? `user-${turnId}` : existing?.replyToId,
  }))

  if (activeRun.value?.turnId === turnId && (nextStatus === 'complete' || nextStatus === 'cancelled' || nextStatus === 'error')) {
    activeRun.value = null
    sending.value = false
    lastNotice.value = ''
  }
}

function demoReply(prompt: string) {
  const subject = prompt.replace(/\s+/g, ' ').trim().slice(0, 120)
  return `For "${subject}", Sockudo carries the user input as an AI event, streams the assistant response as versioned appends, and persists the final aggregate for rewind and recovery. If the browser reconnects, the chat can rebuild from durable history while live subscribers keep receiving the same turn.`
}

function chunkReply(reply: string) {
  const words = reply.split(/\s+/).filter(Boolean)
  const chunks: string[] = []
  for (let i = 0; i < words.length; i += 7) {
    chunks.push(`${i === 0 ? '' : ' '}${words.slice(i, i + 7).join(' ')}`)
  }
  return chunks
}

async function publishAiInput(turnId: string, text: string) {
  const res = await useHttpApi().publishAdvancedEvent({
    name: 'ai-input',
    channel: channel.value.trim(),
    data: JSON.stringify({
      role: 'user',
      content: text,
      turn_id: turnId,
      client_id: clientId.value.trim(),
    }),
    message_id: `ai-input-${turnId}`,
    info: 'message_serial,history_serial,delivery_serial,version_serial',
    extras: aiExtras(turnId, 'complete', 'user'),
  })
  const error = responseError(res, 'ai-input')
  if (error) throw new Error(error)
}

async function createAiOutput(turnId: string) {
  const messageId = `ai-output-${turnId}`
  const res = await useHttpApi().publishAdvancedEvent({
    name: 'ai-output',
    channel: channel.value.trim(),
    data: '',
    message_id: messageId,
    info: 'message_serial,history_serial,delivery_serial,version_serial',
    extras: aiExtras(turnId, 'streaming', 'assistant'),
  })
  const error = responseError(res, 'ai-output')
  if (error) throw new Error(error)
  const serial = extractAckSerial(res, channel.value.trim())
  if (!serial) throw new Error('Sockudo did not return a message serial for ai-output.')
  return { messageId, messageSerial: String(serial) }
}

async function appendAiChunk(turnId: string, messageSerial: string, chunk: string, index: number) {
  const res = await useHttpApi().appendMessage(channel.value.trim(), messageSerial, {
    data: chunk,
    extras: aiExtras(turnId, 'streaming', 'assistant'),
    op_id: `${turnId}-append-${index + 1}`,
    description: `chat chunk ${index + 1}`,
  })
  const error = responseError(res, 'message.append')
  if (error) throw new Error(error)
}

async function finishAiOutput(turnId: string, messageSerial: string, status: 'complete' | 'cancelled') {
  const res = await useHttpApi().updateMessage(channel.value.trim(), messageSerial, {
    extras: aiExtras(turnId, status, 'assistant'),
    op_id: `${turnId}-${status}`,
    description: `chat ${status}`,
  })
  const error = responseError(res, 'message.update')
  if (error) throw new Error(error)
}

async function sendMessage(textOverride?: string) {
  const text = (textOverride ?? composer.value).trim()
  if (!text || sending.value) return
  lastError.value = ''
  lastNotice.value = ''
  sending.value = true

  const turnId = `turn-${now()}-${randomSuffix()}`
  const user = chatMessage('user', text, 'pending', {
    id: `user-${turnId}`,
    turnId,
    messageId: `ai-input-${turnId}`,
  })
  const assistant = chatMessage('assistant', '', agentMode.value === 'demo' ? 'pending' : 'streaming', {
    id: `assistant-pending-${turnId}`,
    turnId,
    replyToId: user.id,
  })
  messages.value.push(user, assistant)
  composer.value = ''
  activeRun.value = { turnId, assistantId: assistant.id, cancelled: false }
  await scrollToBottom()

  try {
    await ensureSubscribed()
    await publishAiInput(turnId, text)
    updateMessage(user.id, { status: 'complete' })

    if (agentMode.value === 'external') {
      lastNotice.value = 'Waiting for an agent response...'
      sending.value = false
      return
    }

    const output = await createAiOutput(turnId)
    updateMessage(assistant.id, {
      id: `assistant-${output.messageSerial}`,
      status: 'streaming',
      messageId: output.messageId,
      messageSerial: output.messageSerial,
    })
    activeRun.value = { turnId, assistantId: `assistant-${output.messageSerial}`, cancelled: false }

    const reply = demoReply(text)
    for (const [index, chunk] of chunkReply(reply).entries()) {
      if (activeRun.value?.cancelled) break
      await appendAiChunk(turnId, output.messageSerial, chunk, index)
      const current = messages.value.find((message) => message.messageSerial === output.messageSerial)
      updateMessage(current?.id ?? `assistant-${output.messageSerial}`, {
        content: `${current?.content ?? ''}${chunk}`,
        status: 'streaming',
      })
      await scrollToBottom()
      await sleep(90)
    }

    if (activeRun.value?.cancelled) {
      await finishAiOutput(turnId, output.messageSerial, 'cancelled')
      updateMessage(`assistant-${output.messageSerial}`, { status: 'cancelled' })
    } else {
      await finishAiOutput(turnId, output.messageSerial, 'complete')
      updateMessage(`assistant-${output.messageSerial}`, { status: 'complete' })
    }
  } catch (error: any) {
    lastError.value = error.message ?? 'The chat turn failed.'
    updateMessage(user.id, { status: user.status === 'pending' ? 'error' : user.status })
    updateMessage(activeRun.value?.assistantId ?? assistant.id, {
      status: 'error',
      error: lastError.value,
      content: activeAssistant.value?.content ?? '',
    })
  } finally {
    sending.value = false
    if (agentMode.value === 'demo') activeRun.value = null
    await scrollToBottom()
  }
}

async function cancelTurn() {
  const run = activeRun.value
  if (!run || cancelling.value) return
  cancelling.value = true
  run.cancelled = true
  lastNotice.value = 'Stopping...'

  try {
    await useHttpApi().publishAdvancedEvent({
      name: 'ai-cancel',
      channel: channel.value.trim(),
      data: JSON.stringify({
        turn_id: run.turnId,
        client_id: clientId.value.trim(),
      }),
      message_id: `ai-cancel-${run.turnId}-${now()}`,
      extras: aiExtras(run.turnId, 'cancelled', 'user'),
    })

    const assistant = messages.value.find((message) => message.id === run.assistantId)
    if (assistant?.messageSerial) {
      await finishAiOutput(run.turnId, assistant.messageSerial, 'cancelled')
    }
    updateMessage(run.assistantId, { status: 'cancelled' })
    activeRun.value = null
    lastNotice.value = ''
  } catch (error: any) {
    lastError.value = error.message ?? 'Could not stop the turn.'
  } finally {
    cancelling.value = false
  }
}

async function retryAssistant(message: ChatMessage) {
  const user = messages.value.find((entry) => entry.id === message.replyToId)
  if (!user) return
  await sendMessage(user.content)
}

async function newChat() {
  if (activeRun.value) await cancelTurn()
  messages.value = []
  lastError.value = ''
  lastNotice.value = ''
}

function normalizeHistoryItem(item: any) {
  const envelope = item?.message ?? {}
  const message = envelope?.message ?? envelope
  const event = item?.event_name ?? message?.event ?? envelope?.event
  const extras = message?.extras ?? envelope?.extras
  const turnId = getTurnId(extras, item?.message_id)
  const data = message?.data ?? envelope?.data

  if (event === 'ai-input') {
    return chatMessage('user', textFromData(data), 'complete', {
      id: `user-${turnId ?? item.serial}`,
      turnId,
      messageId: item?.message_id,
      createdAtMs: item?.published_at_ms ?? now(),
    })
  }

  const name = message?.name ?? envelope?.name
  const serial = envelope?.message_serial ?? envelope?.messageSerial ?? item?.message_id
  if (name === 'ai-output' || Boolean(extras?.ai)) {
    return chatMessage('assistant', textFromData(data), getTransportStatus(extras), {
      id: `assistant-${serial ?? item.serial}`,
      turnId,
      messageSerial: serial ? String(serial) : undefined,
      messageId: item?.message_id,
      replyToId: turnId ? `user-${turnId}` : undefined,
      createdAtMs: item?.published_at_ms ?? now(),
    })
  }

  return null
}

async function loadHistory() {
  if (!channel.value.trim() || loadingHistory.value) return
  loadingHistory.value = true
  lastError.value = ''
  lastNotice.value = ''

  try {
    const res = await useHttpApi().getChannelHistory(channel.value.trim(), {
      limit: '100',
      direction: 'oldest_first',
    })
    const error = responseError(res, 'history')
    if (error) throw new Error(error)

    const recovered = ((res.data as any)?.items ?? [])
      .map(normalizeHistoryItem)
      .filter(Boolean) as ChatMessage[]
    messages.value = []
    for (const message of recovered) upsertMessage(message)
    lastNotice.value = recovered.length ? `Recovered ${recovered.length} messages.` : 'No retained chat history.'
  } catch (error: any) {
    lastError.value = error.message ?? 'Could not load chat history.'
  } finally {
    loadingHistory.value = false
    await scrollToBottom()
  }
}

function formatTime(ms: number) {
  return new Date(ms).toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
  })
}

watch(messages, scrollToBottom, { deep: true })
watch(() => store.socketId, () => {
  boundChannels.clear()
  if (isConnected.value && channel.value.trim()) {
    ensureSubscribed().catch(() => {})
  }
})

onMounted(async () => {
  try {
    if (isConnected.value) await ensureSubscribed()
    await loadHistory()
  } catch {
    // The visible chat state already reports connection/history errors.
  }
})
</script>

<template>
  <div class="h-[calc(100vh-4rem)] min-h-[620px] animate-fade-in flex flex-col">
    <div class="panel flex-1 min-h-0 overflow-hidden flex flex-col">
      <header class="px-4 py-3 border-b border-surface-700/40 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div class="flex items-center gap-3 min-w-0">
          <div class="w-9 h-9 rounded-lg bg-brand-500/15 text-brand-300 flex items-center justify-center">
            <Bot class="w-5 h-5" />
          </div>
          <div class="min-w-0">
            <h2 class="text-sm font-semibold text-surface-50">AI Chat</h2>
            <div class="flex items-center gap-2 text-xs text-surface-500">
              <span :class="['w-2 h-2 rounded-full', isConnected ? 'bg-emerald-400' : 'bg-surface-500']" />
              <span>{{ isConnected ? 'connected' : store.connectionState }}</span>
              <span class="text-surface-700">/</span>
              <span class="font-mono truncate max-w-[220px]">{{ channel }}</span>
            </div>
          </div>
        </div>

        <div class="flex items-center gap-2">
          <div class="flex gap-1 p-1 bg-surface-800/70 rounded-lg">
            <button
              v-for="mode in (['demo', 'external'] as const)"
              :key="mode"
              @click="agentMode = mode"
              :class="[agentMode === mode ? 'tab-btn-active' : 'tab-btn']"
            >
              {{ mode === 'demo' ? 'Demo agent' : 'External agent' }}
            </button>
          </div>
          <button @click="showSettings = !showSettings" class="btn-icon" title="Chat settings">
            <Settings2 class="w-4 h-4" />
          </button>
          <button @click="loadHistory" :disabled="loadingHistory" class="btn-icon" title="Recover history">
            <RefreshCcw :class="['w-4 h-4', loadingHistory ? 'animate-spin' : '']" />
          </button>
          <button @click="newChat" class="btn-icon" title="New chat">
            <Trash2 class="w-4 h-4" />
          </button>
        </div>
      </header>

      <div v-if="showSettings" class="px-4 py-3 border-b border-surface-700/40 grid grid-cols-1 md:grid-cols-3 gap-3 bg-surface-950/25">
        <div>
          <label class="text-[10px] text-surface-500 mb-1 block">Channel</label>
          <input v-model="channel" class="input-field font-mono text-xs" />
        </div>
        <div>
          <label class="text-[10px] text-surface-500 mb-1 block">Client ID</label>
          <input v-model="clientId" class="input-field font-mono text-xs" />
        </div>
        <div>
          <label class="text-[10px] text-surface-500 mb-1 block">Rollup</label>
          <select v-model.number="store.config.appendRollupWindow" class="input-field font-mono text-xs">
            <option :value="0">0 ms</option>
            <option :value="20">20 ms</option>
            <option :value="40">40 ms</option>
            <option :value="100">100 ms</option>
            <option :value="500">500 ms</option>
          </select>
        </div>
      </div>

      <div v-if="lastError || lastNotice" class="px-4 pt-3">
        <div
          :class="[
            'rounded-lg border px-3 py-2 text-xs',
            lastError
              ? 'bg-red-500/10 border-red-500/25 text-red-300'
              : 'bg-brand-500/10 border-brand-500/25 text-brand-300',
          ]"
        >
          {{ lastError || lastNotice }}
        </div>
      </div>

      <div ref="scrollEl" class="flex-1 min-h-0 overflow-y-auto px-4 py-5">
        <div v-if="messages.length === 0" class="h-full flex items-center justify-center">
          <div class="max-w-xl w-full text-center space-y-4">
            <div class="w-12 h-12 mx-auto rounded-xl bg-brand-500/15 text-brand-300 flex items-center justify-center">
              <MessageSquarePlus class="w-6 h-6" />
            </div>
            <div>
              <h3 class="text-base font-semibold text-surface-100">Start an AI Transport chat</h3>
              <p class="text-sm text-surface-500 mt-1">Pick a prompt or type your own.</p>
            </div>
            <div class="grid grid-cols-1 gap-2">
              <button
                v-for="prompt in emptyPrompts"
                :key="prompt"
                @click="sendMessage(prompt)"
                :disabled="sending"
                class="text-left rounded-lg border border-surface-700/40 bg-surface-800/50 hover:bg-surface-800 px-3 py-2 text-sm text-surface-300 transition-colors"
              >
                {{ prompt }}
              </button>
            </div>
          </div>
        </div>

        <div v-else class="space-y-4">
          <div
            v-for="message in messages"
            :key="message.id"
            :class="['flex gap-3', message.role === 'user' ? 'justify-end' : 'justify-start']"
          >
            <div v-if="message.role !== 'user'" class="w-8 h-8 rounded-lg bg-brand-500/15 text-brand-300 flex items-center justify-center shrink-0 mt-1">
              <Bot class="w-4 h-4" />
            </div>
            <div :class="['max-w-[78%] space-y-1', message.role === 'user' ? 'items-end' : 'items-start']">
              <div
                :class="[
                  'rounded-xl px-4 py-3 text-sm leading-relaxed whitespace-pre-wrap',
                  message.role === 'user'
                    ? 'bg-brand-600 text-white'
                    : message.status === 'error'
                      ? 'bg-red-500/10 text-red-200 border border-red-500/25'
                      : 'bg-surface-800/80 text-surface-100 border border-surface-700/35',
                ]"
              >
                <span v-if="message.content">{{ message.content }}</span>
                <span v-else class="inline-flex items-center gap-2 text-surface-400">
                  <Loader2 class="w-3.5 h-3.5 animate-spin" /> Thinking
                </span>
              </div>
              <div :class="['flex items-center gap-2 text-[10px] text-surface-500', message.role === 'user' ? 'justify-end' : 'justify-start']">
                <span>{{ formatTime(message.createdAtMs) }}</span>
                <span v-if="message.status !== 'complete'" :class="message.status === 'error' ? 'text-red-400' : message.status === 'cancelled' ? 'text-amber-400' : 'text-brand-400'">
                  {{ statusLabel(message.status) }}
                </span>
                <button
                  v-if="message.role === 'assistant' && (message.status === 'error' || message.status === 'cancelled')"
                  @click="retryAssistant(message)"
                  class="inline-flex items-center gap-1 text-brand-300 hover:text-brand-200"
                >
                  <RotateCcw class="w-3 h-3" /> Retry
                </button>
              </div>
              <p v-if="message.error" class="text-[11px] text-red-300">{{ message.error }}</p>
            </div>
            <div v-if="message.role === 'user'" class="w-8 h-8 rounded-lg bg-surface-700 text-surface-200 flex items-center justify-center shrink-0 mt-1">
              {{ clientId.charAt(0).toUpperCase() || 'U' }}
            </div>
          </div>
        </div>
      </div>

      <footer class="border-t border-surface-700/40 p-4">
        <div class="flex items-end gap-2">
          <button v-if="!isConnected" @click="connect" class="btn-secondary flex items-center gap-2">
            <Wifi class="w-4 h-4" /> Connect
          </button>
          <textarea
            v-model="composer"
            class="input-field min-h-[48px] max-h-[140px] resize-none text-sm"
            placeholder="Message Sockudo AI..."
            :disabled="sending"
            @keydown.enter.exact.prevent="sendMessage()"
          />
          <button
            v-if="activeRun"
            @click="cancelTurn"
            :disabled="cancelling"
            class="btn-secondary h-12 w-12 flex items-center justify-center"
            title="Stop"
          >
            <Square class="w-4 h-4" />
          </button>
          <button
            v-else
            @click="sendMessage()"
            :disabled="!composer.trim() || sending"
            class="btn-primary h-12 w-12 flex items-center justify-center"
            title="Send"
          >
            <Send class="w-4 h-4" />
          </button>
        </div>
        <div class="mt-2 flex items-center justify-between text-[10px] text-surface-600">
          <span>{{ agentMode === 'demo' ? 'Demo agent streams through Sockudo.' : 'Waiting for an external agent on the channel.' }}</span>
          <span v-if="isBusy" class="inline-flex items-center gap-1 text-brand-400">
            <Loader2 class="w-3 h-3 animate-spin" /> active
          </span>
          <span v-else class="inline-flex items-center gap-1 text-emerald-400">
            <CheckCircle2 class="w-3 h-3" /> ready
          </span>
        </div>
      </footer>
    </div>
  </div>
</template>
