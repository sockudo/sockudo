<script setup lang="ts">
import { computed, ref } from 'vue'
import { Bot, CheckCircle2, History, ListTree, Radio, Send, Square, Workflow } from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { useHttpApi, type ApiResponse } from '../composables/useHttpApi'
import { usePusher } from '../composables/usePusher'

const store = useDashboardStore()
const { subscribe } = usePusher()

const channel = ref('private-ai-demo')
const turnId = ref(`turn-${Date.now()}`)
const userId = ref('demo-user')
const inputText = ref('Summarize Sockudo recovery, push, and AI Transport in one paragraph.')
const outputMessageId = ref(`ai-output-${Date.now()}`)
const outputMessageSerial = ref('')
const chunkText = ref('Sockudo combines durable history\\nwith versioned mutable messages\\nso AI streams can recover, rewind, and fan out safely.')
const terminalStatus = ref<'complete' | 'cancelled'>('complete')

const inputResult = ref<ApiResponse | null>(null)
const outputCreateResult = ref<ApiResponse | null>(null)
const appendResults = ref<ApiResponse[]>([])
const terminalResult = ref<ApiResponse | null>(null)
const latestResult = ref<ApiResponse | null>(null)
const versionsResult = ref<ApiResponse | null>(null)
const historyResult = ref<ApiResponse | null>(null)
const channelResult = ref<ApiResponse | null>(null)

const isConnected = computed(() => store.connectionState === 'connected')
const subscribed = computed(() => store.channels.has(channel.value))
const chunks = computed(() => chunkText.value.split('\n').map((line) => line.trim()).filter(Boolean))

function fmtResponse(data: unknown) {
  return typeof data === 'string' ? data : JSON.stringify(data, null, 2)
}

function aiExtras(status: 'streaming' | 'complete' | 'cancelled', role: 'user' | 'assistant') {
  return {
    ai: {
      transport: {
        'turn-id': turnId.value,
        status,
        role,
      },
      codec: {
        'content-type': 'application/json',
      },
    },
  }
}

function extractOutputSerial(res: ApiResponse | null) {
  const data = res?.data as any
  const ack = data?.channels?.[channel.value]
  return ack?.message_serial ?? ack?.messageSerial ?? ''
}

function subscribeAiChannel() {
  if (!isConnected.value || subscribed.value || !channel.value.trim()) return
  subscribe(channel.value.trim())
}

async function publishInput() {
  inputResult.value = await useHttpApi().publishAdvancedEvent({
    name: 'ai-input',
    channel: channel.value.trim(),
    data: JSON.stringify({
      role: 'user',
      content: inputText.value,
      turn_id: turnId.value,
      client_id: userId.value,
    }),
    message_id: `ai-input-${turnId.value}`,
    info: 'message_serial,history_serial,delivery_serial,version_serial',
    extras: aiExtras('complete', 'user'),
  })
}

async function createOutput() {
  outputCreateResult.value = await useHttpApi().publishAdvancedEvent({
    name: 'ai-output',
    channel: channel.value.trim(),
    data: '',
    message_id: outputMessageId.value.trim(),
    info: 'message_serial,history_serial,delivery_serial,version_serial',
    extras: aiExtras('streaming', 'assistant'),
  })
  const serial = extractOutputSerial(outputCreateResult.value)
  if (serial) outputMessageSerial.value = String(serial)
}

async function appendChunks() {
  if (!outputMessageSerial.value.trim()) return
  appendResults.value = []
  const api = useHttpApi()
  for (let i = 0; i < chunks.value.length; i++) {
    const res = await api.appendMessage(channel.value.trim(), outputMessageSerial.value.trim(), {
      data: `${i === 0 ? '' : ' '}${chunks.value[i]}`,
      extras: aiExtras('streaming', 'assistant'),
      op_id: `${turnId.value}-append-${i + 1}`,
      description: `AI stream chunk ${i + 1}`,
    })
    appendResults.value.push(res)
  }
}

async function finishStream() {
  if (!outputMessageSerial.value.trim()) return
  terminalResult.value = await useHttpApi().updateMessage(
    channel.value.trim(),
    outputMessageSerial.value.trim(),
    {
      extras: aiExtras(terminalStatus.value, 'assistant'),
      op_id: `${turnId.value}-${terminalStatus.value}`,
      description: `AI stream ${terminalStatus.value}`,
    },
  )
}

async function runTurn() {
  appendResults.value = []
  terminalResult.value = null
  if (isConnected.value && !subscribed.value) subscribeAiChannel()
  await publishInput()
  await createOutput()
  await appendChunks()
  await finishStream()
  await readAll()
}

async function readAll() {
  const api = useHttpApi()
  channelResult.value = await api.getChannel(channel.value.trim(), 'subscription_count')
  historyResult.value = await api.getChannelHistory(channel.value.trim(), {
    limit: '25',
    direction: 'newest_first',
  })
  if (outputMessageSerial.value.trim()) {
    latestResult.value = await api.getMessage(channel.value.trim(), outputMessageSerial.value.trim())
    versionsResult.value = await api.getMessageVersions(channel.value.trim(), outputMessageSerial.value.trim(), {
      limit: '50',
      direction: 'oldest_first',
    })
  }
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div class="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
      <div>
        <h2 class="text-lg font-bold text-surface-50">AI Transport</h2>
        <p class="text-sm text-surface-400 mt-1">AI lifecycle events on durable history, versioned messages, and append rollup.</p>
      </div>
      <div class="panel p-3 min-w-[240px]">
        <div class="flex items-center justify-between text-xs">
          <span class="text-surface-500">append_rollup_window</span>
          <span class="font-mono text-brand-300">{{ store.config.appendRollupWindow }} ms</span>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-3 gap-6">
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Workflow class="w-4 h-4 text-brand-400" /> Turn Setup
        </h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">AI Channel</label>
          <input v-model="channel" class="input-field font-mono text-xs" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <input v-model="turnId" class="input-field font-mono text-xs" placeholder="turn id" />
          <input v-model="userId" class="input-field font-mono text-xs" placeholder="client id" />
        </div>
        <textarea v-model="inputText" class="input-field text-xs h-24 resize-none" />
        <div class="grid grid-cols-2 gap-2">
          <button @click="subscribeAiChannel" :disabled="!isConnected || subscribed" class="btn-secondary btn-sm flex items-center justify-center gap-2">
            <Radio class="w-4 h-4" /> {{ subscribed ? 'Subscribed' : 'Subscribe' }}
          </button>
          <button @click="publishInput" class="btn-primary btn-sm flex items-center justify-center gap-2">
            <Send class="w-4 h-4" /> ai-input
          </button>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Bot class="w-4 h-4 text-brand-400" /> Assistant Stream
        </h3>
        <input v-model="outputMessageId" class="input-field font-mono text-xs" placeholder="message_id" />
        <input v-model="outputMessageSerial" class="input-field font-mono text-xs" placeholder="message serial" />
        <textarea v-model="chunkText" class="input-field text-xs h-28 resize-none" />
        <div class="grid grid-cols-2 gap-2">
          <button @click="createOutput" class="btn-primary btn-sm">Create</button>
          <button @click="appendChunks" :disabled="!outputMessageSerial.trim()" class="btn-secondary btn-sm">Append {{ chunks.length }}</button>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <select v-model="terminalStatus" class="input-field font-mono text-xs">
            <option value="complete">complete</option>
            <option value="cancelled">cancelled</option>
          </select>
          <button @click="finishStream" :disabled="!outputMessageSerial.trim()" class="btn-primary btn-sm flex items-center justify-center gap-2">
            <CheckCircle2 v-if="terminalStatus === 'complete'" class="w-4 h-4" />
            <Square v-else class="w-4 h-4" />
            Finish
          </button>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <History class="w-4 h-4 text-brand-400" /> Evidence
        </h3>
        <div class="grid grid-cols-2 gap-2">
          <button @click="runTurn" class="btn-primary btn-sm">Run Turn</button>
          <button @click="readAll" class="btn-secondary btn-sm">Read All</button>
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div class="bg-surface-800/50 border border-surface-700/30 rounded-lg p-3">
            <p class="text-[10px] text-surface-500 uppercase">Appends</p>
            <p class="text-lg font-mono font-bold text-surface-100">{{ appendResults.length }}</p>
          </div>
          <div class="bg-surface-800/50 border border-surface-700/30 rounded-lg p-3">
            <p class="text-[10px] text-surface-500 uppercase">Terminal</p>
            <p class="text-lg font-mono font-bold" :class="terminalResult ? 'text-emerald-400' : 'text-surface-400'">{{ terminalResult ? terminalStatus : 'none' }}</p>
          </div>
        </div>
        <div class="rounded-lg border border-surface-700/30 bg-surface-800/40 p-3 text-xs text-surface-400 space-y-1">
          <p><span class="text-surface-500">Lifecycle:</span> ai-input, ai-output, append, terminal append</p>
          <p><span class="text-surface-500">Reads:</span> channel AI stats, latest aggregate, versions, history</p>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-4 gap-6">
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Lifecycle</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse({ input: inputResult?.data, create: outputCreateResult?.data, appends: appendResults.map((r) => r.data), terminal: terminalResult?.data }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Channel / History</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse({ channel: channelResult?.data, history: historyResult?.data }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Latest Aggregate</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse(latestResult?.data ?? null) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <ListTree class="w-4 h-4 text-brand-400" /> Versions
        </h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse(versionsResult?.data ?? null) }}</pre>
      </div>
    </div>
  </div>
</template>
