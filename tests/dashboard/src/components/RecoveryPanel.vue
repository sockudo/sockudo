<script setup lang="ts">
import { computed, ref } from 'vue'
import { History, Radio, RotateCcw, Search, Send, Unplug, Wifi } from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { useHttpApi, type ApiResponse } from '../composables/useHttpApi'
import { usePusher } from '../composables/usePusher'

const store = useDashboardStore()
const { subscribe, disconnect, connect, sendResume } = usePusher()

const channel = ref('history-room')
const eventName = ref('demo.history')
const burstCount = ref(5)
const publishing = ref(false)
const publishLines = ref<string[]>([])

const historyLimit = ref('25')
const historyDirection = ref<'newest_first' | 'oldest_first'>('newest_first')
const historyCursor = ref('')
const historyResult = ref<ApiResponse | null>(null)
const stateResult = ref<ApiResponse | null>(null)

const resumeSerial = ref('')
const resumeStreamId = ref('')
const resumeLastMessageId = ref('')

const isConnected = computed(() => store.connectionState === 'connected')
const subscribed = computed(() => store.channels.has(channel.value))

function fmtResponse(data: unknown) {
  return typeof data === 'string' ? data : JSON.stringify(data, null, 2)
}

function historyParams() {
  const params: Record<string, string> = {
    limit: historyLimit.value || '25',
    direction: historyDirection.value,
  }
  if (historyCursor.value.trim()) params.cursor = historyCursor.value.trim()
  return params
}

function handleSubscribe() {
  if (!isConnected.value || !channel.value.trim()) return
  subscribe(channel.value.trim())
}

async function publishBurst() {
  if (!channel.value.trim()) return
  const api = useHttpApi()
  publishing.value = true
  publishLines.value = []
  for (let i = 0; i < burstCount.value; i++) {
    const messageId = `demo-${Date.now()}-${i}`
    const payload = JSON.stringify({
      index: i + 1,
      message_id: messageId,
      body: `durable history sample ${i + 1}`,
      sent_at_ms: Date.now(),
    })
    const res = await api.publishAdvancedEvent({
      name: eventName.value,
      channel: channel.value.trim(),
      data: payload,
      message_id: messageId,
      info: 'message_serial,history_serial,delivery_serial',
    })
    publishLines.value.push(`[${i + 1}] HTTP ${res.status}`)
  }
  publishing.value = false
}

async function fetchHistory() {
  if (!channel.value.trim()) return
  historyResult.value = await useHttpApi().getChannelHistory(channel.value.trim(), historyParams())
}

async function fetchState() {
  if (!channel.value.trim()) return
  stateResult.value = await useHttpApi().getChannelHistoryState(channel.value.trim())
}

function useNextCursor() {
  const cursor = historyResult.value?.data && typeof historyResult.value.data === 'object'
    ? (historyResult.value.data as any).next_cursor
    : undefined
  if (cursor) historyCursor.value = cursor
}

function applyPositionFromHistory() {
  const data = historyResult.value?.data as any
  const items = Array.isArray(data?.items) ? data.items : []
  const item = items[0]
  if (!item) return
  const serial = item.serial ?? item.history_serial ?? item.delivery_serial
  if (serial != null) resumeSerial.value = String(serial)
  if (item.stream_id) resumeStreamId.value = String(item.stream_id)
  if (item.message_id) resumeLastMessageId.value = String(item.message_id)
}

function sendResumeFrame() {
  if (!channel.value.trim() || !resumeSerial.value.trim()) return
  const position: Record<string, unknown> = {
    serial: Number(resumeSerial.value),
  }
  if (resumeStreamId.value.trim()) position.stream_id = resumeStreamId.value.trim()
  if (resumeLastMessageId.value.trim()) position.last_message_id = resumeLastMessageId.value.trim()
  sendResume({ [channel.value.trim()]: position })
}

function reconnect() {
  disconnect()
  window.setTimeout(connect, 500)
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">History, Rewind, Recovery</h2>
      <p class="text-sm text-surface-400 mt-1">Durable channel history plus Protocol V2 resume positions.</p>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-3 gap-6">
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Send class="w-4 h-4 text-brand-400" /> Produce Durable Events
        </h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Channel</label>
          <input v-model="channel" class="input-field font-mono" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Event</label>
            <input v-model="eventName" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Count</label>
            <input v-model.number="burstCount" type="number" min="1" max="50" class="input-field font-mono" />
          </div>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button @click="handleSubscribe" :disabled="!isConnected || subscribed" class="btn-secondary btn-sm flex items-center justify-center gap-2">
            <Radio class="w-4 h-4" /> {{ subscribed ? 'Subscribed' : 'Subscribe' }}
          </button>
          <button @click="publishBurst" :disabled="publishing" class="btn-primary btn-sm flex items-center justify-center gap-2">
            <Send class="w-4 h-4" /> {{ publishing ? 'Publishing...' : 'Publish' }}
          </button>
        </div>
        <div v-if="publishLines.length" class="space-y-1 max-h-[150px] overflow-y-auto">
          <div v-for="line in publishLines" :key="line" class="text-xs font-mono bg-emerald-500/5 text-emerald-400/80 px-3 py-1 rounded">
            {{ line }}
          </div>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <History class="w-4 h-4 text-brand-400" /> History Reader
        </h3>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Limit</label>
            <input v-model="historyLimit" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Direction</label>
            <select v-model="historyDirection" class="input-field font-mono">
              <option value="newest_first">newest_first</option>
              <option value="oldest_first">oldest_first</option>
            </select>
          </div>
        </div>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Cursor</label>
          <input v-model="historyCursor" class="input-field font-mono text-xs" />
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button @click="fetchHistory" class="btn-primary btn-sm flex items-center justify-center gap-2">
            <History class="w-4 h-4" /> Fetch
          </button>
          <button @click="fetchState" class="btn-secondary btn-sm flex items-center justify-center gap-2">
            <Search class="w-4 h-4" /> State
          </button>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button @click="useNextCursor" :disabled="!(historyResult?.data as any)?.next_cursor" class="btn-secondary btn-sm">Use Cursor</button>
          <button @click="applyPositionFromHistory" :disabled="!historyResult" class="btn-secondary btn-sm">Use Position</button>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <RotateCcw class="w-4 h-4 text-brand-400" /> V2 Resume
        </h3>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Serial</label>
            <input v-model="resumeSerial" class="input-field font-mono" placeholder="1" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Stream ID</label>
            <input v-model="resumeStreamId" class="input-field font-mono text-xs" />
          </div>
        </div>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Last Message ID</label>
          <input v-model="resumeLastMessageId" class="input-field font-mono text-xs" />
        </div>
        <button @click="sendResumeFrame" :disabled="!isConnected || !resumeSerial.trim()" class="btn-primary w-full btn-sm flex items-center justify-center gap-2">
          <RotateCcw class="w-4 h-4" /> Send Resume
        </button>
        <div class="grid grid-cols-2 gap-2">
          <button @click="disconnect" :disabled="!isConnected" class="btn-secondary btn-sm flex items-center justify-center gap-2">
            <Unplug class="w-4 h-4" /> Drop
          </button>
          <button @click="reconnect" :disabled="!isConnected" class="btn-secondary btn-sm flex items-center justify-center gap-2">
            <Wifi class="w-4 h-4" /> Reconnect
          </button>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-2 gap-6">
      <div class="panel p-5 space-y-3">
        <div class="flex items-center justify-between">
          <h3 class="text-sm font-semibold text-surface-200">History Response</h3>
          <span v-if="historyResult" :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', historyResult.status >= 200 && historyResult.status < 300 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">HTTP {{ historyResult.status }}</span>
        </div>
        <div v-if="!historyResult" class="text-sm text-surface-500 py-16 text-center">No history response</div>
        <pre v-else class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[430px] text-surface-300">{{ fmtResponse(historyResult.data) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <div class="flex items-center justify-between">
          <h3 class="text-sm font-semibold text-surface-200">State Response</h3>
          <span v-if="stateResult" :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', stateResult.status >= 200 && stateResult.status < 300 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">HTTP {{ stateResult.status }}</span>
        </div>
        <div v-if="!stateResult" class="text-sm text-surface-500 py-16 text-center">No state response</div>
        <pre v-else class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[430px] text-surface-300">{{ fmtResponse(stateResult.data) }}</pre>
      </div>
    </div>
  </div>
</template>
