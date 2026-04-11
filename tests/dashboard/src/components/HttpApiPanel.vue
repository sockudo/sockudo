<script setup lang="ts">
import { ref } from 'vue'
import { Globe, Send, Layers, Radio, Heart, UserX, History, UsersRound, RotateCcw, Search } from 'lucide-vue-next'
import { useHttpApi } from '../composables/useHttpApi'

const activeTab = ref<'publish' | 'batch' | 'channels' | 'history' | 'presence-history' | 'health' | 'users'>('publish')

const tabs = [
  { id: 'publish' as const, label: 'Publish', icon: Send },
  { id: 'batch' as const, label: 'Batch', icon: Layers },
  { id: 'channels' as const, label: 'Channels', icon: Radio },
  { id: 'history' as const, label: 'History', icon: History },
  { id: 'presence-history' as const, label: 'Presence Hist', icon: UsersRound },
  { id: 'health' as const, label: 'Health', icon: Heart },
  { id: 'users' as const, label: 'Users', icon: UserX },
]

// ─── Publish ──────────────────────────────────────────
const pubChannel = ref('my-channel')
const pubEvent = ref('my-event')
const pubData = ref('{"message": "Hello from HTTP API!"}')
const pubTags = ref('')
const pubResult = ref<any>(null)
const pubLoading = ref(false)

async function handlePublish() {
  const api = useHttpApi()
  pubLoading.value = true
  let tags: Record<string, string> | undefined
  if (pubTags.value.trim()) try { tags = JSON.parse(pubTags.value) } catch {}
  pubResult.value = await api.publishEvent(pubChannel.value, pubEvent.value, pubData.value, tags)
  pubLoading.value = false
}

// ─── Batch ────────────────────────────────────────────
const batchJson = ref(JSON.stringify([
  { name: 'event-1', channel: 'my-channel', data: '{"msg":"batch 1"}' },
  { name: 'event-2', channel: 'my-channel', data: '{"msg":"batch 2"}' },
], null, 2))
const batchResult = ref<any>(null)
const batchLoading = ref(false)

async function handleBatch() {
  const api = useHttpApi()
  batchLoading.value = true
  try {
    batchResult.value = await api.publishBatch(JSON.parse(batchJson.value))
  } catch (e: any) {
    batchResult.value = { status: 0, data: { error: e.message } }
  }
  batchLoading.value = false
}

// ─── Channels ─────────────────────────────────────────
const chPrefix = ref('')
const chSingle = ref('')
const chListResult = ref<any>(null)
const chSingleResult = ref<any>(null)

async function handleListChannels() {
  const api = useHttpApi()
  chListResult.value = await api.getChannels(chPrefix.value || undefined)
}

async function handleGetChannel() {
  if (!chSingle.value.trim()) return
  const api = useHttpApi()
  chSingleResult.value = await api.getChannel(chSingle.value)
}

// ─── History ──────────────────────────────────────────
const historyChannel = ref('history-room')
const historyLimit = ref('50')
const historyDirection = ref<'newest_first' | 'oldest_first'>('newest_first')
const historyCursor = ref('')
const historyStartSerial = ref('')
const historyEndSerial = ref('')
const historyStartTime = ref('')
const historyEndTime = ref('')
const historyResult = ref<any>(null)
const historyStateResult = ref<any>(null)
const historyResetReason = ref('operator reset from dashboard')
const historyResetRequestedBy = ref('dashboard')
const historyResetResult = ref<any>(null)

function buildHistoryParams() {
  const params: Record<string, string> = {
    limit: historyLimit.value || '50',
    direction: historyDirection.value,
  }
  if (historyCursor.value.trim()) params.cursor = historyCursor.value.trim()
  if (historyStartSerial.value.trim()) params.start_serial = historyStartSerial.value.trim()
  if (historyEndSerial.value.trim()) params.end_serial = historyEndSerial.value.trim()
  if (historyStartTime.value.trim()) params.start_time_ms = historyStartTime.value.trim()
  if (historyEndTime.value.trim()) params.end_time_ms = historyEndTime.value.trim()
  return params
}

async function handleGetHistory() {
  if (!historyChannel.value.trim()) return
  const api = useHttpApi()
  historyResult.value = await api.getChannelHistory(historyChannel.value.trim(), buildHistoryParams())
}

async function handleGetHistoryState() {
  if (!historyChannel.value.trim()) return
  const api = useHttpApi()
  historyStateResult.value = await api.getChannelHistoryState(historyChannel.value.trim())
}

async function handleResetHistory() {
  if (!historyChannel.value.trim() || !historyResetReason.value.trim()) return
  const api = useHttpApi()
  historyResetResult.value = await api.resetChannelHistory(
    historyChannel.value.trim(),
    historyResetReason.value.trim(),
    historyResetRequestedBy.value.trim() || undefined,
  )
}

function applyHistoryCursorFromResult() {
  const cursor = historyResult.value?.data?.next_cursor
  if (cursor) historyCursor.value = cursor
}

// ─── Presence History ─────────────────────────────────
const presenceHistoryChannel = ref('presence-room')
const presenceHistoryLimit = ref('50')
const presenceHistoryDirection = ref<'newest_first' | 'oldest_first'>('newest_first')
const presenceHistoryCursor = ref('')
const presenceHistoryStartSerial = ref('')
const presenceHistoryEndSerial = ref('')
const presenceHistoryStartTime = ref('')
const presenceHistoryEndTime = ref('')
const presenceHistoryResult = ref<any>(null)
const presenceHistoryStateResult = ref<any>(null)
const presenceHistorySnapshotAtTime = ref('')
const presenceHistorySnapshotAtSerial = ref('')
const presenceHistorySnapshotResult = ref<any>(null)
const presenceHistoryResetReason = ref('operator reset from dashboard')
const presenceHistoryResetRequestedBy = ref('dashboard')
const presenceHistoryResetResult = ref<any>(null)

function presenceChannelName(name: string) {
  const trimmed = name.trim()
  return trimmed.startsWith('presence-') ? trimmed : `presence-${trimmed}`
}

function buildPresenceHistoryParams() {
  const params: Record<string, string> = {
    limit: presenceHistoryLimit.value || '50',
    direction: presenceHistoryDirection.value,
  }
  if (presenceHistoryCursor.value.trim()) params.cursor = presenceHistoryCursor.value.trim()
  if (presenceHistoryStartSerial.value.trim()) params.start_serial = presenceHistoryStartSerial.value.trim()
  if (presenceHistoryEndSerial.value.trim()) params.end_serial = presenceHistoryEndSerial.value.trim()
  if (presenceHistoryStartTime.value.trim()) params.start_time_ms = presenceHistoryStartTime.value.trim()
  if (presenceHistoryEndTime.value.trim()) params.end_time_ms = presenceHistoryEndTime.value.trim()
  return params
}

async function handleGetPresenceHistory() {
  if (!presenceHistoryChannel.value.trim()) return
  const api = useHttpApi()
  presenceHistoryResult.value = await api.getPresenceHistory(
    presenceChannelName(presenceHistoryChannel.value),
    buildPresenceHistoryParams(),
  )
}

async function handleGetPresenceHistoryState() {
  if (!presenceHistoryChannel.value.trim()) return
  const api = useHttpApi()
  presenceHistoryStateResult.value = await api.getPresenceHistoryState(
    presenceChannelName(presenceHistoryChannel.value),
  )
}

async function handleGetPresenceHistorySnapshot() {
  if (!presenceHistoryChannel.value.trim()) return
  const api = useHttpApi()
  const params: Record<string, string> = {}
  if (presenceHistorySnapshotAtTime.value.trim()) params.at_time_ms = presenceHistorySnapshotAtTime.value.trim()
  if (presenceHistorySnapshotAtSerial.value.trim()) params.at_serial = presenceHistorySnapshotAtSerial.value.trim()
  presenceHistorySnapshotResult.value = await api.getPresenceHistorySnapshot(
    presenceChannelName(presenceHistoryChannel.value),
    params,
  )
}

async function handleResetPresenceHistory() {
  if (!presenceHistoryChannel.value.trim() || !presenceHistoryResetReason.value.trim()) return
  const api = useHttpApi()
  presenceHistoryResetResult.value = await api.resetPresenceHistory(
    presenceChannelName(presenceHistoryChannel.value),
    presenceHistoryResetReason.value.trim(),
    presenceHistoryResetRequestedBy.value.trim() || undefined,
  )
}

function applyPresenceHistoryCursorFromResult() {
  const cursor = presenceHistoryResult.value?.data?.next_cursor
  if (cursor) presenceHistoryCursor.value = cursor
}

// ─── Health ───────────────────────────────────────────
const healthResult = ref<any>(null)
const healthLoading = ref(false)

async function handleHealth() {
  const api = useHttpApi()
  healthLoading.value = true
  healthResult.value = await api.healthCheck()
  healthLoading.value = false
}

// ─── Users ────────────────────────────────────────────
const userId = ref('')
const userResult = ref<any>(null)

async function handleTerminate() {
  if (!userId.value.trim()) return
  const api = useHttpApi()
  userResult.value = await api.terminateUser(userId.value)
}

function fmtResponse(data: unknown) {
  return typeof data === 'string' ? data : JSON.stringify(data, null, 2)
}
</script>

<template>
  <div class="space-y-4 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">HTTP API</h2>
      <p class="text-sm text-surface-400 mt-1">Test the Pusher-compatible REST API endpoints</p>
    </div>

    <!-- Tabs -->
    <div class="flex gap-1 p-1 bg-surface-800/50 rounded-lg w-fit">
      <button v-for="tab in tabs" :key="tab.id" @click="activeTab = tab.id" :class="['flex items-center gap-1.5', activeTab === tab.id ? 'tab-btn-active' : 'tab-btn']">
        <component :is="tab.icon" class="w-3.5 h-3.5" /> {{ tab.label }}
      </button>
    </div>

    <!-- Publish -->
    <div v-if="activeTab === 'publish'" class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">POST /apps/{appId}/events</h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Channel</label>
          <input v-model="pubChannel" class="input-field font-mono" />
        </div>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Event Name</label>
          <input v-model="pubEvent" class="input-field font-mono" />
        </div>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Data (JSON string)</label>
          <textarea v-model="pubData" class="input-field font-mono text-xs h-24 resize-none" />
        </div>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Tags (optional JSON)</label>
          <input v-model="pubTags" class="input-field font-mono" placeholder='{"symbol": "BTC"}' />
        </div>
        <button @click="handlePublish" :disabled="pubLoading" class="btn-primary w-full flex items-center justify-center gap-2">
          <Send class="w-4 h-4" /> {{ pubLoading ? 'Publishing...' : 'Publish Event' }}
        </button>
      </div>
      <div class="panel p-5 space-y-3">
        <div v-if="!pubResult" class="flex items-center justify-center h-full text-surface-500 text-sm">Response will appear here</div>
        <template v-else>
          <div class="flex items-center justify-between">
            <h3 class="text-sm font-semibold text-surface-200">Response</h3>
            <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', pubResult.status >= 200 && pubResult.status < 300 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">HTTP {{ pubResult.status }}</span>
          </div>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[400px] text-surface-300">{{ fmtResponse(pubResult.data) }}</pre>
        </template>
      </div>
    </div>

    <!-- Batch -->
    <div v-if="activeTab === 'batch'" class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">POST /apps/{appId}/batch_events</h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Batch Events (JSON array)</label>
          <textarea v-model="batchJson" class="input-field font-mono text-xs h-48 resize-none" />
        </div>
        <button @click="handleBatch" :disabled="batchLoading" class="btn-primary w-full flex items-center justify-center gap-2">
          <Layers class="w-4 h-4" /> {{ batchLoading ? 'Sending...' : 'Send Batch' }}
        </button>
      </div>
      <div class="panel p-5 space-y-3">
        <div v-if="!batchResult" class="flex items-center justify-center h-full text-surface-500 text-sm">Response will appear here</div>
        <template v-else>
          <div class="flex items-center justify-between">
            <h3 class="text-sm font-semibold text-surface-200">Response</h3>
            <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', batchResult.status >= 200 && batchResult.status < 300 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">HTTP {{ batchResult.status }}</span>
          </div>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[400px] text-surface-300">{{ fmtResponse(batchResult.data) }}</pre>
        </template>
      </div>
    </div>

    <!-- Channels -->
    <div v-if="activeTab === 'channels'" class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">GET /apps/{appId}/channels</h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Filter Prefix (optional)</label>
          <input v-model="chPrefix" class="input-field font-mono" placeholder="presence-" />
        </div>
        <button @click="handleListChannels" class="btn-primary w-full btn-sm">List Channels</button>
        <div v-if="chListResult" class="space-y-2">
          <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', chListResult.status === 200 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">{{ chListResult.status }}</span>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[300px] text-surface-300">{{ fmtResponse(chListResult.data) }}</pre>
        </div>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">GET /apps/{appId}/channels/{name}</h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Channel Name</label>
          <input v-model="chSingle" class="input-field font-mono" placeholder="my-channel" />
        </div>
        <button @click="handleGetChannel" class="btn-primary w-full btn-sm" :disabled="!chSingle.trim()">Get Info</button>
        <div v-if="chSingleResult" class="space-y-2">
          <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', chSingleResult.status === 200 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">{{ chSingleResult.status }}</span>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[300px] text-surface-300">{{ fmtResponse(chSingleResult.data) }}</pre>
        </div>
      </div>
    </div>

    <!-- Health -->
    <div v-if="activeTab === 'history'" class="grid grid-cols-1 xl:grid-cols-3 gap-6">
      <div class="panel p-5 space-y-3 xl:col-span-1">
        <h3 class="text-sm font-semibold text-surface-200">Channel History</h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Channel</label>
          <input v-model="historyChannel" class="input-field font-mono" placeholder="history-room" />
        </div>
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
          <input v-model="historyCursor" class="input-field font-mono text-xs" placeholder="opaque cursor" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Start Serial</label>
            <input v-model="historyStartSerial" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">End Serial</label>
            <input v-model="historyEndSerial" class="input-field font-mono" />
          </div>
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Start Time Ms</label>
            <input v-model="historyStartTime" class="input-field font-mono text-xs" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">End Time Ms</label>
            <input v-model="historyEndTime" class="input-field font-mono text-xs" />
          </div>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button @click="handleGetHistory" class="btn-primary btn-sm flex items-center justify-center gap-2">
            <History class="w-4 h-4" /> Fetch
          </button>
          <button @click="handleGetHistoryState" class="btn-secondary btn-sm flex items-center justify-center gap-2">
            <Search class="w-4 h-4" /> State
          </button>
        </div>
        <button @click="applyHistoryCursorFromResult" class="btn-secondary btn-sm w-full" :disabled="!historyResult?.data?.next_cursor">Use Next Cursor</button>
        <div class="border-t border-surface-800 pt-3 space-y-2">
          <label class="text-xs text-surface-400 mb-1 block">Reset Reason</label>
          <input v-model="historyResetReason" class="input-field font-mono text-xs" />
          <label class="text-xs text-surface-400 mb-1 block">Requested By</label>
          <input v-model="historyResetRequestedBy" class="input-field font-mono text-xs" />
          <button @click="handleResetHistory" class="btn-danger btn-sm w-full flex items-center justify-center gap-2">
            <RotateCcw class="w-4 h-4" /> Reset History
          </button>
        </div>
      </div>
      <div class="panel p-5 space-y-3 xl:col-span-1">
        <h3 class="text-sm font-semibold text-surface-200">History Response</h3>
        <div v-if="!historyResult" class="flex items-center justify-center h-64 text-surface-500 text-sm">History page response will appear here</div>
        <template v-else>
          <div class="flex items-center justify-between">
            <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', historyResult.status >= 200 && historyResult.status < 300 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">HTTP {{ historyResult.status }}</span>
          </div>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse(historyResult.data) }}</pre>
        </template>
      </div>
      <div class="panel p-5 space-y-3 xl:col-span-1">
        <h3 class="text-sm font-semibold text-surface-200">History State / Reset</h3>
        <div v-if="!historyStateResult && !historyResetResult" class="flex items-center justify-center h-64 text-surface-500 text-sm">State and reset responses will appear here</div>
        <template v-else>
          <div v-if="historyStateResult" class="space-y-2">
            <p class="section-title mb-0">State</p>
            <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[180px] text-surface-300">{{ fmtResponse(historyStateResult.data) }}</pre>
          </div>
          <div v-if="historyResetResult" class="space-y-2">
            <p class="section-title mb-0">Reset</p>
            <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[180px] text-surface-300">{{ fmtResponse(historyResetResult.data) }}</pre>
          </div>
        </template>
      </div>
    </div>

    <div v-if="activeTab === 'presence-history'" class="grid grid-cols-1 xl:grid-cols-3 gap-6">
      <div class="panel p-5 space-y-3 xl:col-span-1">
        <h3 class="text-sm font-semibold text-surface-200">Presence History</h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Presence Channel</label>
          <input v-model="presenceHistoryChannel" class="input-field font-mono" placeholder="presence-room" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Limit</label>
            <input v-model="presenceHistoryLimit" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Direction</label>
            <select v-model="presenceHistoryDirection" class="input-field font-mono">
              <option value="newest_first">newest_first</option>
              <option value="oldest_first">oldest_first</option>
            </select>
          </div>
        </div>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Cursor</label>
          <input v-model="presenceHistoryCursor" class="input-field font-mono text-xs" placeholder="opaque cursor" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Start Serial</label>
            <input v-model="presenceHistoryStartSerial" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">End Serial</label>
            <input v-model="presenceHistoryEndSerial" class="input-field font-mono" />
          </div>
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Start Time Ms</label>
            <input v-model="presenceHistoryStartTime" class="input-field font-mono text-xs" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">End Time Ms</label>
            <input v-model="presenceHistoryEndTime" class="input-field font-mono text-xs" />
          </div>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button @click="handleGetPresenceHistory" class="btn-primary btn-sm flex items-center justify-center gap-2">
            <UsersRound class="w-4 h-4" /> Fetch
          </button>
          <button @click="handleGetPresenceHistoryState" class="btn-secondary btn-sm flex items-center justify-center gap-2">
            <Search class="w-4 h-4" /> State
          </button>
        </div>
        <button @click="applyPresenceHistoryCursorFromResult" class="btn-secondary btn-sm w-full" :disabled="!presenceHistoryResult?.data?.next_cursor">Use Next Cursor</button>
        <div class="border-t border-surface-800 pt-3 space-y-2">
          <p class="section-title mb-0">Snapshot</p>
          <div class="grid grid-cols-2 gap-3">
            <input v-model="presenceHistorySnapshotAtTime" class="input-field font-mono text-xs" placeholder="at_time_ms" />
            <input v-model="presenceHistorySnapshotAtSerial" class="input-field font-mono text-xs" placeholder="at_serial" />
          </div>
          <button @click="handleGetPresenceHistorySnapshot" class="btn-secondary btn-sm w-full">Fetch Snapshot</button>
        </div>
        <div class="border-t border-surface-800 pt-3 space-y-2">
          <label class="text-xs text-surface-400 mb-1 block">Reset Reason</label>
          <input v-model="presenceHistoryResetReason" class="input-field font-mono text-xs" />
          <label class="text-xs text-surface-400 mb-1 block">Requested By</label>
          <input v-model="presenceHistoryResetRequestedBy" class="input-field font-mono text-xs" />
          <button @click="handleResetPresenceHistory" class="btn-danger btn-sm w-full flex items-center justify-center gap-2">
            <RotateCcw class="w-4 h-4" /> Reset Presence History
          </button>
        </div>
      </div>
      <div class="panel p-5 space-y-3 xl:col-span-1">
        <h3 class="text-sm font-semibold text-surface-200">Presence History Response</h3>
        <div v-if="!presenceHistoryResult" class="flex items-center justify-center h-64 text-surface-500 text-sm">Presence history response will appear here</div>
        <template v-else>
          <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', presenceHistoryResult.status >= 200 && presenceHistoryResult.status < 300 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">HTTP {{ presenceHistoryResult.status }}</span>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse(presenceHistoryResult.data) }}</pre>
        </template>
      </div>
      <div class="panel p-5 space-y-3 xl:col-span-1">
        <h3 class="text-sm font-semibold text-surface-200">State / Snapshot / Reset</h3>
        <div v-if="!presenceHistoryStateResult && !presenceHistorySnapshotResult && !presenceHistoryResetResult" class="flex items-center justify-center h-64 text-surface-500 text-sm">Supplementary responses will appear here</div>
        <template v-else>
          <div v-if="presenceHistoryStateResult" class="space-y-2">
            <p class="section-title mb-0">State</p>
            <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[150px] text-surface-300">{{ fmtResponse(presenceHistoryStateResult.data) }}</pre>
          </div>
          <div v-if="presenceHistorySnapshotResult" class="space-y-2">
            <p class="section-title mb-0">Snapshot</p>
            <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[150px] text-surface-300">{{ fmtResponse(presenceHistorySnapshotResult.data) }}</pre>
          </div>
          <div v-if="presenceHistoryResetResult" class="space-y-2">
            <p class="section-title mb-0">Reset</p>
            <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[150px] text-surface-300">{{ fmtResponse(presenceHistoryResetResult.data) }}</pre>
          </div>
        </template>
      </div>
    </div>

    <div v-if="activeTab === 'health'" class="panel p-5 space-y-4 max-w-lg">
      <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
        <Heart class="w-4 h-4 text-brand-400" /> GET /up
      </h3>
      <button @click="handleHealth" :disabled="healthLoading" class="btn-primary flex items-center gap-2">
        <Heart class="w-4 h-4" /> {{ healthLoading ? 'Checking...' : 'Health Check' }}
      </button>
      <div v-if="healthResult" class="space-y-2">
        <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', healthResult.status === 200 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">HTTP {{ healthResult.status }}</span>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 text-surface-300">{{ fmtResponse(healthResult.data) }}</pre>
      </div>
    </div>

    <!-- Users -->
    <div v-if="activeTab === 'users'" class="panel p-5 space-y-4 max-w-lg">
      <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
        <UserX class="w-4 h-4 text-brand-400" /> Terminate User Connections
      </h3>
      <div>
        <label class="text-xs text-surface-400 mb-1 block">User ID</label>
        <input v-model="userId" class="input-field font-mono" placeholder="user-123" />
      </div>
      <button @click="handleTerminate" class="btn-danger flex items-center gap-2" :disabled="!userId.trim()">
        <UserX class="w-4 h-4" /> Terminate Connections
      </button>
      <div v-if="userResult" class="space-y-2">
        <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', userResult.status >= 200 && userResult.status < 300 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">{{ userResult.status }}</span>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 text-surface-300">{{ fmtResponse(userResult.data) }}</pre>
      </div>
    </div>
  </div>
</template>
