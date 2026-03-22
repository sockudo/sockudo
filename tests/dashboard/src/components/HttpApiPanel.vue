<script setup lang="ts">
import { ref } from 'vue'
import { Globe, Send, Layers, Radio, Heart, UserX } from 'lucide-vue-next'
import { useHttpApi } from '../composables/useHttpApi'

const activeTab = ref<'publish' | 'batch' | 'channels' | 'health' | 'users'>('publish')

const tabs = [
  { id: 'publish' as const, label: 'Publish', icon: Send },
  { id: 'batch' as const, label: 'Batch', icon: Layers },
  { id: 'channels' as const, label: 'Channels', icon: Radio },
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
