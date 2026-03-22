<script setup lang="ts">
import { ref, computed } from 'vue'
import { Radio, Plus, X, Hash, Lock, Users, ShieldCheck, Database } from 'lucide-vue-next'
import { useDashboardStore, type ChannelInfo } from '../stores/dashboard'
import { usePusher } from '../composables/usePusher'

const store = useDashboardStore()
const { subscribe, unsubscribe, bindEvent, triggerClientEvent } = usePusher()

const channelInput = ref('')
const eventBindName = ref('')
const clientEventName = ref('client-message')
const clientEventData = ref('{"text": "hello"}')
const selectedChannel = ref<string | null>(null)
const boundEvents = ref<Map<string, string[]>>(new Map())

const isConnected = computed(() => store.connectionState === 'connected')

const channelIcons: Record<ChannelInfo['type'], any> = { public: Hash, private: Lock, presence: Users, encrypted: ShieldCheck, cache: Database }
const channelColors: Record<ChannelInfo['type'], string> = {
  public: 'text-emerald-400 bg-emerald-500/10',
  private: 'text-amber-400 bg-amber-500/10',
  presence: 'text-brand-400 bg-brand-500/10',
  encrypted: 'text-purple-400 bg-purple-500/10',
  cache: 'text-cyan-400 bg-cyan-500/10',
}
const badgeColors: Record<ChannelInfo['type'], string> = {
  public: 'bg-emerald-500/15 text-emerald-400 ring-1 ring-emerald-500/20',
  private: 'bg-amber-500/15 text-amber-400 ring-1 ring-amber-500/20',
  presence: 'bg-brand-500/15 text-brand-400 ring-1 ring-brand-500/20',
  encrypted: 'bg-purple-500/15 text-purple-400 ring-1 ring-purple-500/20',
  cache: 'bg-surface-500/15 text-surface-400 ring-1 ring-surface-500/20',
}

const presets = [
  { name: 'my-channel', label: 'Public' },
  { name: 'private-chat', label: 'Private' },
  { name: 'presence-room', label: 'Presence' },
  { name: 'private-encrypted-secret', label: 'Encrypted' },
  { name: 'cache-state', label: 'Cache' },
]

const selectedIsPrivateOrPresence = computed(() => {
  if (!selectedChannel.value) return false
  return selectedChannel.value.startsWith('private-') || selectedChannel.value.startsWith('presence-')
})

function handleSubscribe() {
  const name = channelInput.value.trim()
  if (!name || !isConnected.value) return
  subscribe(name)
  channelInput.value = ''
  selectedChannel.value = name
}

function handlePreset(name: string) {
  if (!isConnected.value || store.channels.has(name)) return
  subscribe(name)
  selectedChannel.value = name
}

function handleBind() {
  if (!selectedChannel.value || !eventBindName.value.trim()) return
  bindEvent(selectedChannel.value, eventBindName.value.trim(), () => {})
  const existing = boundEvents.value.get(selectedChannel.value) || []
  boundEvents.value.set(selectedChannel.value, [...existing, eventBindName.value.trim()])
  boundEvents.value = new Map(boundEvents.value)
  eventBindName.value = ''
}

function handleTrigger() {
  if (!selectedChannel.value || !clientEventName.value.trim()) return
  let data: unknown
  try { data = JSON.parse(clientEventData.value) } catch { data = clientEventData.value }
  triggerClientEvent(selectedChannel.value, clientEventName.value, data)
}

function handleUnsubscribe(name: string) {
  unsubscribe(name)
  if (selectedChannel.value === name) selectedChannel.value = null
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">Channels</h2>
      <p class="text-sm text-surface-400 mt-1">Subscribe to channels and manage event bindings</p>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <!-- Subscribe -->
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Plus class="w-4 h-4 text-brand-400" /> Subscribe
        </h3>
        <div>
          <input v-model="channelInput" @keydown.enter="handleSubscribe" class="input-field font-mono mb-2" placeholder="channel-name" :disabled="!isConnected" />
          <button @click="handleSubscribe" :disabled="!isConnected || !channelInput.trim()" class="btn-primary w-full btn-sm">Subscribe</button>
        </div>
        <div>
          <p class="section-title">Quick Presets</p>
          <div class="flex flex-wrap gap-1.5">
            <button v-for="p in presets" :key="p.name" @click="handlePreset(p.name)" :disabled="!isConnected || store.channels.has(p.name)" class="btn-secondary btn-sm text-[11px] disabled:opacity-30">{{ p.label }}</button>
          </div>
        </div>
      </div>

      <!-- Channel list -->
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Radio class="w-4 h-4 text-brand-400" /> Subscribed ({{ store.channelList.length }})
        </h3>
        <div v-if="store.channelList.length === 0" class="text-center py-8 text-surface-500 text-sm">No channels subscribed</div>
        <div v-else class="space-y-1.5 max-h-[400px] overflow-y-auto">
          <div
            v-for="ch in store.channelList"
            :key="ch.name"
            @click="selectedChannel = ch.name"
            :class="['flex items-center gap-2 p-2.5 rounded-lg cursor-pointer transition-all duration-200 group', selectedChannel === ch.name ? 'bg-brand-600/10 ring-1 ring-brand-500/20' : 'hover:bg-surface-800/60']"
          >
            <div :class="['p-1.5 rounded-md', channelColors[ch.type]]">
              <component :is="channelIcons[ch.type]" class="w-3.5 h-3.5" />
            </div>
            <div class="flex-1 min-w-0">
              <p class="text-xs font-mono text-surface-200 truncate">{{ ch.name }}</p>
              <div class="flex items-center gap-1.5 mt-0.5">
                <span :class="['inline-flex items-center px-2 py-0 rounded-full text-[10px] font-medium', badgeColors[ch.type]]">{{ ch.type }}</span>
                <span v-if="ch.subscribed" class="inline-flex items-center px-2 py-0 rounded-full text-[10px] font-medium bg-emerald-500/15 text-emerald-400 ring-1 ring-emerald-500/20">active</span>
              </div>
            </div>
            <button @click.stop="handleUnsubscribe(ch.name)" class="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-red-500/20 text-surface-500 hover:text-red-400 transition-all">
              <X class="w-3.5 h-3.5" />
            </button>
          </div>
        </div>
      </div>

      <!-- Channel actions -->
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200">
          <span v-if="selectedChannel" class="font-mono">{{ selectedChannel }}</span>
          <span v-else>Select a channel</span>
        </h3>

        <template v-if="selectedChannel">
          <!-- Bind -->
          <div>
            <p class="section-title">Bind Event</p>
            <div class="flex gap-2">
              <input v-model="eventBindName" @keydown.enter="handleBind" class="input-field font-mono flex-1" placeholder="event-name" />
              <button @click="handleBind" class="btn-secondary btn-sm" :disabled="!eventBindName.trim()">Bind</button>
            </div>
            <div v-if="(boundEvents.get(selectedChannel) || []).length" class="flex flex-wrap gap-1 mt-2">
              <span v-for="evt in boundEvents.get(selectedChannel)" :key="evt" class="inline-flex items-center px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-brand-500/15 text-brand-400 ring-1 ring-brand-500/20">{{ evt }}</span>
            </div>
          </div>

          <!-- Client events -->
          <div v-if="selectedIsPrivateOrPresence">
            <p class="section-title">Trigger Client Event</p>
            <div class="space-y-2">
              <input v-model="clientEventName" class="input-field font-mono" placeholder="client-event-name" />
              <textarea v-model="clientEventData" class="input-field font-mono text-xs h-20 resize-none" placeholder='{"key": "value"}' />
              <button @click="handleTrigger" class="btn-primary w-full btn-sm">Trigger</button>
            </div>
          </div>
        </template>

        <div v-else class="text-center py-12 text-surface-500 text-sm">Subscribe to a channel and select it to manage events</div>
      </div>
    </div>
  </div>
</template>
