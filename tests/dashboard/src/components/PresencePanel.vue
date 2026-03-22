<script setup lang="ts">
import { ref, computed } from 'vue'
import { Users, UserPlus, Eye } from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { usePusher } from '../composables/usePusher'
import { useHttpApi } from '../composables/useHttpApi'

const store = useDashboardStore()
const { subscribe, unsubscribe } = usePusher()
const { getChannelUsers } = useHttpApi()

const channelInput = ref('presence-room')
const queryChannel = ref('')
const queryResult = ref<any>(null)
const isConnected = computed(() => store.connectionState === 'connected')

const presenceChannels = computed(() => store.channelList.filter(c => c.type === 'presence'))

function handleSubscribe() {
  let name = channelInput.value.trim()
  if (!name || !isConnected.value) return
  if (!name.startsWith('presence-')) name = `presence-${name}`
  subscribe(name)
  channelInput.value = ''
}

async function handleQueryUsers() {
  if (!queryChannel.value.trim()) return
  queryResult.value = await getChannelUsers(queryChannel.value.trim())
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">Presence</h2>
      <p class="text-sm text-surface-400 mt-1">Monitor presence channels and member activity</p>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Join -->
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <UserPlus class="w-4 h-4 text-brand-400" /> Join Presence Channel
        </h3>
        <div class="flex gap-2">
          <input v-model="channelInput" @keydown.enter="handleSubscribe" class="input-field font-mono flex-1" placeholder="presence-room" :disabled="!isConnected" />
          <button @click="handleSubscribe" :disabled="!isConnected" class="btn-primary btn-sm">Join</button>
        </div>

        <div>
          <p class="section-title">Active Presence Channels</p>
          <p v-if="presenceChannels.length === 0" class="text-sm text-surface-500 text-center py-6">No presence channels</p>
          <div v-else class="space-y-2">
            <div v-for="ch in presenceChannels" :key="ch.name" class="bg-surface-800/60 backdrop-blur-lg border border-surface-700/30 rounded-xl p-3">
              <div class="flex items-center justify-between mb-2">
                <span class="text-xs font-mono text-surface-200">{{ ch.name }}</span>
                <div class="flex items-center gap-2">
                  <span class="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-brand-500/15 text-brand-400 ring-1 ring-brand-500/20">
                    <Users class="w-3 h-3" /> {{ (store.presenceMembers.get(ch.name) || []).length }}
                  </span>
                  <button @click="unsubscribe(ch.name)" class="text-[10px] text-red-400/60 hover:text-red-400 transition-colors">Leave</button>
                </div>
              </div>
              <div v-if="(store.presenceMembers.get(ch.name) || []).length" class="space-y-1">
                <div v-for="m in store.presenceMembers.get(ch.name)" :key="m.id" class="flex items-center gap-2 py-1">
                  <div class="w-6 h-6 rounded-full bg-brand-600/30 flex items-center justify-center text-[10px] font-bold text-brand-300">
                    {{ m.id.charAt(0).toUpperCase() }}
                  </div>
                  <span class="text-xs text-surface-300 font-mono">{{ m.id }}</span>
                  <span v-if="Object.keys(m.info).length" class="text-[10px] text-surface-500 truncate">{{ JSON.stringify(m.info) }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Query via API -->
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Eye class="w-4 h-4 text-brand-400" /> Query Channel Users (HTTP API)
        </h3>
        <div class="flex gap-2">
          <input v-model="queryChannel" @keydown.enter="handleQueryUsers" class="input-field font-mono flex-1" placeholder="presence-room" />
          <button @click="handleQueryUsers" class="btn-secondary btn-sm" :disabled="!queryChannel.trim()">Query</button>
        </div>
        <div v-if="queryResult">
          <div class="flex items-center justify-between mb-2">
            <p class="section-title mb-0">Result</p>
            <span :class="['inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1', queryResult.status === 200 ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' : 'bg-red-500/15 text-red-400 ring-red-500/20']">{{ queryResult.status }}</span>
          </div>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[300px] text-surface-300">{{ JSON.stringify(queryResult.data, null, 2) }}</pre>
        </div>
      </div>
    </div>
  </div>
</template>
