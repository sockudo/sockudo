<script setup lang="ts">
import { Wifi, Radio, Zap, Users, Globe, Filter, BarChart3, type LucideIcon } from 'lucide-vue-next'
import { useDashboardStore, type Tab, type ConnectionState } from '../stores/dashboard'

const store = useDashboardStore()

const tabs: { id: Tab; label: string; icon: LucideIcon }[] = [
  { id: 'connection', label: 'Connection', icon: Wifi },
  { id: 'channels', label: 'Channels', icon: Radio },
  { id: 'events', label: 'Events', icon: Zap },
  { id: 'presence', label: 'Presence', icon: Users },
  { id: 'api', label: 'HTTP API', icon: Globe },
  { id: 'filters', label: 'Filters', icon: Filter },
  { id: 'delta', label: 'Delta', icon: BarChart3 },
]

const stateColors: Record<ConnectionState, string> = {
  connected: 'bg-emerald-400',
  connecting: 'bg-amber-400 animate-pulse-dot',
  reconnecting: 'bg-amber-400 animate-pulse-dot',
  disconnected: 'bg-surface-500',
  failed: 'bg-red-400',
}

const stateLabels: Record<ConnectionState, string> = {
  connected: 'Connected',
  connecting: 'Connecting...',
  reconnecting: 'Reconnecting...',
  disconnected: 'Disconnected',
  failed: 'Failed',
}
</script>

<template>
  <aside class="w-[220px] flex-shrink-0 flex flex-col h-screen bg-surface-900/80 backdrop-blur-xl border-r border-surface-700/40">
    <!-- Logo -->
    <div class="p-5 pb-4">
      <div class="flex items-center gap-2.5">
        <div class="w-8 h-8 rounded-lg bg-gradient-to-br from-brand-500 to-brand-700 flex items-center justify-center shadow-lg shadow-brand-600/30">
          <Zap class="w-4 h-4 text-white" />
        </div>
        <div>
          <h1 class="text-sm font-bold text-surface-50 tracking-tight">Sockudo</h1>
          <p class="text-[10px] text-surface-500 font-medium">Testing Dashboard</p>
        </div>
      </div>
    </div>

    <!-- Connection status pill -->
    <div class="mx-3 mb-4 p-3 bg-surface-800/60 backdrop-blur-lg border border-surface-700/30 rounded-xl">
      <div class="flex items-center gap-2 mb-1.5">
        <div :class="['w-2 h-2 rounded-full', stateColors[store.connectionState]]" />
        <span class="text-xs font-medium text-surface-300">{{ stateLabels[store.connectionState] }}</span>
      </div>
      <p v-if="store.socketId" class="text-[10px] font-mono text-surface-500 truncate" :title="store.socketId">
        {{ store.socketId }}
      </p>
    </div>

    <!-- Nav -->
    <nav class="flex-1 px-2 space-y-0.5 overflow-y-auto">
      <button
        v-for="tab in tabs"
        :key="tab.id"
        @click="store.activeTab = tab.id"
        :class="[
          'w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-200 group',
          store.activeTab === tab.id
            ? 'bg-brand-600/15 text-brand-300 shadow-sm'
            : 'text-surface-400 hover:text-surface-200 hover:bg-surface-800/60',
        ]"
      >
        <component
          :is="tab.icon"
          :class="['w-4 h-4', store.activeTab === tab.id ? 'text-brand-400' : 'text-surface-500 group-hover:text-surface-400']"
        />
        {{ tab.label }}
        <span
          v-if="tab.id === 'events' && store.eventLog.length > 0"
          class="ml-auto text-[10px] font-mono bg-surface-700/80 text-surface-400 px-1.5 py-0.5 rounded-md"
        >
          {{ store.eventLog.length }}
        </span>
      </button>
    </nav>

    <!-- Footer -->
    <div class="p-3 border-t border-surface-700/30">
      <p class="text-[10px] text-surface-600 text-center">Pusher Protocol v7</p>
    </div>
  </aside>
</template>
