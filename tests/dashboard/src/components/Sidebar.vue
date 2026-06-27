<script setup lang="ts">
import { computed } from 'vue'
import {
  Wifi,
  Radio,
  Zap,
  Users,
  Globe,
  Filter,
  BarChart3,
  Sparkles,
  RotateCcw,
  MessagesSquare,
  Bell,
  Bot,
  Gauge,
  type LucideIcon,
} from 'lucide-vue-next'
import { useDashboardStore, type Tab, type ConnectionState } from '../stores/dashboard'

const store = useDashboardStore()

const tabs: { id: Tab; label: string; icon: LucideIcon; group: string }[] = [
  { id: 'showcase', label: 'Demo', icon: Sparkles, group: 'Run' },
  { id: 'connection', label: 'Connection', icon: Wifi, group: 'Realtime' },
  { id: 'channels', label: 'Channels', icon: Radio, group: 'Realtime' },
  { id: 'presence', label: 'Presence', icon: Users, group: 'Realtime' },
  { id: 'api', label: 'HTTP API', icon: Globe, group: 'Realtime' },
  { id: 'recovery', label: 'Recovery', icon: RotateCcw, group: 'Stateful' },
  { id: 'mutable', label: 'Messages', icon: MessagesSquare, group: 'Stateful' },
  { id: 'push', label: 'Push', icon: Bell, group: 'Delivery' },
  { id: 'ai-chat', label: 'AI Chat', icon: Bot, group: 'Delivery' },
  { id: 'events', label: 'Events', icon: Zap, group: 'Inspect' },
  { id: 'filters', label: 'Filters', icon: Filter, group: 'Inspect' },
  { id: 'delta', label: 'Delta', icon: BarChart3, group: 'Inspect' },
  { id: 'ai', label: 'AI Transport', icon: Bot, group: 'Inspect' },
  { id: 'ops', label: 'Ops', icon: Gauge, group: 'Inspect' },
]

const tabSections = computed(() => {
  const sections: Array<{ group: string; tabs: typeof tabs }> = []
  for (const tab of tabs) {
    let section = sections.find((item) => item.group === tab.group)
    if (!section) {
      section = { group: tab.group, tabs: [] }
      sections.push(section)
    }
    section.tabs.push(tab)
  }
  return sections
})

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
    <nav class="flex-1 px-2 space-y-3 overflow-y-auto">
      <div v-for="section in tabSections" :key="section.group" class="space-y-0.5">
        <p class="px-3 pb-1 text-[10px] font-semibold uppercase tracking-wide text-surface-600">
          {{ section.group }}
        </p>
        <button
          v-for="tab in section.tabs"
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
      </div>
    </nav>

    <!-- Footer -->
    <div class="p-3 border-t border-surface-700/30">
      <p class="text-[10px] text-surface-600 text-center">Pusher Protocol v7</p>
    </div>
  </aside>
</template>
