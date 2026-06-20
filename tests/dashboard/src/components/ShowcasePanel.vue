<script setup lang="ts">
import {
  Activity,
  BarChart3,
  Bell,
  Bot,
  CheckCircle2,
  Filter,
  Globe,
  Gauge,
  History,
  MessagesSquare,
  Radio,
  RotateCcw,
  Sparkles,
  Users,
  Wifi,
  type LucideIcon,
} from 'lucide-vue-next'
import { useDashboardStore, type Tab } from '../stores/dashboard'

const store = useDashboardStore()

const featureGroups: Array<{
  title: string
  icon: LucideIcon
  tab: Tab
  items: string[]
  accent: string
}> = [
  {
    title: 'Protocol V1/V2 realtime',
    icon: Wifi,
    tab: 'connection',
    accent: 'text-emerald-400 bg-emerald-500/10',
    items: ['WebSocket connection', 'private auth', 'presence auth', 'wire formats'],
  },
  {
    title: 'Channels and presence',
    icon: Users,
    tab: 'presence',
    accent: 'text-brand-400 bg-brand-500/10',
    items: ['public/private', 'presence members', 'namespaces', 'metachannels'],
  },
  {
    title: 'Durable history and recovery',
    icon: RotateCcw,
    tab: 'recovery',
    accent: 'text-cyan-400 bg-cyan-500/10',
    items: ['history pages', 'stream state', 'rewind ranges', 'V2 resume'],
  },
  {
    title: 'Mutable messages',
    icon: MessagesSquare,
    tab: 'mutable',
    accent: 'text-amber-400 bg-amber-500/10',
    items: ['create', 'update', 'append', 'delete', 'versions', 'annotations'],
  },
  {
    title: 'Push notifications',
    icon: Bell,
    tab: 'push',
    accent: 'text-pink-400 bg-pink-500/10',
    items: ['credentials', 'devices', 'subscriptions', 'publish', 'status'],
  },
  {
    title: 'AI Transport',
    icon: Bot,
    tab: 'ai-chat',
    accent: 'text-purple-400 bg-purple-500/10',
    items: ['chat UI', 'ai-input', 'ai-output', 'streaming appends', 'cancel', 'history'],
  },
  {
    title: 'Filters and delta',
    icon: Filter,
    tab: 'filters',
    accent: 'text-lime-400 bg-lime-500/10',
    items: ['tag operators', 'wildcards', 'delta burst', 'bandwidth stats'],
  },
  {
    title: 'HTTP and operations',
    icon: Globe,
    tab: 'api',
    accent: 'text-sky-400 bg-sky-500/10',
    items: ['signed API', 'channel info', 'presence history', 'health', 'user terminate'],
  },
  {
    title: 'Observability and limits',
    icon: Gauge,
    tab: 'ops',
    accent: 'text-red-400 bg-red-500/10',
    items: ['live/up probes', 'usage', 'stats', 'metrics', 'rate probe', 'webhooks'],
  },
]

const demoSequence: Array<{ label: string; tab: Tab; icon: LucideIcon }> = [
  { label: 'Connect', tab: 'connection', icon: Wifi },
  { label: 'Channels', tab: 'channels', icon: Radio },
  { label: 'Publish', tab: 'api', icon: Globe },
  { label: 'History', tab: 'recovery', icon: History },
  { label: 'Messages', tab: 'mutable', icon: MessagesSquare },
  { label: 'Push', tab: 'push', icon: Bell },
  { label: 'AI Chat', tab: 'ai-chat', icon: Bot },
  { label: 'AI Internals', tab: 'ai', icon: Bot },
  { label: 'Ops', tab: 'ops', icon: Gauge },
  { label: 'Delta', tab: 'delta', icon: BarChart3 },
  { label: 'Events', tab: 'events', icon: Activity },
]
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
      <div>
        <div class="flex items-center gap-2 mb-2">
          <div class="p-2 rounded-lg bg-brand-500/10 text-brand-400">
            <Sparkles class="w-5 h-5" />
          </div>
          <h2 class="text-xl font-bold text-surface-50">Sockudo Demo Console</h2>
        </div>
        <p class="text-sm text-surface-400 max-w-3xl">
          End-to-end coverage for realtime compatibility, V2 stateful features, push, and AI Transport.
        </p>
      </div>

      <div class="grid grid-cols-3 gap-2 min-w-[280px]">
        <div class="panel p-3">
          <p class="text-[10px] text-surface-500 uppercase">Connected</p>
          <p class="text-lg font-mono font-bold" :class="store.connectionState === 'connected' ? 'text-emerald-400' : 'text-surface-400'">
            {{ store.connectionState === 'connected' ? 'yes' : 'no' }}
          </p>
        </div>
        <div class="panel p-3">
          <p class="text-[10px] text-surface-500 uppercase">Channels</p>
          <p class="text-lg font-mono font-bold text-surface-100">{{ store.channelList.length }}</p>
        </div>
        <div class="panel p-3">
          <p class="text-[10px] text-surface-500 uppercase">Events</p>
          <p class="text-lg font-mono font-bold text-surface-100">{{ store.eventLog.length }}</p>
        </div>
      </div>
    </div>

    <div class="panel p-5">
      <div class="flex flex-wrap gap-2">
        <button
          v-for="step in demoSequence"
          :key="step.label"
          @click="store.activeTab = step.tab"
          class="btn-secondary btn-sm flex items-center gap-2"
        >
          <component :is="step.icon" class="w-3.5 h-3.5" />
          {{ step.label }}
        </button>
      </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
      <button
        v-for="group in featureGroups"
        :key="group.title"
        @click="store.activeTab = group.tab"
        class="panel p-4 text-left hover:border-brand-500/40 transition-all"
      >
        <div class="flex items-start gap-3 mb-3">
          <div :class="['p-2 rounded-lg', group.accent]">
            <component :is="group.icon" class="w-4 h-4" />
          </div>
          <div class="min-w-0">
            <h3 class="text-sm font-semibold text-surface-100">{{ group.title }}</h3>
            <p class="text-[11px] text-surface-500 mt-0.5">{{ group.items.length }} checks</p>
          </div>
        </div>
        <div class="space-y-1.5">
          <div v-for="item in group.items" :key="item" class="flex items-center gap-2 text-xs text-surface-400">
            <CheckCircle2 class="w-3.5 h-3.5 text-emerald-400/80 shrink-0" />
            <span>{{ item }}</span>
          </div>
        </div>
      </button>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
      <div class="panel p-5 lg:col-span-2">
        <h3 class="text-sm font-semibold text-surface-200 mb-3">Demo Storyline</h3>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div class="bg-surface-800/50 border border-surface-700/30 rounded-lg p-3">
            <p class="text-xs font-semibold text-surface-200">Realtime core</p>
            <p class="text-xs text-surface-500 mt-1">Connect, subscribe, publish, inspect live protocol frames.</p>
          </div>
          <div class="bg-surface-800/50 border border-surface-700/30 rounded-lg p-3">
            <p class="text-xs font-semibold text-surface-200">Stateful V2</p>
            <p class="text-xs text-surface-500 mt-1">Fetch retained history, recover positions, mutate and annotate messages.</p>
          </div>
          <div class="bg-surface-800/50 border border-surface-700/30 rounded-lg p-3">
            <p class="text-xs font-semibold text-surface-200">Product surfaces</p>
            <p class="text-xs text-surface-500 mt-1">Exercise push, tag filtering, delta compression, and AI streams.</p>
          </div>
        </div>
      </div>

      <div class="panel p-5">
        <h3 class="text-sm font-semibold text-surface-200 mb-3">Runtime Evidence</h3>
        <div class="space-y-2 text-xs">
          <div class="flex items-center justify-between">
            <span class="text-surface-500">Socket ID</span>
            <span class="font-mono text-surface-300 truncate max-w-[160px]">{{ store.socketId || 'none' }}</span>
          </div>
          <div class="flex items-center justify-between">
            <span class="text-surface-500">Wire format</span>
            <span class="font-mono text-surface-300">{{ store.config.wireFormat }}</span>
          </div>
          <div class="flex items-center justify-between">
            <span class="text-surface-500">Delta messages</span>
            <span class="font-mono text-surface-300">{{ store.deltaStats.deltaMessages }}</span>
          </div>
          <div class="flex items-center justify-between">
            <span class="text-surface-500">Presence channels</span>
            <span class="font-mono text-surface-300">{{ store.channelList.filter((c) => c.type === 'presence').length }}</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
