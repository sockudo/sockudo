<script setup lang="ts">
import { ref, computed } from 'vue'
import { Zap, Trash2, ArrowDownLeft, ArrowUpRight, Info, Pause, Play, Search } from 'lucide-vue-next'
import { useDashboardStore, type EventLogEntry } from '../stores/dashboard'

const store = useDashboardStore()

const paused = ref(false)
const filter = ref('')
const expandedId = ref<string | null>(null)
const dirFilter = ref<'all' | 'in' | 'out' | 'system'>('all')

const dirConfig = {
  in:     { icon: ArrowDownLeft,  color: 'text-emerald-400', bg: 'bg-emerald-500/10', label: 'IN' },
  out:    { icon: ArrowUpRight,   color: 'text-brand-400',   bg: 'bg-brand-500/10',   label: 'OUT' },
  system: { icon: Info,           color: 'text-amber-400',   bg: 'bg-amber-500/10',   label: 'SYS' },
}

const displayEvents = computed(() => {
  if (paused.value) return []
  return store.eventLog.filter(e => {
    if (dirFilter.value !== 'all' && e.direction !== dirFilter.value) return false
    if (filter.value) {
      const q = filter.value.toLowerCase()
      return e.event.toLowerCase().includes(q)
        || (e.channel && e.channel.toLowerCase().includes(q))
        || (e.data && JSON.stringify(e.data).toLowerCase().includes(q))
    }
    return true
  })
})

function fmtTime(ts: number) {
  const d = new Date(ts)
  const t = d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
  return { time: t, ms: `.${d.getMilliseconds().toString().padStart(3, '0')}` }
}

function fmtData(data: unknown) {
  return typeof data === 'string' ? data : JSON.stringify(data, null, 2)
}
</script>

<template>
  <div class="space-y-4 animate-fade-in h-full flex flex-col">
    <div class="flex items-center justify-between">
      <div>
        <h2 class="text-lg font-bold text-surface-50">Event Log</h2>
        <p class="text-sm text-surface-400 mt-0.5">{{ store.eventLog.length }} events captured</p>
      </div>
      <div class="flex gap-2">
        <button @click="paused = !paused" class="btn-icon" :title="paused ? 'Resume' : 'Pause'">
          <Play v-if="paused" class="w-4 h-4" />
          <Pause v-else class="w-4 h-4" />
        </button>
        <button @click="store.clearEvents()" class="btn-icon" title="Clear">
          <Trash2 class="w-4 h-4" />
        </button>
      </div>
    </div>

    <!-- Filters -->
    <div class="flex gap-2 items-center">
      <div class="relative flex-1">
        <Search class="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-surface-500" />
        <input v-model="filter" class="input-field pl-8 text-xs" placeholder="Filter events..." />
      </div>
      <div class="flex gap-1">
        <button v-for="d in (['all', 'in', 'out', 'system'] as const)" :key="d" @click="dirFilter = d" :class="[dirFilter === d ? 'tab-btn-active' : 'tab-btn']">
          {{ d === 'all' ? 'All' : d.toUpperCase() }}
        </button>
      </div>
    </div>

    <!-- Event list -->
    <div class="panel flex-1 overflow-y-auto min-h-0" style="max-height: calc(100vh - 280px)">
      <div v-if="displayEvents.length === 0" class="flex items-center justify-center h-full text-surface-500 text-sm py-16">
        {{ paused ? 'Paused — events are being captured in the background' : 'No events yet' }}
      </div>
      <div v-else class="divide-y divide-surface-800/50">
        <div v-for="evt in displayEvents" :key="evt.id" class="hover:bg-surface-800/40 transition-colors">
          <div class="flex items-center gap-3 px-4 py-2.5 cursor-pointer" @click="expandedId = expandedId === evt.id ? null : evt.id">
            <div :class="['p-1 rounded', dirConfig[evt.direction].bg]">
              <component :is="dirConfig[evt.direction].icon" :class="['w-3 h-3', dirConfig[evt.direction].color]" />
            </div>
            <span class="text-[10px] font-mono text-surface-500 w-[85px] shrink-0">
              {{ fmtTime(evt.timestamp).time }}<span class="text-surface-600">{{ fmtTime(evt.timestamp).ms }}</span>
            </span>
            <span class="text-xs font-mono font-medium text-surface-200 truncate">{{ evt.event }}</span>
            <span v-if="evt.channel" class="inline-flex items-center px-2.5 py-0.5 rounded-full text-[10px] font-medium bg-surface-500/15 text-surface-400 ring-1 ring-surface-500/20 shrink-0">{{ evt.channel }}</span>
            <span :class="['ml-auto text-[10px] font-bold shrink-0', dirConfig[evt.direction].color]">{{ dirConfig[evt.direction].label }}</span>
          </div>
          <div v-if="expandedId === evt.id && evt.data != null" class="px-4 pb-3">
            <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-x-auto text-surface-300 max-h-[300px] overflow-y-auto">{{ fmtData(evt.data) }}</pre>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
