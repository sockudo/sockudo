<script setup lang="ts">
import { computed, ref } from 'vue'
import { Activity, Gauge, HeartPulse, RadioTower, Send, ShieldAlert, Webhook } from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { useHttpApi, type ApiResponse } from '../composables/useHttpApi'

const store = useDashboardStore()

const metricsPort = ref(9601)
const probeResult = ref<ApiResponse | null>(null)
const metricsResult = ref<ApiResponse | null>(null)
const statsResult = ref<ApiResponse | null>(null)
const usageResult = ref<ApiResponse | null>(null)

const rateProbeCount = ref(20)
const rateProbePrefix = ref('')
const rateProbeRunning = ref(false)
const rateProbeResults = ref<ApiResponse[]>([])

const webhookChannel = ref('webhook-demo')
const webhookEvent = ref('webhook.probe')
const webhookPayload = ref('{"source":"tests/dashboard","kind":"operator-probe"}')
const webhookResult = ref<ApiResponse | null>(null)

const rateSummary = computed(() => {
  const counts = new Map<number, number>()
  for (const result of rateProbeResults.value) {
    counts.set(result.status, (counts.get(result.status) ?? 0) + 1)
  }
  return Array.from(counts.entries()).sort(([left], [right]) => left - right)
})

function fmtResponse(data: unknown) {
  return typeof data === 'string' ? data : JSON.stringify(data, null, 2)
}

function parseMaybeJson(raw: string): unknown {
  try {
    return JSON.parse(raw)
  } catch {
    return raw
  }
}

async function runProbe(kind: 'live' | 'up' | 'app-up') {
  const api = useHttpApi()
  if (kind === 'live') probeResult.value = await api.liveCheck()
  if (kind === 'up') probeResult.value = await api.upCheck()
  if (kind === 'app-up') probeResult.value = await api.appUpCheck()
}

async function runUsage() {
  usageResult.value = await useHttpApi().usageCheck()
}

async function runStats() {
  statsResult.value = await useHttpApi().statsCheck()
}

async function scrapeMetrics() {
  metricsResult.value = await useHttpApi().metricsCheck(metricsPort.value)
}

async function runRateProbe() {
  const api = useHttpApi()
  rateProbeRunning.value = true
  rateProbeResults.value = []
  for (let i = 0; i < rateProbeCount.value; i++) {
    rateProbeResults.value.push(await api.getChannels(rateProbePrefix.value || undefined))
  }
  rateProbeRunning.value = false
}

async function publishWebhookProbe() {
  webhookResult.value = await useHttpApi().publishAdvancedEvent({
    name: webhookEvent.value.trim(),
    channel: webhookChannel.value.trim(),
    data: parseMaybeJson(webhookPayload.value),
    idempotency_key: `webhook-probe-${Date.now()}`,
    info: 'subscription_count',
  })
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">Operations</h2>
      <p class="text-sm text-surface-400 mt-1">Health probes, usage, stats, metrics, rate-limit behavior, and webhook trigger paths.</p>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-4 gap-6">
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <HeartPulse class="w-4 h-4 text-brand-400" /> Health
        </h3>
        <div class="grid grid-cols-3 gap-2">
          <button @click="runProbe('live')" class="btn-secondary btn-sm">Live</button>
          <button @click="runProbe('up')" class="btn-secondary btn-sm">Up</button>
          <button @click="runProbe('app-up')" class="btn-secondary btn-sm">App</button>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button @click="runUsage" class="btn-secondary btn-sm">Usage</button>
          <button @click="runStats" class="btn-primary btn-sm">Stats</button>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Gauge class="w-4 h-4 text-brand-400" /> Metrics
        </h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Metrics Port</label>
          <input v-model.number="metricsPort" type="number" class="input-field font-mono" />
        </div>
        <button @click="scrapeMetrics" class="btn-primary btn-sm w-full flex items-center justify-center gap-2">
          <RadioTower class="w-4 h-4" /> Scrape
        </button>
        <div class="text-xs text-surface-500">
          <p>Default: <span class="font-mono text-surface-300">9601</span></p>
          <p>Endpoint: <span class="font-mono text-surface-300">/metrics</span></p>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <ShieldAlert class="w-4 h-4 text-brand-400" /> Rate Probe
        </h3>
        <div class="grid grid-cols-2 gap-2">
          <input v-model.number="rateProbeCount" type="number" min="1" max="200" class="input-field font-mono" />
          <input v-model="rateProbePrefix" class="input-field font-mono text-xs" placeholder="prefix" />
        </div>
        <button @click="runRateProbe" :disabled="rateProbeRunning" class="btn-primary btn-sm w-full">
          {{ rateProbeRunning ? `Running ${rateProbeResults.length}/${rateProbeCount}` : 'Run Signed Burst' }}
        </button>
        <div v-if="rateSummary.length" class="flex flex-wrap gap-1.5">
          <span v-for="[status, count] in rateSummary" :key="status" class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-surface-500/15 text-surface-300 ring-1 ring-surface-500/20">
            {{ status }} x {{ count }}
          </span>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Webhook class="w-4 h-4 text-brand-400" /> Webhook Trigger
        </h3>
        <div class="grid grid-cols-2 gap-2">
          <input v-model="webhookChannel" class="input-field font-mono text-xs" />
          <input v-model="webhookEvent" class="input-field font-mono text-xs" />
        </div>
        <textarea v-model="webhookPayload" class="input-field font-mono text-xs h-20 resize-none" />
        <button @click="publishWebhookProbe" class="btn-primary btn-sm w-full flex items-center justify-center gap-2">
          <Send class="w-4 h-4" /> Publish
        </button>
      </div>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-4 gap-6">
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Health / Usage</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse({ probe: probeResult?.data, usage: usageResult?.data }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Stats</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse(statsResult?.data ?? null) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Metrics / Rate</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse({ metrics: metricsResult?.data, rate: rateSummary }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Activity class="w-4 h-4 text-brand-400" /> Webhook Probe
        </h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[420px] text-surface-300">{{ fmtResponse(webhookResult?.data ?? null) }}</pre>
      </div>
    </div>
  </div>
</template>
