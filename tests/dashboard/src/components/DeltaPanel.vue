<script setup lang="ts">
import { ref, computed } from 'vue'
import { BarChart3, Zap, TrendingDown, Activity, Send, Radio, ArrowDown, Hash } from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { useHttpApi } from '../composables/useHttpApi'
import { usePusher } from '../composables/usePusher'

const store = useDashboardStore()
const { subscribe, bindEvent } = usePusher()

const channel = ref('ticker:btc')
const burstCount = ref(20)
const sending = ref(false)
const burstResults = ref<string[]>([])
const subscribed = ref(false)
const httpBytesSent = ref(0)

const ds = computed(() => store.deltaStats)
const isConnected = computed(() => store.connectionState === 'connected')

function fmtBytes(bytes: number) {
  if (bytes === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`
}

function resetStats() {
  httpBytesSent.value = 0
  burstResults.value = []
}

function handleSubscribe() {
  if (subscribed.value) return
  subscribe(channel.value)
  subscribed.value = true
}

// Generate a large (~1-2 KB) message where only a few fields change between calls
// This is realistic for market data: lots of static fields, only price/volume tick
const STATIC_ORDERBOOK = Array.from({ length: 10 }, (_, i) => ({
  level: i + 1,
  bid_qty: (100 - i * 5).toFixed(2),
  ask_qty: (100 - i * 5).toFixed(2),
}))

function buildMessage() {
  const price = (50000 + Math.random() * 1000).toFixed(2)
  const volume = (Math.random() * 10000).toFixed(2)
  return JSON.stringify({
    item_id: 'BTC',
    symbol: 'BTC',
    exchange: 'NYSE',
    asset_class: 'cryptocurrency',
    currency: 'USD',
    price,
    volume,
    timestamp: Date.now(),
    bid: (parseFloat(price) - 0.5).toFixed(2),
    ask: (parseFloat(price) + 0.5).toFixed(2),
    high_24h: '51200.00',
    low_24h: '49800.00',
    open_24h: '50100.00',
    vwap_24h: '50450.00',
    change_24h: (parseFloat(price) - 50100).toFixed(2),
    change_pct_24h: ((parseFloat(price) - 50100) / 50100 * 100).toFixed(4),
    market_cap: '987654321000',
    circulating_supply: '19500000',
    total_supply: '21000000',
    last_trade_size: (Math.random() * 2).toFixed(6),
    last_trade_side: Math.random() > 0.5 ? 'buy' : 'sell',
    orderbook_depth: STATIC_ORDERBOOK,
    metadata: {
      source: 'sockudo-dashboard-test',
      version: '1.0.0',
      sequence: Date.now(),
      region: 'us-east-1',
    },
  })
}

async function sendBurst() {
  const api = useHttpApi()
  sending.value = true
  burstResults.value = []

  if (!subscribed.value && isConnected.value) handleSubscribe()

  for (let i = 0; i < burstCount.value; i++) {
    const data = buildMessage()
    const res = await api.publishEvent(channel.value, 'price-update', data, { item_id: 'BTC', symbol: 'BTC' })
    if (res.status === 200) {
      httpBytesSent.value += data.length
    }
    burstResults.value.push(`[${i + 1}] ${res.status === 200 ? 'OK' : `ERR ${res.status}`} (${data.length} bytes)`)
  }
  sending.value = false
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">Delta Compression</h2>
      <p class="text-sm text-surface-400 mt-1">Test delta compression with message bursts and monitor real wire-level bandwidth savings</p>
    </div>

    <!-- Stats from @sockudo/client delta manager -->
    <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
      <div class="panel p-3.5">
        <div class="flex items-center gap-1.5 mb-1.5">
          <div class="p-1 rounded-md text-brand-400 bg-brand-500/10"><Hash class="w-3.5 h-3.5" /></div>
          <span class="text-[10px] text-surface-500">Total Msgs</span>
        </div>
        <p class="text-lg font-bold text-surface-100 font-mono">{{ ds.totalMessages }}</p>
      </div>
      <div class="panel p-3.5">
        <div class="flex items-center gap-1.5 mb-1.5">
          <div class="p-1 rounded-md text-emerald-400 bg-emerald-500/10"><ArrowDown class="w-3.5 h-3.5" /></div>
          <span class="text-[10px] text-surface-500">Delta Msgs</span>
        </div>
        <p class="text-lg font-bold text-emerald-400 font-mono">{{ ds.deltaMessages }}</p>
      </div>
      <div class="panel p-3.5">
        <div class="flex items-center gap-1.5 mb-1.5">
          <div class="p-1 rounded-md text-surface-400 bg-surface-500/10"><Send class="w-3.5 h-3.5" /></div>
          <span class="text-[10px] text-surface-500">Full Msgs</span>
        </div>
        <p class="text-lg font-bold text-surface-300 font-mono">{{ ds.fullMessages }}</p>
      </div>
      <div class="panel p-3.5">
        <div class="flex items-center gap-1.5 mb-1.5">
          <div class="p-1 rounded-md text-amber-400 bg-amber-500/10"><Activity class="w-3.5 h-3.5" /></div>
          <span class="text-[10px] text-surface-500">Wire Bytes</span>
        </div>
        <p class="text-lg font-bold text-surface-100 font-mono">{{ fmtBytes(ds.totalBytesWithCompression) }}</p>
        <p class="text-[10px] text-surface-500 font-mono">of {{ fmtBytes(ds.totalBytesWithoutCompression) }} original</p>
      </div>
      <div class="panel p-3.5">
        <div class="flex items-center gap-1.5 mb-1.5">
          <div class="p-1 rounded-md text-purple-400 bg-purple-500/10"><TrendingDown class="w-3.5 h-3.5" /></div>
          <span class="text-[10px] text-surface-500">Saved</span>
        </div>
        <p class="text-lg font-bold text-purple-400 font-mono">{{ fmtBytes(ds.bandwidthSaved) }}</p>
      </div>
      <div class="panel p-3.5">
        <div class="flex items-center gap-1.5 mb-1.5">
          <div class="p-1 rounded-md text-emerald-400 bg-emerald-500/10"><BarChart3 class="w-3.5 h-3.5" /></div>
          <span class="text-[10px] text-surface-500">Saved %</span>
        </div>
        <p class="text-lg font-bold font-mono" :class="ds.bandwidthSavedPercent > 50 ? 'text-emerald-400' : ds.bandwidthSavedPercent > 0 ? 'text-amber-400' : 'text-surface-400'">
          {{ ds.bandwidthSavedPercent.toFixed(1) }}%
        </p>
      </div>
    </div>

    <!-- Visual bar comparison -->
    <div v-if="ds.totalMessages > 0" class="panel p-5 space-y-3">
      <h3 class="text-sm font-semibold text-surface-200">Bandwidth Comparison</h3>
      <div class="space-y-2">
        <div>
          <div class="flex justify-between text-xs mb-1">
            <span class="text-surface-400">Without delta (original)</span>
            <span class="text-surface-300 font-mono">{{ fmtBytes(ds.totalBytesWithoutCompression) }}</span>
          </div>
          <div class="w-full h-3 rounded-full bg-surface-800">
            <div class="h-3 rounded-full bg-red-500/60" style="width: 100%" />
          </div>
        </div>
        <div>
          <div class="flex justify-between text-xs mb-1">
            <span class="text-surface-400">With delta (wire)</span>
            <span class="text-emerald-400 font-mono">{{ fmtBytes(ds.totalBytesWithCompression) }}</span>
          </div>
          <div class="w-full h-3 rounded-full bg-surface-800">
            <div
              class="h-3 rounded-full bg-emerald-500/60 transition-all duration-500"
              :style="{ width: ds.totalBytesWithoutCompression > 0 ? `${(ds.totalBytesWithCompression / ds.totalBytesWithoutCompression) * 100}%` : '0%' }"
            />
          </div>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Burst sender -->
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Zap class="w-4 h-4 text-brand-400" /> Message Burst Test
        </h3>
        <p class="text-xs text-surface-400">
          Sends large (~1.2 KB) market data messages where only price/volume change between messages.
          Uses <span class="font-mono text-surface-300">ticker:*</span> channel with
          <span class="font-mono text-surface-300">item_id</span> conflation key.
        </p>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Channel</label>
            <input v-model="channel" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Burst Count</label>
            <input v-model.number="burstCount" type="number" min="1" max="100" class="input-field font-mono" />
          </div>
        </div>

        <!-- Subscribe hint -->
        <div v-if="isConnected && !subscribed" class="flex items-center gap-2 p-2.5 rounded-lg bg-brand-500/5 border border-brand-500/20">
          <Radio class="w-4 h-4 text-brand-400 shrink-0" />
          <p class="text-xs text-brand-300 flex-1">Subscribe to <span class="font-mono">{{ channel }}</span> first</p>
          <button @click="handleSubscribe" class="btn-primary btn-sm text-[11px]">Subscribe</button>
        </div>
        <div v-if="subscribed" class="flex items-center gap-2 text-xs">
          <div class="w-2 h-2 rounded-full bg-emerald-400" />
          <span class="text-surface-400">Listening on <span class="font-mono text-surface-300">{{ channel }}</span></span>
        </div>

        <button @click="sendBurst" :disabled="sending || !isConnected" class="btn-primary w-full flex items-center justify-center gap-2">
          <Zap class="w-4 h-4" /> {{ sending ? `Sending... (${burstResults.length}/${burstCount})` : `Send ${burstCount} Messages` }}
        </button>

        <button @click="resetStats" class="btn-secondary w-full btn-sm">Reset Local Stats</button>
      </div>

      <!-- Results -->
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200">Burst Results</h3>
        <div v-if="!burstResults.length" class="text-center py-12 text-surface-500 text-sm">Run a burst test to see results</div>
        <div v-else class="space-y-1 max-h-[400px] overflow-y-auto">
          <div v-for="(line, i) in burstResults" :key="i" :class="['text-xs font-mono px-3 py-1.5 rounded', line.includes('OK') ? 'bg-emerald-500/5 text-emerald-400/80' : 'bg-red-500/5 text-red-400/80']">{{ line }}</div>
          <div class="border-t border-surface-700/30 pt-2 mt-2">
            <p class="text-xs text-surface-400">
              Total: {{ burstResults.length }} |
              Success: {{ burstResults.filter(l => l.includes('OK')).length }} |
              HTTP payload: {{ fmtBytes(httpBytesSent) }}
            </p>
          </div>
        </div>
      </div>
    </div>

    <!-- Algorithm info -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Fossil Delta</h3>
        <p class="text-xs text-surface-400">Fast general-purpose delta encoding. Good balance of speed and compression ratio.</p>
        <div class="grid grid-cols-3 gap-3">
          <div><p class="text-[10px] text-surface-500 uppercase">Compression</p><p class="text-sm font-mono text-emerald-400">60-80%</p></div>
          <div><p class="text-[10px] text-surface-500 uppercase">Speed</p><p class="text-sm font-mono text-brand-400">Fast</p></div>
          <div><p class="text-[10px] text-surface-500 uppercase">Best For</p><p class="text-xs text-surface-300">General real-time</p></div>
        </div>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">XDelta3 (VCDIFF)</h3>
        <p class="text-xs text-surface-400">RFC 3284 compliant delta encoding. Better compression for large payloads.</p>
        <div class="grid grid-cols-3 gap-3">
          <div><p class="text-[10px] text-surface-500 uppercase">Compression</p><p class="text-sm font-mono text-emerald-400">70-90%</p></div>
          <div><p class="text-[10px] text-surface-500 uppercase">Speed</p><p class="text-sm font-mono text-brand-400">Moderate</p></div>
          <div><p class="text-[10px] text-surface-500 uppercase">Best For</p><p class="text-xs text-surface-300">Large payloads</p></div>
        </div>
      </div>
    </div>
  </div>
</template>
