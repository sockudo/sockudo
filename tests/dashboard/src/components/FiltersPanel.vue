<script setup lang="ts">
import { ref, computed } from 'vue'
import { Filter, Plus, X, Play, Copy, Check } from 'lucide-vue-next'

type FilterOp = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'nin' | 'exists' | 'nexists' | 'prefix' | 'suffix' | 'contains'

interface FilterRule { id: string; op: FilterOp; key: string; value: string }

const OPS: { id: FilterOp; label: string; cat: string; val: boolean }[] = [
  { id: 'eq', label: '= Equal', cat: 'Compare', val: true },
  { id: 'neq', label: '!= Not Equal', cat: 'Compare', val: true },
  { id: 'gt', label: '> Greater Than', cat: 'Numeric', val: true },
  { id: 'gte', label: '>= Greater or Equal', cat: 'Numeric', val: true },
  { id: 'lt', label: '< Less Than', cat: 'Numeric', val: true },
  { id: 'lte', label: '<= Less or Equal', cat: 'Numeric', val: true },
  { id: 'in', label: 'IN Set', cat: 'Set', val: true },
  { id: 'nin', label: 'NOT IN Set', cat: 'Set', val: true },
  { id: 'exists', label: 'EXISTS', cat: 'Existence', val: false },
  { id: 'nexists', label: 'NOT EXISTS', cat: 'Existence', val: false },
  { id: 'prefix', label: 'Starts With', cat: 'Pattern', val: true },
  { id: 'suffix', label: 'Ends With', cat: 'Pattern', val: true },
  { id: 'contains', label: 'Contains', cat: 'Pattern', val: true },
]

const categories = [...new Set(OPS.map(o => o.cat))]
let ruleId = 0

const rules = ref<FilterRule[]>([])
const logicalOp = ref<'and' | 'or'>('and')
const copied = ref(false)
const testTags = ref('{"symbol": "BTC", "exchange": "NYSE", "volume": "5000"}')
const matchResult = ref<boolean | null>(null)

function addRule(op: FilterOp = 'eq') {
  rules.value.push({ id: `r-${++ruleId}`, op, key: '', value: '' })
}

function removeRule(id: string) {
  rules.value = rules.value.filter(r => r.id !== id)
}

function opNeedsValue(op: FilterOp) {
  return OPS.find(o => o.id === op)?.val ?? true
}

const filterCode = computed(() => {
  const valid = rules.value.filter(r => r.key.trim())
  if (!valid.length) return '// Add filter rules above'
  const lines = valid.map(r => {
    if (!opNeedsValue(r.op)) return `Filter.${r.op}("${r.key}")`
    if (r.op === 'in' || r.op === 'nin') {
      const vals = r.value.split(',').map(v => `"${v.trim()}"`)
      return `Filter.${r.op}("${r.key}", [${vals.join(', ')}])`
    }
    return `Filter.${r.op}("${r.key}", "${r.value}")`
  })
  if (lines.length === 1) return lines[0]
  return `Filter.${logicalOp.value}(\n  ${lines.join(',\n  ')}\n)`
})

const filterJson = computed(() => {
  const valid = rules.value.filter(r => r.key.trim())
  if (!valid.length) return null
  const filters = valid.map(r => {
    if (!opNeedsValue(r.op)) return { op: r.op, key: r.key }
    if (r.op === 'in' || r.op === 'nin') return { op: r.op, key: r.key, value: r.value.split(',').map(v => v.trim()) }
    return { op: r.op, key: r.key, value: r.value }
  })
  if (filters.length === 1) return filters[0]
  return { op: logicalOp.value, conditions: filters }
})

function handleCopy() {
  navigator.clipboard.writeText(filterCode.value)
  copied.value = true
  setTimeout(() => copied.value = false, 2000)
}

function testFilter() {
  if (!filterJson.value) return
  try {
    matchResult.value = evaluate(filterJson.value, JSON.parse(testTags.value))
  } catch { matchResult.value = null }
}

function evaluate(f: any, tags: Record<string, string>): boolean {
  if (f.op === 'and') return (f.conditions || []).every((c: any) => evaluate(c, tags))
  if (f.op === 'or') return (f.conditions || []).some((c: any) => evaluate(c, tags))
  const v = tags[f.key]
  switch (f.op) {
    case 'eq': return v === f.value
    case 'neq': return v !== f.value
    case 'gt': return v != null && parseFloat(v) > parseFloat(f.value)
    case 'gte': return v != null && parseFloat(v) >= parseFloat(f.value)
    case 'lt': return v != null && parseFloat(v) < parseFloat(f.value)
    case 'lte': return v != null && parseFloat(v) <= parseFloat(f.value)
    case 'in': return Array.isArray(f.value) && f.value.includes(v)
    case 'nin': return Array.isArray(f.value) && !f.value.includes(v)
    case 'exists': return v !== undefined
    case 'nexists': return v === undefined
    case 'prefix': return v != null && v.startsWith(f.value)
    case 'suffix': return v != null && v.endsWith(f.value)
    case 'contains': return v != null && v.includes(f.value)
    default: return false
  }
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">Tag Filters</h2>
      <p class="text-sm text-surface-400 mt-1">Build and test server-side message filters with all 13 operators</p>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Builder -->
      <div class="panel p-5 space-y-4">
        <div class="flex items-center justify-between">
          <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
            <Filter class="w-4 h-4 text-brand-400" /> Filter Builder
          </h3>
          <button @click="addRule()" class="btn-secondary btn-sm flex items-center gap-1">
            <Plus class="w-3.5 h-3.5" /> Add Rule
          </button>
        </div>

        <div v-if="rules.length > 1" class="flex items-center gap-2">
          <span class="text-xs text-surface-400">Combine with:</span>
          <div class="flex gap-1">
            <button v-for="op in (['and', 'or'] as const)" :key="op" @click="logicalOp = op" :class="[logicalOp === op ? 'tab-btn-active' : 'tab-btn']">{{ op.toUpperCase() }}</button>
          </div>
        </div>

        <div v-if="!rules.length" class="text-center py-8 text-surface-500 text-sm">Click "Add Rule" to start building a filter</div>
        <div v-else class="space-y-2">
          <div v-for="(rule, i) in rules" :key="rule.id" class="bg-surface-800/60 backdrop-blur-lg border border-surface-700/30 rounded-xl p-3 space-y-2 animate-slide-in">
            <div v-if="i > 0" class="text-[10px] font-bold text-brand-400 uppercase">{{ logicalOp }}</div>
            <div class="flex gap-2 items-start">
              <div class="flex-1 space-y-2">
                <div class="grid grid-cols-2 gap-2">
                  <div>
                    <label class="text-[10px] text-surface-500 mb-0.5 block">Tag Key</label>
                    <input v-model="rule.key" class="input-field font-mono text-xs" placeholder="symbol" />
                  </div>
                  <div>
                    <label class="text-[10px] text-surface-500 mb-0.5 block">Operator</label>
                    <select v-model="rule.op" class="input-field text-xs">
                      <optgroup v-for="cat in categories" :key="cat" :label="cat">
                        <option v-for="o in OPS.filter(o => o.cat === cat)" :key="o.id" :value="o.id">{{ o.label }}</option>
                      </optgroup>
                    </select>
                  </div>
                </div>
                <div v-if="opNeedsValue(rule.op)">
                  <label class="text-[10px] text-surface-500 mb-0.5 block">Value{{ (rule.op === 'in' || rule.op === 'nin') ? ' (comma-separated)' : '' }}</label>
                  <input v-model="rule.value" class="input-field font-mono text-xs" :placeholder="(rule.op === 'in' || rule.op === 'nin') ? 'BTC, ETH, SOL' : 'BTC'" />
                </div>
              </div>
              <button @click="removeRule(rule.id)" class="mt-4 p-1.5 rounded hover:bg-red-500/20 text-surface-500 hover:text-red-400 transition-all">
                <X class="w-3.5 h-3.5" />
              </button>
            </div>
          </div>
        </div>

        <!-- Operator reference -->
        <div>
          <p class="section-title">Operator Reference</p>
          <div class="grid grid-cols-2 gap-1">
            <div v-for="op in OPS" :key="op.id" @click="addRule(op.id)" class="text-[10px] font-mono text-surface-400 py-0.5 px-2 rounded bg-surface-800/40 cursor-pointer hover:bg-surface-700/40 hover:text-surface-300 transition-colors">
              <span class="text-brand-400">{{ op.id }}</span> — {{ op.label }}
            </div>
          </div>
        </div>
      </div>

      <!-- Output -->
      <div class="space-y-4">
        <div class="panel p-5 space-y-3">
          <div class="flex items-center justify-between">
            <h3 class="text-sm font-semibold text-surface-200">@sockudo/client Code</h3>
            <button @click="handleCopy" class="btn-icon p-1.5">
              <Check v-if="copied" class="w-3.5 h-3.5 text-emerald-400" />
              <Copy v-else class="w-3.5 h-3.5" />
            </button>
          </div>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-4 overflow-auto text-surface-300"><span class="text-surface-500">import { Filter } from "@sockudo/client/filter";

</span><span class="text-brand-400">const filter = </span>{{ filterCode }}<span class="text-surface-500">;</span></pre>
        </div>

        <div class="panel p-5 space-y-3">
          <h3 class="text-sm font-semibold text-surface-200">Filter JSON</h3>
          <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-4 overflow-auto max-h-[300px] text-surface-300">{{ filterJson ? JSON.stringify(filterJson, null, 2) : '// Build a filter to see JSON output' }}</pre>
        </div>

        <div class="panel p-5 space-y-3">
          <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
            <Play class="w-4 h-4 text-brand-400" /> Test Filter Locally
          </h3>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Test Tags (JSON)</label>
            <textarea v-model="testTags" class="input-field font-mono text-xs h-16 resize-none" />
          </div>
          <button @click="testFilter" class="btn-primary btn-sm w-full" :disabled="!filterJson">Test Match</button>
          <div v-if="matchResult !== null" :class="['text-center py-2 rounded-lg text-sm font-medium', matchResult ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-400']">
            {{ matchResult ? 'MATCH' : 'NO MATCH' }}
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
