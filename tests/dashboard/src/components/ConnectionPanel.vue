<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { Wifi, Activity, Settings2, Plug, Unplug, Server, UserCheck, ShieldCheck } from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { usePusher } from '../composables/usePusher'

const store = useDashboardStore()
const { connect, disconnect, sendPing, signin } = usePusher()

const showAdvanced = ref(false)
const authStatus = ref<'unknown' | 'online' | 'offline'>('unknown')

function reconnect() {
  disconnect()
  window.setTimeout(connect, 500)
}

async function checkAuth() {
  try {
    const res = await fetch('http://localhost:3457/health', { signal: AbortSignal.timeout(2000) })
    authStatus.value = res.ok ? 'online' : 'offline'
  } catch {
    authStatus.value = 'offline'
  }
}

onMounted(checkAuth)
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">Connection</h2>
      <p class="text-sm text-surface-400 mt-1">Configure and manage your WebSocket connection</p>
    </div>

    <!-- Auth server warning -->
    <div v-if="authStatus === 'offline'" class="bg-surface-900/80 backdrop-blur-xl border border-amber-500/30 bg-amber-500/5 rounded-2xl p-4 flex items-start gap-3">
      <Server class="w-5 h-5 text-amber-400 shrink-0 mt-0.5" />
      <div>
        <p class="text-sm font-medium text-amber-300">Auth server not running</p>
        <p class="text-xs text-surface-400 mt-1">Private and presence channels require it. Start with:</p>
        <code class="text-xs font-mono text-amber-400/80 bg-surface-900/60 px-2 py-1 rounded mt-1.5 block w-fit">bun run dev:server</code>
      </div>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
      <!-- Config -->
      <div class="bg-surface-900/80 backdrop-blur-xl border border-surface-700/40 rounded-2xl p-5 space-y-4">
        <div class="flex items-center justify-between">
          <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
            <Settings2 class="w-4 h-4 text-brand-400" />
            Configuration
          </h3>
          <button @click="showAdvanced = !showAdvanced" class="text-xs text-surface-500 hover:text-surface-300 transition-colors">
            {{ showAdvanced ? 'Simple' : 'Advanced' }}
          </button>
        </div>

        <div class="grid grid-cols-2 gap-3">
          <div class="col-span-2">
            <label class="text-xs text-surface-400 mb-1 block">App Key</label>
            <input v-model="store.config.appKey" class="input-field font-mono" placeholder="app-key" />
          </div>
          <div class="col-span-2">
            <label class="text-xs text-surface-400 mb-1 block">App Secret</label>
            <input v-model="store.config.appSecret" type="password" class="input-field font-mono" placeholder="app-secret" />
          </div>
          <div class="col-span-2">
            <label class="text-xs text-surface-400 mb-1 block">App ID</label>
            <input v-model="store.config.appId" class="input-field font-mono" placeholder="app-id" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Host</label>
            <input v-model="store.config.host" class="input-field font-mono" placeholder="127.0.0.1" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Port</label>
            <input v-model.number="store.config.port" type="number" class="input-field font-mono" />
          </div>
        </div>

        <div v-if="showAdvanced" class="space-y-3 pt-2 border-t border-surface-700/30">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Cluster</label>
            <input v-model="store.config.cluster" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Auth Endpoint</label>
            <input v-model="store.config.authEndpoint" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">User Auth Endpoint</label>
            <input v-model="store.config.userAuthEndpoint" class="input-field font-mono" />
          </div>
          <label class="flex items-center gap-2 cursor-pointer">
            <input v-model="store.config.useTLS" type="checkbox" class="w-4 h-4 rounded border-surface-600 bg-surface-800 text-brand-500 accent-brand-500" />
            <span class="text-xs text-surface-300">Use TLS (wss://)</span>
          </label>
        </div>

        <div class="flex gap-2 pt-2">
          <button v-if="store.connectionState !== 'connected'" @click="connect" :disabled="store.connectionState === 'connecting'" class="btn-primary flex items-center gap-2 flex-1">
            <Plug class="w-4 h-4" />
            {{ store.connectionState === 'connecting' ? 'Connecting...' : 'Connect' }}
          </button>
          <button v-else @click="disconnect" class="btn-danger flex items-center gap-2 flex-1">
            <Unplug class="w-4 h-4" />
            Disconnect
          </button>
        </div>
      </div>

      <!-- Status -->
      <div class="space-y-4">
        <div class="bg-surface-900/80 backdrop-blur-xl border border-surface-700/40 rounded-2xl p-5">
          <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2 mb-4">
            <Activity class="w-4 h-4 text-brand-400" />
            Status
          </h3>
          <div class="space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs text-surface-500">State</span>
              <span :class="[
                'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ring-1',
                store.connectionState === 'connected' ? 'bg-emerald-500/15 text-emerald-400 ring-emerald-500/20' :
                store.connectionState === 'failed' ? 'bg-red-500/15 text-red-400 ring-red-500/20' :
                'bg-amber-500/15 text-amber-400 ring-amber-500/20'
              ]">{{ store.connectionState }}</span>
            </div>
            <div class="flex items-center justify-between">
              <span class="text-xs text-surface-500">Socket ID</span>
              <span class="font-mono text-xs">{{ store.socketId || '—' }}</span>
            </div>
            <div class="flex items-center justify-between">
              <span class="text-xs text-surface-500">Protocol</span>
              <span class="text-xs text-surface-300">Pusher v7</span>
            </div>
            <div class="flex items-center justify-between">
              <span class="text-xs text-surface-500">Transport</span>
              <span class="text-xs text-surface-300">{{ store.config.useTLS ? 'wss' : 'ws' }}</span>
            </div>
            <div class="flex items-center justify-between">
              <span class="text-xs text-surface-500">Endpoint</span>
              <span class="text-xs font-mono text-surface-400 truncate max-w-[200px] block">
                {{ store.config.useTLS ? 'wss' : 'ws' }}://{{ store.config.host }}:{{ store.config.port }}/app/{{ store.config.appKey }}
              </span>
            </div>
            <div class="flex items-center justify-between">
              <span class="text-xs text-surface-500">Auth Server</span>
              <button @click="checkAuth" class="flex items-center gap-1.5">
                <div :class="['w-2 h-2 rounded-full', authStatus === 'online' ? 'bg-emerald-400' : authStatus === 'offline' ? 'bg-red-400' : 'bg-surface-500 animate-pulse-dot']" />
                <span class="text-xs text-surface-300">{{ authStatus }}</span>
              </button>
            </div>
          </div>
        </div>

        <!-- Quick actions -->
        <div class="bg-surface-900/80 backdrop-blur-xl border border-surface-700/40 rounded-2xl p-5">
          <h3 class="text-sm font-semibold text-surface-200 mb-4">Quick Actions</h3>
          <div class="grid grid-cols-3 gap-2">
            <button @click="sendPing" :disabled="store.connectionState !== 'connected'" class="btn-secondary btn-sm flex items-center justify-center gap-1.5">
              <Activity class="w-3.5 h-3.5" /> Ping
            </button>
            <button @click="signin" :disabled="store.connectionState !== 'connected'" class="btn-secondary btn-sm flex items-center justify-center gap-1.5">
              <UserCheck class="w-3.5 h-3.5" /> Sign In
            </button>
            <button @click="reconnect" :disabled="store.connectionState !== 'connected'" class="btn-secondary btn-sm flex items-center justify-center gap-1.5">
              <Wifi class="w-3.5 h-3.5" /> Reconnect
            </button>
          </div>
        </div>

        <!-- Auth info -->
        <div class="bg-surface-900/80 backdrop-blur-xl border border-surface-700/40 rounded-2xl p-5">
          <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2 mb-3">
            <ShieldCheck class="w-4 h-4 text-brand-400" /> Auth Server
          </h3>
          <p class="text-xs text-surface-400 leading-relaxed">
            Handles channel authorization for private/presence channels and user authentication for signin.
            Uses the Pusher server SDK to generate valid HMAC signatures.
          </p>
          <div class="mt-3 space-y-1.5 text-xs font-mono">
            <div class="flex items-center gap-2">
              <span class="text-surface-500">POST</span>
              <span class="text-brand-400">/pusher/auth</span>
              <span class="text-surface-600">— channel auth</span>
            </div>
            <div class="flex items-center gap-2">
              <span class="text-surface-500">POST</span>
              <span class="text-brand-400">/pusher/user-auth</span>
              <span class="text-surface-600">— user signin</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
