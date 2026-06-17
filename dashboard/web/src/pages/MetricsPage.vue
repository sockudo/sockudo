<script setup lang="ts">
import { onMounted, onUnmounted, ref } from "vue";
import { RefreshCw } from "lucide-vue-next";
import { api, type MetricsSummary } from "@/api/client";

const summary = ref<MetricsSummary | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);
let timer: ReturnType<typeof setInterval> | null = null;

onMounted(() => {
  load();
  timer = setInterval(load, 15_000);
});

onUnmounted(() => {
  if (timer) clearInterval(timer);
});

async function load() {
  try {
    summary.value = await api.metricsSummary();
    error.value = null;
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Metrics unavailable";
  } finally {
    loading.value = false;
  }
}

function formatNum(n: number) {
  return new Intl.NumberFormat().format(Math.round(n));
}
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <div>
        <h1 class="text-2xl font-semibold text-surface-100">Metrics</h1>
        <p class="text-sm text-surface-400 mt-1">
          Live Prometheus summary from Sockudo (refreshes every 15s).
        </p>
      </div>
      <button class="btn-secondary flex items-center gap-2" @click="load">
        <RefreshCw class="w-4 h-4" />
        Refresh
      </button>
    </div>

    <p v-if="error" class="text-amber-400 text-sm mb-4">
      {{ error }} — ensure Sockudo metrics are enabled on port 9601.
    </p>

    <div v-if="loading && !summary" class="text-surface-400">Loading metrics...</div>

    <template v-else-if="summary">
      <div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <div class="panel p-4">
          <p class="text-xs text-surface-500">Connected sockets</p>
          <p class="text-2xl font-semibold mt-1">{{ formatNum(summary.connected_sockets) }}</p>
        </div>
        <div class="panel p-4">
          <p class="text-xs text-surface-500">WS messages in</p>
          <p class="text-2xl font-semibold mt-1">
            {{ formatNum(summary.ws_messages_received_total) }}
          </p>
        </div>
        <div class="panel p-4">
          <p class="text-xs text-surface-500">WS messages out</p>
          <p class="text-2xl font-semibold mt-1">
            {{ formatNum(summary.ws_messages_sent_total) }}
          </p>
        </div>
        <div class="panel p-4">
          <p class="text-xs text-surface-500">HTTP API calls</p>
          <p class="text-2xl font-semibold mt-1">
            {{ formatNum(summary.http_calls_received_total) }}
          </p>
        </div>
        <div class="panel p-4">
          <p class="text-xs text-surface-500">Subscriptions</p>
          <p class="text-2xl font-semibold mt-1">
            {{ formatNum(summary.channel_subscriptions_total) }}
          </p>
        </div>
        <div class="panel p-4">
          <p class="text-xs text-surface-500">Connection errors</p>
          <p class="text-2xl font-semibold mt-1 text-red-400">
            {{ formatNum(summary.connection_errors_total) }}
          </p>
        </div>
        <div class="panel p-4">
          <p class="text-xs text-surface-500">Rate limits hit</p>
          <p class="text-2xl font-semibold mt-1 text-amber-400">
            {{ formatNum(summary.rate_limit_triggered_total) }}
          </p>
        </div>
        <div class="panel p-4">
          <p class="text-xs text-surface-500">Total connections (lifetime)</p>
          <p class="text-2xl font-semibold mt-1">
            {{ formatNum(summary.new_connections_total) }}
          </p>
        </div>
      </div>

      <div class="panel p-6">
        <h2 class="font-medium text-surface-200 mb-4">Connections by app</h2>
        <div v-if="summary.by_app.length === 0" class="text-surface-500 text-sm">
          No per-app socket metrics yet.
        </div>
        <table v-else class="w-full text-sm">
          <thead class="text-surface-400 text-left">
            <tr>
              <th class="pb-2 font-medium">App ID</th>
              <th class="pb-2 font-medium text-right">Connected sockets</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="row in summary.by_app"
              :key="row.app_id"
              class="border-t border-surface-800"
            >
              <td class="py-2 font-mono text-brand-300">{{ row.app_id }}</td>
              <td class="py-2 text-right">{{ formatNum(row.connected_sockets) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>
  </div>
</template>
