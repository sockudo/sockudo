<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { Plus, Trash2, Pencil } from "lucide-vue-next";
import { api, type App, type StatsResponse } from "@/api/client";

const router = useRouter();
const apps = ref<App[]>([]);
const stats = ref<StatsResponse | null>(null);
const loading = ref(true);
const error = ref<string | null>(null);
const showCreate = ref(false);

const form = ref({
  id: "",
  key: "",
  secret: "",
  enabled: true,
});

onMounted(load);

async function load() {
  loading.value = true;
  error.value = null;
  try {
    const [appList, statsData] = await Promise.all([
      api.listApps(),
      api.stats().catch(() => null),
    ]);
    apps.value = appList;
    stats.value = statsData;
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Failed to load apps";
  } finally {
    loading.value = false;
  }
}

function connectionsFor(appId: string) {
  return stats.value?.apps.find((a) => a.app_id === appId)?.connections ?? 0;
}

async function createApp() {
  try {
    const created = await api.createApp({
      id: form.value.id.trim(),
      key: form.value.key.trim(),
      secret: form.value.secret.trim() || undefined,
      enabled: form.value.enabled,
    });
    showCreate.value = false;
    form.value = { id: "", key: "", secret: "", enabled: true };
    router.push({ name: "app-detail", params: { id: created.id } });
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Create failed";
  }
}

async function removeApp(app: App) {
  if (!confirm(`Delete app "${app.id}"?`)) return;
  try {
    await api.deleteApp(app.id);
    await load();
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Delete failed";
  }
}
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <div>
        <h1 class="text-2xl font-semibold text-surface-100">Applications</h1>
        <p class="text-sm text-surface-400 mt-1">
          Manage Sockudo apps stored in your configured app manager database.
        </p>
      </div>
      <button class="btn-primary flex items-center gap-2" @click="showCreate = !showCreate">
        <Plus class="w-4 h-4" />
        New app
      </button>
    </div>

    <div v-if="stats" class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
      <div class="panel p-4">
        <p class="text-xs text-surface-500">Registered apps</p>
        <p class="text-2xl font-semibold mt-1">{{ stats.totals.apps }}</p>
      </div>
      <div class="panel p-4">
        <p class="text-xs text-surface-500">Live connections</p>
        <p class="text-2xl font-semibold mt-1">{{ stats.totals.connections }}</p>
      </div>
      <div class="panel p-4">
        <p class="text-xs text-surface-500">Users online</p>
        <p class="text-2xl font-semibold mt-1">{{ stats.totals.users }}</p>
      </div>
      <div class="panel p-4">
        <p class="text-xs text-surface-500">Memory used</p>
        <p class="text-2xl font-semibold mt-1">{{ stats.memory.percent.toFixed(1) }}%</p>
      </div>
    </div>

    <div v-if="showCreate" class="panel p-6 mb-6 space-y-4">
      <h2 class="font-medium text-surface-200">Create application</h2>
      <div class="grid md:grid-cols-2 gap-4">
        <div>
          <label class="section-title">App ID</label>
          <input v-model="form.id" class="input-field" placeholder="my-app" />
        </div>
        <div>
          <label class="section-title">App Key</label>
          <input v-model="form.key" class="input-field" placeholder="my-app-key" />
        </div>
        <div class="md:col-span-2">
          <label class="section-title">Secret (optional — auto-generated)</label>
          <input v-model="form.secret" class="input-field font-mono" />
        </div>
      </div>
      <div class="flex gap-2">
        <button class="btn-primary" @click="createApp">Create</button>
        <button class="btn-secondary" @click="showCreate = false">Cancel</button>
      </div>
    </div>

    <p v-if="error" class="text-red-400 text-sm mb-4">{{ error }}</p>

    <div v-if="loading" class="text-surface-400">Loading apps...</div>

    <div v-else class="panel overflow-hidden">
      <table class="w-full text-sm">
        <thead class="bg-surface-800/50 text-surface-400 text-left">
          <tr>
            <th class="px-4 py-3 font-medium">ID</th>
            <th class="px-4 py-3 font-medium">Key</th>
            <th class="px-4 py-3 font-medium">Status</th>
            <th class="px-4 py-3 font-medium">Connections</th>
            <th class="px-4 py-3 font-medium">Webhooks</th>
            <th class="px-4 py-3 font-medium text-right">Actions</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="app in apps"
            :key="app.id"
            class="border-t border-surface-800 hover:bg-surface-800/30"
          >
            <td class="px-4 py-3 font-mono text-brand-300">{{ app.id }}</td>
            <td class="px-4 py-3 font-mono text-surface-300">{{ app.key }}</td>
            <td class="px-4 py-3">
              <span
                class="text-xs px-2 py-0.5 rounded-full"
                :class="app.enabled ? 'bg-green-500/15 text-green-400' : 'bg-red-500/15 text-red-400'"
              >
                {{ app.enabled ? "enabled" : "disabled" }}
              </span>
            </td>
            <td class="px-4 py-3">{{ connectionsFor(app.id) }}</td>
            <td class="px-4 py-3">{{ app.webhook_count ?? app.policy.webhooks?.length ?? 0 }}</td>
            <td class="px-4 py-3">
              <div class="flex justify-end gap-2">
                <button
                  class="btn-secondary btn-sm flex items-center gap-1"
                  @click="router.push({ name: 'app-detail', params: { id: app.id } })"
                >
                  <Pencil class="w-3 h-3" />
                  Edit
                </button>
                <button
                  class="btn-danger btn-sm flex items-center gap-1"
                  @click="removeApp(app)"
                >
                  <Trash2 class="w-3 h-3" />
                </button>
              </div>
            </td>
          </tr>
          <tr v-if="apps.length === 0">
            <td colspan="6" class="px-4 py-8 text-center text-surface-500">
              No apps yet. Create one to get started.
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
