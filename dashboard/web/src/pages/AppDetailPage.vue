<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import { ArrowLeft, Plus, RefreshCw, Trash2 } from "lucide-vue-next";
import { api, type App, type Webhook } from "@/api/client";

const route = useRoute();
const router = useRouter();
const appId = computed(() => route.params.id as string);

const app = ref<App | null>(null);
const webhooks = ref<Webhook[]>([]);
const loading = ref(true);
const saving = ref(false);
const error = ref<string | null>(null);
const revealSecret = ref(false);
const showWebhookForm = ref(false);
const editingWebhookIndex = ref<number | null>(null);

const webhookForm = ref<Webhook>({
  url: "",
  event_types: ["channel_occupied"],
});

const eventTypeOptions = [
  "channel_occupied",
  "channel_vacated",
  "subscription_count",
  "member_added",
  "member_removed",
  "member_updated",
  "client_event",
  "ai_turn_started",
  "ai_turn_ended",
  "message_version_created",
  "annotation_created",
  "annotation_deleted",
];

onMounted(load);
watch(appId, load);

async function load() {
  loading.value = true;
  error.value = null;
  try {
    const [appData, hooks] = await Promise.all([
      api.getApp(appId.value, revealSecret.value),
      api.listWebhooks(appId.value),
    ]);
    app.value = appData;
    webhooks.value = hooks;
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Failed to load app";
  } finally {
    loading.value = false;
  }
}

async function saveApp() {
  if (!app.value) return;
  saving.value = true;
  try {
    app.value = await api.updateApp(appId.value, {
      key: app.value.key,
      enabled: app.value.enabled,
      policy: app.value.policy,
    });
    error.value = null;
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Save failed";
  } finally {
    saving.value = false;
  }
}

async function rotateSecret() {
  if (!confirm("Rotate app secret? Existing signed API calls will fail until updated.")) return;
  try {
    revealSecret.value = true;
    app.value = await api.rotateSecret(appId.value);
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Rotate failed";
  }
}

function resetWebhookForm() {
  webhookForm.value = { url: "", event_types: ["channel_occupied"] };
  editingWebhookIndex.value = null;
  showWebhookForm.value = false;
}

function editWebhook(index: number) {
  editingWebhookIndex.value = index;
  webhookForm.value = structuredClone(webhooks.value[index]);
  showWebhookForm.value = true;
}

async function saveWebhook() {
  try {
    if (editingWebhookIndex.value !== null) {
      await api.updateWebhook(appId.value, editingWebhookIndex.value, webhookForm.value);
    } else {
      await api.createWebhook(appId.value, webhookForm.value);
    }
    webhooks.value = await api.listWebhooks(appId.value);
    resetWebhookForm();
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Webhook save failed";
  }
}

async function deleteWebhook(index: number) {
  if (!confirm("Delete this webhook?")) return;
  try {
    await api.deleteWebhook(appId.value, index);
    webhooks.value = await api.listWebhooks(appId.value);
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Delete failed";
  }
}

function toggleEventType(type: string) {
  const set = new Set(webhookForm.value.event_types);
  if (set.has(type)) set.delete(type);
  else set.add(type);
  webhookForm.value.event_types = [...set];
}
</script>

<template>
  <div>
    <button
      class="btn-secondary btn-sm flex items-center gap-1 mb-4"
      @click="router.push({ name: 'apps' })"
    >
      <ArrowLeft class="w-3 h-3" />
      Back to apps
    </button>

    <div v-if="loading" class="text-surface-400">Loading...</div>

    <template v-else-if="app">
      <div class="flex items-start justify-between mb-6">
        <div>
          <h1 class="text-2xl font-semibold font-mono text-brand-300">{{ app.id }}</h1>
          <p class="text-sm text-surface-400 mt-1">Edit app configuration and webhooks</p>
        </div>
        <button class="btn-primary" :disabled="saving" @click="saveApp">
          {{ saving ? "Saving..." : "Save changes" }}
        </button>
      </div>

      <p v-if="error" class="text-red-400 text-sm mb-4">{{ error }}</p>

      <div class="grid lg:grid-cols-2 gap-6 mb-8">
        <div class="panel p-6 space-y-4">
          <h2 class="font-medium text-surface-200">Credentials</h2>
          <div>
            <label class="section-title">App Key</label>
            <input v-model="app.key" class="input-field font-mono" />
          </div>
          <div>
            <label class="section-title">Secret</label>
            <div class="flex gap-2">
              <input :value="app.secret" class="input-field font-mono flex-1" readonly />
              <button class="btn-secondary btn-sm" @click="revealSecret = !revealSecret; load()">
                {{ revealSecret ? "Hide" : "Reveal" }}
              </button>
              <button class="btn-secondary btn-sm flex items-center gap-1" @click="rotateSecret">
                <RefreshCw class="w-3 h-3" />
                Rotate
              </button>
            </div>
          </div>
          <label class="flex items-center gap-2 text-sm text-surface-300">
            <input v-model="app.enabled" type="checkbox" class="rounded" />
            Enabled
          </label>
        </div>

        <div class="panel p-6 space-y-4">
          <h2 class="font-medium text-surface-200">Limits</h2>
          <div class="grid grid-cols-2 gap-4">
            <div>
              <label class="section-title">Max connections</label>
              <input
                v-model.number="app.policy.limits.max_connections"
                type="number"
                class="input-field"
              />
            </div>
            <div>
              <label class="section-title">Client events / sec</label>
              <input
                v-model.number="app.policy.limits.max_client_events_per_second"
                type="number"
                class="input-field"
              />
            </div>
            <div>
              <label class="section-title">Backend events / sec</label>
              <input
                v-model.number="app.policy.limits.max_backend_events_per_second"
                type="number"
                class="input-field"
              />
            </div>
            <div>
              <label class="section-title">Read requests / sec</label>
              <input
                v-model.number="app.policy.limits.max_read_requests_per_second"
                type="number"
                class="input-field"
              />
            </div>
          </div>
          <label class="flex items-center gap-2 text-sm text-surface-300">
            <input
              v-model="app.policy.features.enable_client_messages"
              type="checkbox"
              class="rounded"
            />
            Enable client messages
          </label>
          <label class="flex items-center gap-2 text-sm text-surface-300">
            <input
              v-model="app.policy.features.enable_user_authentication"
              type="checkbox"
              class="rounded"
            />
            Enable user authentication
          </label>
          <div>
            <label class="section-title">Allowed origins (comma-separated)</label>
            <input
              :value="(app.policy.channels.allowed_origins ?? []).join(', ')"
              class="input-field"
              @input="
                app.policy.channels.allowed_origins = ($event.target as HTMLInputElement).value
                  .split(',')
                  .map((s) => s.trim())
                  .filter(Boolean)
              "
            />
          </div>
        </div>
      </div>

      <div class="panel p-6">
        <div class="flex items-center justify-between mb-4">
          <h2 class="font-medium text-surface-200">Webhooks</h2>
          <button
            class="btn-primary btn-sm flex items-center gap-1"
            @click="resetWebhookForm(); showWebhookForm = true"
          >
            <Plus class="w-3 h-3" />
            Add webhook
          </button>
        </div>

        <div v-if="showWebhookForm" class="border border-surface-700 rounded-lg p-4 mb-4 space-y-4">
          <div>
            <label class="section-title">URL</label>
            <input v-model="webhookForm.url" class="input-field" placeholder="https://..." />
          </div>
          <div>
            <label class="section-title">Event types</label>
            <div class="flex flex-wrap gap-2">
              <button
                v-for="type in eventTypeOptions"
                :key="type"
                type="button"
                class="text-xs px-2 py-1 rounded border"
                :class="
                  webhookForm.event_types.includes(type)
                    ? 'border-brand-500 bg-brand-500/15 text-brand-300'
                    : 'border-surface-600 text-surface-400'
                "
                @click="toggleEventType(type)"
              >
                {{ type }}
              </button>
            </div>
          </div>
          <div>
            <label class="section-title">Channel prefix filter (optional)</label>
            <input
              :value="webhookForm.filter?.channel_prefix ?? ''"
              class="input-field"
              @input="
                webhookForm.filter = {
                  ...webhookForm.filter,
                  channel_prefix: ($event.target as HTMLInputElement).value || undefined,
                }
              "
            />
          </div>
          <div class="flex gap-2">
            <button class="btn-primary btn-sm" @click="saveWebhook">Save webhook</button>
            <button class="btn-secondary btn-sm" @click="resetWebhookForm">Cancel</button>
          </div>
        </div>

        <div v-if="webhooks.length === 0" class="text-surface-500 text-sm">
          No webhooks configured.
        </div>
        <div v-else class="space-y-3">
          <div
            v-for="(hook, index) in webhooks"
            :key="index"
            class="border border-surface-700 rounded-lg p-4 flex justify-between gap-4"
          >
            <div class="min-w-0">
              <p class="font-mono text-sm text-brand-300 truncate">{{ hook.url }}</p>
              <p class="text-xs text-surface-500 mt-1">
                {{ hook.event_types.join(", ") }}
              </p>
            </div>
            <div class="flex gap-2 shrink-0">
              <button class="btn-secondary btn-sm" @click="editWebhook(index)">Edit</button>
              <button class="btn-danger btn-sm" @click="deleteWebhook(index)">
                <Trash2 class="w-3 h-3" />
              </button>
            </div>
          </div>
        </div>
      </div>
    </template>
  </div>
</template>
