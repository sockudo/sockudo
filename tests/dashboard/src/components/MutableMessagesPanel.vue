<script setup lang="ts">
import { computed, ref } from 'vue'
import { Edit3, Heart, History, ListTree, MessageSquarePlus, Plus, Send, Trash2 } from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { useHttpApi, type ApiResponse } from '../composables/useHttpApi'
import { usePusher } from '../composables/usePusher'

const store = useDashboardStore()
const { subscribe } = usePusher()

const channel = ref('chat:mutable')
const createEvent = ref('demo.message')
const createData = ref('{"text":"First mutable message","status":"draft"}')
const messageId = ref(`msg-${Date.now()}`)
const messageSerial = ref('')
const latestResult = ref<ApiResponse | null>(null)
const createResult = ref<ApiResponse | null>(null)
const versionsResult = ref<ApiResponse | null>(null)

const updateData = ref('{"text":"Edited mutable message","status":"published"}')
const updateOpId = ref('')
const updateDescription = ref('dashboard update')
const updateResult = ref<ApiResponse | null>(null)

const appendData = ref(' plus streamed text')
const appendOpId = ref('')
const appendStatus = ref<'none' | 'streaming' | 'complete' | 'cancelled'>('none')
const appendResult = ref<ApiResponse | null>(null)

const deleteDescription = ref('dashboard soft delete')
const deleteOpId = ref('')
const deleteResult = ref<ApiResponse | null>(null)

const versionLimit = ref('20')
const versionDirection = ref<'newest_first' | 'oldest_first'>('newest_first')
const versionCursor = ref('')

const annotationType = ref('reactions:distinct.v1')
const annotationName = ref('thumbsup')
const annotationClientId = ref('demo-user')
const annotationCount = ref('1')
const annotationSerial = ref('')
const annotationResult = ref<ApiResponse | null>(null)
const annotationsResult = ref<ApiResponse | null>(null)
const annotationDeleteResult = ref<ApiResponse | null>(null)

const isConnected = computed(() => store.connectionState === 'connected')
const subscribed = computed(() => store.channels.has(channel.value))

function fmtResponse(data: unknown) {
  return typeof data === 'string' ? data : JSON.stringify(data, null, 2)
}

function toWireData(raw: string) {
  const trimmed = raw.trim()
  if (!trimmed) return ''
  try {
    return JSON.stringify(JSON.parse(trimmed))
  } catch {
    return raw
  }
}

function extractSerial(res: ApiResponse | null) {
  const data = res?.data as any
  const ack = data?.channels?.[channel.value] ?? data?.channels?.[channel.value.trim()]
  return ack?.message_serial ?? ack?.messageSerial ?? ack?.message_id ?? ack?.messageId ?? ''
}

function applyCreateAck(res: ApiResponse | null) {
  const serial = extractSerial(res)
  if (serial) messageSerial.value = String(serial)
}

function versionParams() {
  const params: Record<string, string> = {
    limit: versionLimit.value || '20',
    direction: versionDirection.value,
  }
  if (versionCursor.value.trim()) params.cursor = versionCursor.value.trim()
  return params
}

function subscribeChannel() {
  if (!isConnected.value || subscribed.value || !channel.value.trim()) return
  subscribe(channel.value.trim())
}

async function createMessage() {
  const api = useHttpApi()
  createResult.value = await api.publishAdvancedEvent({
    name: createEvent.value.trim(),
    channel: channel.value.trim(),
    data: toWireData(createData.value),
    message_id: messageId.value.trim(),
    info: 'message_serial,history_serial,delivery_serial,version_serial',
    extras: {
      demo: {
        surface: 'mutable-messages',
        created_at_ms: Date.now(),
      },
    },
  })
  applyCreateAck(createResult.value)
}

async function fetchLatest() {
  if (!messageSerial.value.trim()) return
  latestResult.value = await useHttpApi().getMessage(channel.value.trim(), messageSerial.value.trim())
}

async function fetchVersions() {
  if (!messageSerial.value.trim()) return
  versionsResult.value = await useHttpApi().getMessageVersions(
    channel.value.trim(),
    messageSerial.value.trim(),
    versionParams(),
  )
}

function useVersionCursor() {
  const data = versionsResult.value?.data as any
  if (data?.next_cursor) versionCursor.value = data.next_cursor
}

async function updateMessage() {
  if (!messageSerial.value.trim()) return
  const body: Record<string, unknown> = {
    data: toWireData(updateData.value),
    description: updateDescription.value.trim() || undefined,
  }
  if (updateOpId.value.trim()) body.op_id = updateOpId.value.trim()
  updateResult.value = await useHttpApi().updateMessage(channel.value.trim(), messageSerial.value.trim(), body)
}

async function appendMessage() {
  if (!messageSerial.value.trim()) return
  const extras = appendStatus.value === 'none'
    ? undefined
    : {
        ai: {
          transport: {
            status: appendStatus.value,
          },
        },
      }
  const body: Record<string, unknown> = {
    data: appendData.value,
    description: `dashboard append ${appendStatus.value}`,
  }
  if (extras) body.extras = extras
  if (appendOpId.value.trim()) body.op_id = appendOpId.value.trim()
  appendResult.value = await useHttpApi().appendMessage(channel.value.trim(), messageSerial.value.trim(), body)
}

async function deleteMessage() {
  if (!messageSerial.value.trim()) return
  const body: Record<string, unknown> = {
    description: deleteDescription.value.trim() || undefined,
  }
  if (deleteOpId.value.trim()) body.op_id = deleteOpId.value.trim()
  deleteResult.value = await useHttpApi().deleteMessage(channel.value.trim(), messageSerial.value.trim(), body)
}

async function publishAnnotation() {
  if (!messageSerial.value.trim()) return
  const body: Record<string, unknown> = {
    type: annotationType.value.trim(),
    name: annotationName.value.trim() || undefined,
    clientId: annotationClientId.value.trim() || undefined,
    count: Number(annotationCount.value) || 1,
  }
  annotationResult.value = await useHttpApi().publishAnnotation(
    channel.value.trim(),
    messageSerial.value.trim(),
    body,
  )
  const serial = (annotationResult.value.data as any)?.annotationSerial
  if (serial) annotationSerial.value = serial
}

async function listAnnotations() {
  if (!messageSerial.value.trim()) return
  annotationsResult.value = await useHttpApi().getAnnotations(
    channel.value.trim(),
    messageSerial.value.trim(),
    {
      type: annotationType.value.trim(),
      limit: '50',
    },
  )
}

async function removeAnnotation() {
  if (!messageSerial.value.trim() || !annotationSerial.value.trim()) return
  annotationDeleteResult.value = await useHttpApi().deleteAnnotation(
    channel.value.trim(),
    messageSerial.value.trim(),
    annotationSerial.value.trim(),
  )
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">Versioned Messages and Annotations</h2>
      <p class="text-sm text-surface-400 mt-1">Mutable V2 messages backed by durable history and version store state.</p>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-3 gap-6">
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <MessageSquarePlus class="w-4 h-4 text-brand-400" /> Create
        </h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Channel</label>
          <input v-model="channel" class="input-field font-mono" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Event</label>
            <input v-model="createEvent" class="input-field font-mono" />
          </div>
          <div>
            <label class="text-xs text-surface-400 mb-1 block">Message ID</label>
            <input v-model="messageId" class="input-field font-mono text-xs" />
          </div>
        </div>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Data</label>
          <textarea v-model="createData" class="input-field font-mono text-xs h-24 resize-none" />
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button @click="subscribeChannel" :disabled="!isConnected || subscribed" class="btn-secondary btn-sm">
            {{ subscribed ? 'Subscribed' : 'Subscribe' }}
          </button>
          <button @click="createMessage" class="btn-primary btn-sm flex items-center justify-center gap-2">
            <Send class="w-4 h-4" /> Create
          </button>
        </div>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Message Serial</label>
          <input v-model="messageSerial" class="input-field font-mono text-xs" placeholder="returned by create" />
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Edit3 class="w-4 h-4 text-brand-400" /> Mutate
        </h3>
        <div>
          <label class="text-xs text-surface-400 mb-1 block">Update Data</label>
          <textarea v-model="updateData" class="input-field font-mono text-xs h-20 resize-none" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <input v-model="updateDescription" class="input-field font-mono text-xs" placeholder="description" />
          <input v-model="updateOpId" class="input-field font-mono text-xs" placeholder="op_id" />
        </div>
        <button @click="updateMessage" :disabled="!messageSerial.trim()" class="btn-primary btn-sm w-full flex items-center justify-center gap-2">
          <Edit3 class="w-4 h-4" /> Update
        </button>

        <div class="border-t border-surface-800 pt-3 space-y-2">
          <label class="text-xs text-surface-400 mb-1 block">Append Data</label>
          <textarea v-model="appendData" class="input-field font-mono text-xs h-16 resize-none" />
          <div class="grid grid-cols-2 gap-3">
            <select v-model="appendStatus" class="input-field font-mono text-xs">
              <option value="none">no AI status</option>
              <option value="streaming">streaming</option>
              <option value="complete">complete</option>
              <option value="cancelled">cancelled</option>
            </select>
            <input v-model="appendOpId" class="input-field font-mono text-xs" placeholder="op_id" />
          </div>
          <button @click="appendMessage" :disabled="!messageSerial.trim()" class="btn-primary btn-sm w-full flex items-center justify-center gap-2">
            <Plus class="w-4 h-4" /> Append
          </button>
        </div>

        <div class="border-t border-surface-800 pt-3 space-y-2">
          <div class="grid grid-cols-2 gap-3">
            <input v-model="deleteDescription" class="input-field font-mono text-xs" />
            <input v-model="deleteOpId" class="input-field font-mono text-xs" placeholder="op_id" />
          </div>
          <button @click="deleteMessage" :disabled="!messageSerial.trim()" class="btn-danger btn-sm w-full flex items-center justify-center gap-2">
            <Trash2 class="w-4 h-4" /> Delete
          </button>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <ListTree class="w-4 h-4 text-brand-400" /> Read
        </h3>
        <div class="grid grid-cols-2 gap-2">
          <button @click="fetchLatest" :disabled="!messageSerial.trim()" class="btn-secondary btn-sm">Latest</button>
          <button @click="fetchVersions" :disabled="!messageSerial.trim()" class="btn-secondary btn-sm flex items-center justify-center gap-2">
            <History class="w-4 h-4" /> Versions
          </button>
        </div>
        <div class="grid grid-cols-3 gap-2">
          <input v-model="versionLimit" class="input-field font-mono text-xs" placeholder="limit" />
          <select v-model="versionDirection" class="input-field font-mono text-xs col-span-2">
            <option value="newest_first">newest_first</option>
            <option value="oldest_first">oldest_first</option>
          </select>
        </div>
        <div class="flex gap-2">
          <input v-model="versionCursor" class="input-field font-mono text-xs" placeholder="cursor" />
          <button @click="useVersionCursor" class="btn-secondary btn-sm" :disabled="!(versionsResult?.data as any)?.next_cursor">Use</button>
        </div>

        <div class="border-t border-surface-800 pt-3 space-y-2">
          <h4 class="text-xs font-semibold text-surface-300 flex items-center gap-2">
            <Heart class="w-3.5 h-3.5 text-pink-400" /> Annotations
          </h4>
          <div class="grid grid-cols-2 gap-2">
            <input v-model="annotationType" class="input-field font-mono text-xs" />
            <input v-model="annotationName" class="input-field font-mono text-xs" />
            <input v-model="annotationClientId" class="input-field font-mono text-xs" />
            <input v-model="annotationCount" class="input-field font-mono text-xs" />
          </div>
          <input v-model="annotationSerial" class="input-field font-mono text-xs" placeholder="annotation serial" />
          <div class="grid grid-cols-3 gap-2">
            <button @click="publishAnnotation" :disabled="!messageSerial.trim()" class="btn-primary btn-sm">Add</button>
            <button @click="listAnnotations" :disabled="!messageSerial.trim()" class="btn-secondary btn-sm">List</button>
            <button @click="removeAnnotation" :disabled="!messageSerial.trim() || !annotationSerial.trim()" class="btn-danger btn-sm">Delete</button>
          </div>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-3 gap-6">
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Create / Latest</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[380px] text-surface-300">{{ fmtResponse({ create: createResult?.data, latest: latestResult?.data }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Mutations / Versions</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[380px] text-surface-300">{{ fmtResponse({ update: updateResult?.data, append: appendResult?.data, delete: deleteResult?.data, versions: versionsResult?.data }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Annotations</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[380px] text-surface-300">{{ fmtResponse({ publish: annotationResult?.data, list: annotationsResult?.data, delete: annotationDeleteResult?.data }) }}</pre>
      </div>
    </div>
  </div>
</template>
