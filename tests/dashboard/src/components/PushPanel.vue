<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { Bell, ClipboardList, KeyRound, Radio, Send, Smartphone, Trash2 } from 'lucide-vue-next'
import { useDashboardStore } from '../stores/dashboard'
import { useHttpApi, type ApiResponse } from '../composables/useHttpApi'

const store = useDashboardStore()

const credentialProvider = ref<'webpush' | 'apns'>('webpush')
const credentialId = ref('webpush-demo')
const vapidPublicKey = ref('')
const vapidPrivateKey = ref('')
const apnsTeamId = ref('')
const apnsKeyId = ref('RCXB2UP956')
const apnsPrivateKey = ref('')
const credentialResult = ref<ApiResponse | null>(null)
const credentialListResult = ref<ApiResponse | null>(null)

const templateId = ref('demo-alert')
const templateTitle = ref('Sockudo demo')
const templateBody = ref('Push from the dashboard')
const templateResult = ref<ApiResponse | null>(null)
const templateListResult = ref<ApiResponse | null>(null)

const deviceId = ref('browser-device-1')
const clientId = ref('demo-user')
const deviceProvider = ref<'web' | 'apns'>('web')
const providerToken = ref('https://push.example.test/subscription/browser-device-1')
const apnsDeviceToken = ref('')
const p256dh = ref('demo-p256dh')
const auth = ref('demo-auth')
const deviceIdentityToken = ref('')
const tokenHash = ref('')
const deviceResult = ref<ApiResponse | null>(null)
const deviceListResult = ref<ApiResponse | null>(null)
const browserPushStatus = ref('Idle')
const notificationPermission = ref('unknown')
const browserReceipts = ref<Array<{ title: string; body: string; receivedAt: number; payload: unknown }>>([])

const subscriptionChannel = ref('orders')
const subscriptionResult = ref<ApiResponse | null>(null)
const subscriptionListResult = ref<ApiResponse | null>(null)
const subscriptionChannelsResult = ref<ApiResponse | null>(null)

const publishId = ref(`push-${Date.now()}`)
const publishTitle = ref('Order updated')
const publishBody = ref('Order ord_123 is packed')
const publishSync = ref(false)
const publishTarget = ref<'channel' | 'client' | 'device'>('channel')
const publishResult = ref<ApiResponse | null>(null)
const publishStatusResult = ref<ApiResponse | null>(null)
const deliveryResult = ref<ApiResponse | null>(null)

function fmtResponse(data: unknown) {
  return typeof data === 'string' ? data : JSON.stringify(data, null, 2)
}

function api() {
  return useHttpApi()
}

function formatTime(value: number) {
  return new Date(value).toLocaleTimeString()
}

function recordBrowserReceipt(receipt: { title?: string; body?: string; receivedAt?: number; payload?: unknown }) {
  browserReceipts.value.unshift({
    title: receipt.title || 'Sockudo push',
    body: receipt.body || 'Notification received',
    receivedAt: receipt.receivedAt || Date.now(),
    payload: receipt.payload ?? {},
  })
  browserReceipts.value = browserReceipts.value.slice(0, 5)
  browserPushStatus.value = 'Browser push received'
}

function refreshNotificationPermission() {
  notificationPermission.value = 'Notification' in window ? Notification.permission : 'missing'
}

function onServiceWorkerMessage(event: MessageEvent) {
  if (event.data?.type !== 'sockudo-push-received') return
  recordBrowserReceipt(event.data)
}

onMounted(() => {
  refreshNotificationPermission()
  navigator.serviceWorker?.addEventListener('message', onServiceWorkerMessage)
})

onUnmounted(() => {
  navigator.serviceWorker?.removeEventListener('message', onServiceWorkerMessage)
})

function base64UrlToUint8Array(value: string) {
  const padding = '='.repeat((4 - value.length % 4) % 4)
  const base64 = `${value}${padding}`.replace(/-/g, '+').replace(/_/g, '/')
  const raw = atob(base64)
  return Uint8Array.from(raw, char => char.charCodeAt(0))
}

function arrayBufferToBase64Url(buffer?: ArrayBuffer | null) {
  if (!buffer) return ''
  const bytes = new Uint8Array(buffer)
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
}

async function prepareBrowserPush() {
  browserPushStatus.value = 'Preparing browser push'
  try {
    if (!('serviceWorker' in navigator) || !('PushManager' in window) || !('Notification' in window)) {
      browserPushStatus.value = 'This browser does not expose Web Push APIs'
      return
    }

    const publicKey = vapidPublicKey.value.trim()
    if (!publicKey) {
      browserPushStatus.value = 'Paste the VAPID public key first'
      return
    }

    const permission = Notification.permission === 'granted'
      ? 'granted'
      : await Notification.requestPermission()
    refreshNotificationPermission()
    if (permission !== 'granted') {
      browserPushStatus.value = `Notification permission is ${permission}`
      return
    }

    const registration = await navigator.serviceWorker.register('/sockudo-push-sw.js')
    await navigator.serviceWorker.ready

    const existing = await registration.pushManager.getSubscription()
    if (existing) await existing.unsubscribe()

    const subscription = await registration.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: base64UrlToUint8Array(publicKey),
    })
    const keys = subscription.toJSON().keys ?? {}

    credentialProvider.value = 'webpush'
    deviceProvider.value = 'web'
    providerToken.value = subscription.endpoint
    p256dh.value = keys.p256dh ?? arrayBufferToBase64Url(subscription.getKey('p256dh'))
    auth.value = keys.auth ?? arrayBufferToBase64Url(subscription.getKey('auth'))
    deviceId.value = `browser-${new URL(subscription.endpoint).hostname.replace(/[^a-z0-9]/gi, '-').toLowerCase()}`
    tokenHash.value = ''
    deviceIdentityToken.value = ''
    browserPushStatus.value = 'Browser subscription ready'
  } catch (err: any) {
    browserPushStatus.value = err?.message ?? 'Browser push setup failed'
  }
}

async function showLocalNotification() {
  try {
    refreshNotificationPermission()
    const registration = await navigator.serviceWorker.ready
    await registration.showNotification('Sockudo browser push ready', {
      body: 'Local notification path is available.',
      tag: 'sockudo-local-test',
      data: { source: 'tests/dashboard' },
    })
    browserPushStatus.value = 'Local notification shown'
  } catch (err: any) {
    browserPushStatus.value = err?.message ?? 'Local notification failed'
  }
}

async function showDirectNotification() {
  try {
    if (!('Notification' in window)) {
      browserPushStatus.value = 'Notification API is missing'
      return
    }
    const permission = Notification.permission === 'granted'
      ? 'granted'
      : await Notification.requestPermission()
    refreshNotificationPermission()
    if (permission !== 'granted') {
      browserPushStatus.value = `Notification permission is ${permission}`
      return
    }
    new Notification('Sockudo direct browser test', {
      body: 'This notification came from the page Notification API.',
      tag: `sockudo-direct-${Date.now()}`,
    })
    browserPushStatus.value = 'Direct notification requested'
  } catch (err: any) {
    browserPushStatus.value = err?.message ?? 'Direct notification failed'
  }
}

async function putCredential() {
  const id = credentialId.value.trim()
  if (credentialProvider.value === 'apns') {
    credentialResult.value = await api().putPushCredential('apns', {
      credentialId: id,
      version: 1,
      teamId: apnsTeamId.value.trim(),
      keyId: apnsKeyId.value.trim(),
      privateKey: apnsPrivateKey.value,
    })
    return
  }

  credentialResult.value = await api().putPushCredential('webpush', {
    credentialId: id,
    version: 1,
    publicKey: vapidPublicKey.value,
    privateKey: vapidPrivateKey.value,
  })
}

async function listCredentials() {
  credentialListResult.value = await api().listPushCredentials({ limit: '50' })
}

async function putTemplate() {
  templateResult.value = await api().putPushTemplate({
    appId: store.config.appId,
    templateId: templateId.value.trim(),
    defaultLocale: 'en',
    locales: {
      en: {
        title: templateTitle.value,
        body: templateBody.value,
      },
    },
    providerOverrides: {},
  })
}

async function listTemplates() {
  templateListResult.value = await api().listPushTemplates({ limit: '50' })
}

async function registerDevice() {
  const recipient = deviceProvider.value === 'apns'
    ? {
        transportType: 'apns',
        deviceToken: apnsDeviceToken.value.trim(),
      }
    : {
        transportType: 'web',
        endpoint: providerToken.value,
        p256dh: p256dh.value,
        auth: auth.value,
      }

  deviceResult.value = await api().registerPushDevice(
    {
      appId: store.config.appId,
      id: deviceId.value.trim(),
      clientId: clientId.value.trim(),
      formFactor: deviceProvider.value === 'apns' ? 'phone' : 'desktop',
      platform: deviceProvider.value === 'apns' ? 'ios' : 'browser',
      metadata: {
        source: 'tests/dashboard',
        provider: deviceProvider.value,
      },
      deviceSecret: 'dashboard-placeholder',
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC',
      locale: navigator.language || 'en-US',
      lastActiveAtMs: Date.now(),
      push: {
        recipient,
        state: 'ACTIVE',
        failure_count: 0,
      },
    },
    { rotateToken: true },
  )
  const data = deviceResult.value.data as any
  if (data?.deviceIdentityToken) deviceIdentityToken.value = data.deviceIdentityToken
  if (data?.tokenHash) tokenHash.value = data.tokenHash
}

async function listDevices() {
  deviceListResult.value = await api().listPushDevices({ limit: '50' })
}

async function deleteDevice() {
  if (!deviceId.value.trim()) return
  deviceResult.value = await api().deletePushDevice(deviceId.value.trim())
}

async function upsertSubscription() {
  subscriptionResult.value = await api().upsertPushSubscription({
    appId: store.config.appId,
    channel: subscriptionChannel.value.trim(),
    deviceId: deviceId.value.trim(),
    clientId: clientId.value.trim(),
    provider: deviceProvider.value === 'apns' ? 'apns' : 'webPush',
    tokenHash: tokenHash.value.trim(),
    credentialVersion: 1,
  })
}

async function listSubscriptions() {
  subscriptionListResult.value = await api().listPushSubscriptions({ limit: '50' })
}

async function listSubscriptionChannels() {
  subscriptionChannelsResult.value = await api().listPushSubscriptionChannels({ limit: '50' })
}

async function deleteSubscriptions() {
  const params = publishTarget.value === 'device'
    ? { deviceId: deviceId.value.trim() }
    : { channel: subscriptionChannel.value.trim() }
  subscriptionResult.value = await api().deletePushSubscriptions(params)
}

function publishRecipients() {
  if (publishTarget.value === 'client') return [{ type: 'client', client_id: clientId.value.trim() }]
  if (publishTarget.value === 'device') return [{ type: 'device', device_id: deviceId.value.trim() }]
  return [{ type: 'channel', channel: subscriptionChannel.value.trim() }]
}

async function publishPush() {
  publishResult.value = await api().publishPush(
    {
      publishId: publishId.value.trim(),
      recipients: publishRecipients(),
      payload: {
        title: publishTitle.value,
        body: publishBody.value,
        templateData: {
          client_id: clientId.value,
          channel: subscriptionChannel.value,
        },
        collapseKey: 'dashboard-demo',
      },
      sync: publishSync.value,
    },
    publishSync.value ? 'sync' : undefined,
  )
  const data = publishResult.value.data as any
  if (data?.publishId) publishId.value = data.publishId
  if (data?.publish_id) publishId.value = data.publish_id
}

async function getStatus() {
  if (!publishId.value.trim()) return
  publishStatusResult.value = await api().getPushPublishStatus(publishId.value.trim())
}

async function postDeliveryStatus() {
  const eventId = `delivery-${Date.now()}`
  deliveryResult.value = await api().postPushDeliveryStatus({
    appId: store.config.appId,
    publishId: publishId.value.trim(),
    eventId,
    occurredAtMs: Date.now(),
    result: {
      appId: store.config.appId,
      publishId: publishId.value.trim(),
      provider: 'webPush',
      batchId: 'dashboard-batch',
      deviceId: deviceId.value.trim(),
      outcome: 'Accepted',
      providerMessageId: `provider-${eventId}`,
      attempt: 1,
    },
  })
}
</script>

<template>
  <div class="space-y-6 animate-fade-in">
    <div>
      <h2 class="text-lg font-bold text-surface-50">Push Notifications</h2>
      <p class="text-sm text-surface-400 mt-1">Credential, device, subscription, publish, status, and feedback surfaces.</p>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-4 gap-6">
      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <KeyRound class="w-4 h-4 text-brand-400" /> Credentials
        </h3>
        <select v-model="credentialProvider" class="input-field font-mono text-xs">
          <option value="webpush">webpush</option>
          <option value="apns">apns</option>
        </select>
        <input v-model="credentialId" class="input-field font-mono text-xs" placeholder="credentialId" />
        <template v-if="credentialProvider === 'webpush'">
          <input v-model="vapidPublicKey" class="input-field font-mono text-xs" placeholder="VAPID public key" />
          <input v-model="vapidPrivateKey" class="input-field font-mono text-xs" placeholder="VAPID private key" type="password" />
        </template>
        <template v-else>
          <div class="grid grid-cols-2 gap-2">
            <input v-model="apnsTeamId" class="input-field font-mono text-xs" placeholder="teamId" />
            <input v-model="apnsKeyId" class="input-field font-mono text-xs" placeholder="keyId" />
          </div>
          <textarea v-model="apnsPrivateKey" class="input-field font-mono text-xs h-20 resize-none" placeholder="APNs .p8 private key content" />
        </template>
        <div class="grid grid-cols-2 gap-2">
          <button @click="putCredential" class="btn-primary btn-sm">Save</button>
          <button @click="listCredentials" class="btn-secondary btn-sm">List</button>
        </div>
        <div class="border-t border-surface-800 pt-3 space-y-2">
          <h4 class="text-xs font-semibold text-surface-300 flex items-center gap-2">
            <ClipboardList class="w-3.5 h-3.5 text-brand-400" /> Template
          </h4>
          <input v-model="templateId" class="input-field font-mono text-xs" />
          <input v-model="templateTitle" class="input-field text-xs" />
          <input v-model="templateBody" class="input-field text-xs" />
          <div class="grid grid-cols-2 gap-2">
            <button @click="putTemplate" class="btn-primary btn-sm">Save</button>
            <button @click="listTemplates" class="btn-secondary btn-sm">List</button>
          </div>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Smartphone class="w-4 h-4 text-brand-400" /> Device
        </h3>
        <div class="grid grid-cols-2 gap-2">
          <input v-model="deviceId" class="input-field font-mono text-xs" placeholder="device id" />
          <input v-model="clientId" class="input-field font-mono text-xs" placeholder="client id" />
        </div>
        <select v-model="deviceProvider" class="input-field font-mono text-xs">
          <option value="web">web push</option>
          <option value="apns">apns</option>
        </select>
        <template v-if="deviceProvider === 'apns'">
          <input v-model="apnsDeviceToken" class="input-field font-mono text-xs" placeholder="APNs device token" />
        </template>
        <template v-else>
          <input v-model="providerToken" class="input-field font-mono text-xs" placeholder="web endpoint" />
          <div class="grid grid-cols-2 gap-2">
            <input v-model="p256dh" class="input-field font-mono text-xs" placeholder="p256dh" />
            <input v-model="auth" class="input-field font-mono text-xs" placeholder="auth" />
          </div>
          <div class="grid grid-cols-3 gap-2">
            <button @click="prepareBrowserPush" class="btn-secondary btn-sm">Browser</button>
            <button @click="showLocalNotification" class="btn-secondary btn-sm">Local Test</button>
            <button @click="showDirectNotification" class="btn-secondary btn-sm">Direct</button>
          </div>
          <div class="grid grid-cols-2 gap-2 text-xs text-surface-400">
            <p class="break-words">{{ browserPushStatus }}</p>
            <p>permission: <span class="font-mono text-surface-300">{{ notificationPermission }}</span></p>
          </div>
          <div class="rounded-lg border border-surface-700/30 bg-surface-800/40 p-3 text-xs text-surface-400 space-y-2">
            <div class="flex items-center justify-between gap-2">
              <span class="font-semibold text-surface-300">Browser receipts</span>
              <span class="font-mono">{{ browserReceipts.length }}</span>
            </div>
            <p v-if="browserReceipts.length === 0">No pushed payload received by this tab yet.</p>
            <div v-for="receipt in browserReceipts" :key="receipt.receivedAt" class="rounded border border-surface-700/40 bg-surface-900/50 p-2">
              <p class="font-medium text-surface-200">{{ receipt.title }}</p>
              <p>{{ receipt.body }}</p>
              <p class="mt-1 font-mono text-[11px] text-surface-500">{{ formatTime(receipt.receivedAt) }}</p>
            </div>
          </div>
        </template>
        <div class="grid grid-cols-3 gap-2">
          <button @click="registerDevice" class="btn-primary btn-sm">Register</button>
          <button @click="listDevices" class="btn-secondary btn-sm">List</button>
          <button @click="deleteDevice" class="btn-danger btn-sm">
            <Trash2 class="w-3.5 h-3.5 mx-auto" />
          </button>
        </div>
        <input v-model="tokenHash" class="input-field font-mono text-xs" placeholder="token_hash" />
        <input v-model="deviceIdentityToken" class="input-field font-mono text-xs" placeholder="identity token" />
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Radio class="w-4 h-4 text-brand-400" /> Subscription
        </h3>
        <input v-model="subscriptionChannel" class="input-field font-mono text-xs" placeholder="channel" />
        <div class="grid grid-cols-2 gap-2">
          <button @click="upsertSubscription" :disabled="!tokenHash.trim()" class="btn-primary btn-sm">Upsert</button>
          <button @click="listSubscriptions" class="btn-secondary btn-sm">List</button>
        </div>
        <div class="grid grid-cols-2 gap-2">
          <button @click="listSubscriptionChannels" class="btn-secondary btn-sm">Channels</button>
          <button @click="deleteSubscriptions" class="btn-danger btn-sm">Delete</button>
        </div>
        <div class="rounded-lg border border-surface-700/30 bg-surface-800/40 p-3 text-xs text-surface-400">
          <p class="font-mono text-surface-300">{{ subscriptionChannel }}</p>
          <p class="mt-1">device: <span class="font-mono">{{ deviceId }}</span></p>
          <p>client: <span class="font-mono">{{ clientId }}</span></p>
        </div>
      </div>

      <div class="panel p-5 space-y-4">
        <h3 class="text-sm font-semibold text-surface-200 flex items-center gap-2">
          <Bell class="w-4 h-4 text-brand-400" /> Publish
        </h3>
        <input v-model="publishId" class="input-field font-mono text-xs" placeholder="publishId" />
        <div class="grid grid-cols-2 gap-2">
          <input v-model="publishTitle" class="input-field text-xs" />
          <select v-model="publishTarget" class="input-field font-mono text-xs">
            <option value="channel">channel</option>
            <option value="client">client</option>
            <option value="device">device</option>
          </select>
        </div>
        <textarea v-model="publishBody" class="input-field text-xs h-16 resize-none" />
        <label class="flex items-center gap-2 text-xs text-surface-300">
          <input v-model="publishSync" type="checkbox" class="accent-brand-500" />
          Sync admission
        </label>
        <div class="grid grid-cols-3 gap-2">
          <button @click="publishPush" class="btn-primary btn-sm flex items-center justify-center gap-1">
            <Send class="w-3.5 h-3.5" /> Send
          </button>
          <button @click="getStatus" class="btn-secondary btn-sm">Status</button>
          <button @click="postDeliveryStatus" class="btn-secondary btn-sm">Feedback</button>
        </div>
      </div>
    </div>

    <div class="grid grid-cols-1 xl:grid-cols-4 gap-6">
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Credentials / Templates</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[380px] text-surface-300">{{ fmtResponse({ credential: credentialResult?.data, credentials: credentialListResult?.data, template: templateResult?.data, templates: templateListResult?.data }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Devices</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[380px] text-surface-300">{{ fmtResponse({ device: deviceResult?.data, devices: deviceListResult?.data }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Subscriptions</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[380px] text-surface-300">{{ fmtResponse({ subscription: subscriptionResult?.data, list: subscriptionListResult?.data, channels: subscriptionChannelsResult?.data }) }}</pre>
      </div>
      <div class="panel p-5 space-y-3">
        <h3 class="text-sm font-semibold text-surface-200">Publish / Status</h3>
        <pre class="text-xs font-mono bg-surface-900/80 rounded-lg p-3 overflow-auto max-h-[380px] text-surface-300">{{ fmtResponse({ publish: publishResult?.data, status: publishStatusResult?.data, delivery: deliveryResult?.data }) }}</pre>
      </div>
    </div>
  </div>
</template>
