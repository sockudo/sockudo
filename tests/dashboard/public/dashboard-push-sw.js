self.addEventListener('install', event => {
  self.skipWaiting()
})

self.addEventListener('activate', event => {
  event.waitUntil(self.clients.claim())
})

async function notifyClients(message) {
  const allClients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true })
  await Promise.all(allClients.map(client => client.postMessage(message)))
}

self.addEventListener('push', event => {
  let payload = {}
  if (event.data) {
    try {
      payload = event.data.json()
    } catch {
      payload = { notification: { title: 'Sockudo push', body: event.data.text() } }
    }
  }

  const notification = payload.notification || {}
  const data = payload.data || {}
  const title = notification.title || payload.title || 'Sockudo push'
  const body = notification.body || payload.body || 'Notification received'
  const options = {
    body,
    icon: notification.icon || '/favicon.svg',
    badge: notification.badge || '/favicon.svg',
    tag: notification.tag || data.collapseKey || 'sockudo-dashboard-push',
    data: {
      ...data,
      receivedAt: Date.now(),
    },
  }

  event.waitUntil(Promise.all([
    self.registration.showNotification(title, options),
    notifyClients({
      type: 'sockudo-push-received',
      title,
      body,
      payload,
      receivedAt: options.data.receivedAt,
    }),
  ]))
})

self.addEventListener('notificationclick', event => {
  event.notification.close()
  event.waitUntil((async () => {
    const allClients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true })
    const focused = allClients.find(client => 'focus' in client)
    if (focused) {
      await focused.focus()
      return
    }
    await self.clients.openWindow('/')
  })())
})
