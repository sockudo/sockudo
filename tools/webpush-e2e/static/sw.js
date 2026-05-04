self.addEventListener("push", (event) => {
  const payload = readPayload(event);
  const notification = payload.notification || {};
  const title = notification.title || "Sockudo Web Push";
  const body = notification.body || "Push received";

  event.waitUntil(
    Promise.all([
      self.registration.showNotification(title, {
        body,
        data: payload.data || {},
        tag: payload.headers?.topic || `sockudo-webpush-e2e-${Date.now()}`,
        renotify: true,
      }),
      notifyClients(payload),
    ]),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
      if (clients[0]) return clients[0].focus();
      return self.clients.openWindow("/");
    }),
  );
});

function readPayload(event) {
  if (!event.data) return {};
  try {
    return event.data.json();
  } catch {
    return { notification: { title: "Sockudo Web Push", body: event.data.text() } };
  }
}

function notifyClients(payload) {
  return self.clients
    .matchAll({ type: "window", includeUncontrolled: true })
    .then((clients) => {
      for (const client of clients) {
        client.postMessage({ type: "push-received", payload });
      }
    });
}
