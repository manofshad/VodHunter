self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => event.waitUntil(self.clients.claim()));

self.addEventListener("push", (event) => {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch (_error) {
    payload = { body: event.data ? event.data.text() : "Your VodHunter search is ready." };
  }

  const title = payload.title || "VodHunter search finished";
  const options = {
    body: payload.body || "Tap to view your result.",
    icon: "/icons/vodhunter-192.png",
    badge: "/icons/vodhunter-192.png",
    tag: payload.tag || "vodhunter-search",
    data: { url: payload.url || "/" },
  };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const destination = new URL(event.notification.data?.url || "/", self.location.origin).href;
  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clients) => {
      for (const client of clients) {
        if ("navigate" in client) {
          return client.navigate(destination).then(() => client.focus());
        }
      }
      return self.clients.openWindow(destination);
    }),
  );
});
