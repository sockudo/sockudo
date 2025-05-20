require("dotenv").config();
const Pusher = require("pusher-js");
const fetch = (...args) =>
  import("node-fetch").then(({ default: fetch }) => fetch(...args));

const cfg = {
  key: process.env.PUSHER_APP_KEY,
  cluster: "mt1",
  wsHost: process.env.PUSHER_HOST,
  wsPort: Number(process.env.PUSHER_PORT),
  forceTLS: process.env.PUSHER_USE_TLS === "true",
  enabledTransports: ["ws", "wss"],
};

const pusher = new Pusher(cfg.key, cfg);

const channel = pusher.subscribe("my-channel");

channel.bind("my-event", function (data) {
  const receivedAt = Date.now();
  const sentAt = data.sentAt ? Number(data.sentAt) : null;
  if (sentAt) {
    const latency = receivedAt - sentAt;
    console.log(`Received: ${data.message}, Latency: ${latency} ms`);
  } else {
    console.log("Received event:", data);
  }
});

setTimeout(async () => {
  for (let index = 0; index < 10; index++) {
    const sentAt = Date.now();
    const body = {
      name: "my-event",
      channel: "my-channel",
      data: JSON.stringify({
        message: "Latency test --- id: " + index,
        sentAt,
      }),
    };

    try {
      const res = await fetch("http://localhost:6001/apps/app1/events", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
    } catch (err) {
      console.error("Error sending event:", err);
    }
  }
}, 1000);
