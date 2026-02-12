// Generic raw WebSocket inspector for delta-compression traffic.
// No hardcoded credentials; everything comes from env vars.

import WebSocket from "ws";

const CONFIG = {
  host: process.env.PUSHER_HOST || "127.0.0.1",
  port: Number(process.env.PUSHER_PORT || 6001),
  appKey: process.env.PUSHER_APP_KEY || "app-key",
  channel: process.env.TEST_CHANNEL || "benchmark",
  protocolVersion: process.env.PUSHER_PROTOCOL || "7",
  clientVersion: process.env.PUSHER_CLIENT_VERSION || "8.4.0",
};

const wsUrl = `ws://${CONFIG.host}:${CONFIG.port}/app/${CONFIG.appKey}?protocol=${CONFIG.protocolVersion}&client=js&version=${CONFIG.clientVersion}&flash=false`;
const ws = new WebSocket(wsUrl);

let total = 0;
let deltas = 0;
let full = 0;

function printSummary() {
  const ratio = total > 0 ? ((deltas / total) * 100).toFixed(1) : "0.0";
  console.log(`[delta-debug] total=${total} delta=${deltas} full=${full} delta_ratio=${ratio}%`);
}

ws.on("open", () => {
  console.log("[delta-debug] connected");
});

ws.on("message", (raw) => {
  const msg = JSON.parse(raw.toString());

  if (msg.event === "pusher:connection_established") {
    ws.send(
      JSON.stringify({
        event: "pusher:enable_delta_compression",
        data: JSON.stringify({ algorithms: ["fossil", "xdelta3"] }),
      }),
    );

    ws.send(
      JSON.stringify({
        event: "pusher:subscribe",
        data: { channel: CONFIG.channel },
      }),
    );

    return;
  }

  if (msg.channel !== CONFIG.channel) {
    return;
  }

  total += 1;
  if (msg.event === "pusher:delta") {
    deltas += 1;
  } else {
    full += 1;
  }

  if (total % 25 === 0) {
    printSummary();
  }
});

ws.on("close", () => {
  console.log("[delta-debug] closed");
  printSummary();
});

ws.on("error", (err) => {
  console.error("[delta-debug] ws error:", err.message);
});

process.on("SIGINT", () => {
  ws.close();
  process.exit(0);
});
