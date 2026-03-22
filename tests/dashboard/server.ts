/**
 * Sockudo Dashboard Auth Server
 *
 * Lightweight Bun server that provides:
 *   - POST /pusher/auth          → channel authorization (private, presence, encrypted)
 *   - POST /pusher/user-auth     → user authentication (signin)
 *
 * Uses the `pusher` Node SDK to generate correct HMAC signatures
 * that Sockudo will accept.
 *
 * Reads config from environment or falls back to defaults that
 * match the dashboard's default connection config.
 */

import Pusher from "pusher";

const APP_ID = process.env.SOCKUDO_APP_ID ?? "app-id";
const APP_KEY = process.env.SOCKUDO_APP_KEY ?? "app-key";
const APP_SECRET = process.env.SOCKUDO_APP_SECRET ?? "app-secret";
const WS_HOST = process.env.SOCKUDO_HOST ?? "127.0.0.1";
const WS_PORT = process.env.SOCKUDO_PORT ?? "6001";
const SERVER_PORT = parseInt(process.env.AUTH_PORT ?? "3457", 10);

const pusher = new Pusher({
  appId: APP_ID,
  key: APP_KEY,
  secret: APP_SECRET,
  host: WS_HOST,
  port: WS_PORT,
  useTLS: false,
});

// Simple auto-incrementing user id for demo purposes.
// Each new browser tab / connection gets its own user.
let userCounter = 0;
const sessionUsers = new Map<string, { id: string; name: string }>();

function getUserForSession(sessionId: string) {
  if (!sessionUsers.has(sessionId)) {
    userCounter++;
    sessionUsers.set(sessionId, {
      id: `user-${userCounter}`,
      name: `User ${userCounter}`,
    });
  }
  return sessionUsers.get(sessionId)!;
}

function cors(headers: Headers) {
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type, X-Session-Id");
}

const server = Bun.serve({
  port: SERVER_PORT,
  async fetch(req) {
    const url = new URL(req.url);

    // CORS preflight
    if (req.method === "OPTIONS") {
      const h = new Headers();
      cors(h);
      return new Response(null, { status: 204, headers: h });
    }

    const headers = new Headers({ "Content-Type": "application/json" });
    cors(headers);

    // ─── Channel Authorization ───────────────────────────────
    if (req.method === "POST" && url.pathname === "/pusher/auth") {
      try {
        const body = await parseBody(req);
        const socketId = body.socket_id;
        const channelName = body.channel_name;

        if (!socketId || !channelName) {
          return new Response(
            JSON.stringify({ error: "Missing socket_id or channel_name" }),
            {
              status: 400,
              headers,
            },
          );
        }

        const sessionId = req.headers.get("x-session-id") ?? socketId;
        const user = getUserForSession(sessionId);

        // Presence channels need user data
        if (channelName.startsWith("presence-")) {
          const presenceData = {
            user_id: user.id,
            user_info: {
              name: user.name,
              joined_at: new Date().toISOString(),
            },
          };
          const auth = pusher.authorizeChannel(
            socketId,
            channelName,
            presenceData,
          );
          console.log(
            `[auth] presence  ${channelName}  socket=${socketId}  user=${user.id}`,
          );
          return new Response(JSON.stringify(auth), { headers });
        }

        // Private / private-encrypted channels
        const auth = pusher.authorizeChannel(socketId, channelName);
        console.log(`[auth] private   ${channelName}  socket=${socketId}`);
        return new Response(JSON.stringify(auth), { headers });
      } catch (err: any) {
        console.error("[auth] error:", err.message);
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers,
        });
      }
    }

    // ─── User Authentication (signin) ────────────────────────
    if (req.method === "POST" && url.pathname === "/pusher/user-auth") {
      try {
        const body = await parseBody(req);
        const socketId = body.socket_id;

        if (!socketId) {
          return new Response(JSON.stringify({ error: "Missing socket_id" }), {
            status: 400,
            headers,
          });
        }

        const sessionId = req.headers.get("x-session-id") ?? socketId;
        const user = getUserForSession(sessionId);

        const auth = pusher.authenticateUser(socketId, {
          id: user.id,
          name: user.name,
          email: `${user.id}@example.com`,
          watchlist: [],
        });

        console.log(`[user-auth] signin  socket=${socketId}  user=${user.id}`);
        return new Response(JSON.stringify(auth), { headers });
      } catch (err: any) {
        console.error("[user-auth] error:", err.message);
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers,
        });
      }
    }

    // ─── Health ──────────────────────────────────────────────
    if (url.pathname === "/health") {
      return new Response(
        JSON.stringify({
          status: "ok",
          config: {
            appId: APP_ID,
            appKey: APP_KEY,
            host: WS_HOST,
            port: WS_PORT,
          },
          sessions: sessionUsers.size,
        }),
        { headers },
      );
    }

    return new Response(JSON.stringify({ error: "Not found" }), {
      status: 404,
      headers,
    });
  },
});

async function parseBody(req: Request): Promise<Record<string, string>> {
  const ct = req.headers.get("content-type") ?? "";
  if (ct.includes("application/json")) {
    return req.json();
  }
  // application/x-www-form-urlencoded (default for Pusher client)
  const text = await req.text();
  const params: Record<string, string> = {};
  for (const pair of text.split("&")) {
    const [k, v] = pair.split("=");
    if (k) params[decodeURIComponent(k)] = decodeURIComponent(v ?? "");
  }
  return params;
}

console.log(`
┌─────────────────────────────────────────────┐
│  Sockudo Dashboard Auth Server              │
│                                             │
│  POST /pusher/auth       → channel auth     │
│  POST /pusher/user-auth  → user signin      │
│  GET  /health            → server status    │
│                                             │
│  Listening on http://localhost:${SERVER_PORT}        │
│                                             │
│  Sockudo:  ${WS_HOST}:${WS_PORT}                    │
│  App ID:   ${APP_ID}                            │
│  App Key:  ${APP_KEY}                          │
└─────────────────────────────────────────────┘
`);
