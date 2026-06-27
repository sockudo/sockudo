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
import { createHash, createHmac } from "node:crypto";
import {
  createServerTransport,
  type AI,
  type VercelOutput,
} from "../../client-sdks/sockudo-ai-transport-js/src/vercel/index.ts";
import type {
  ChannelEvents,
  ChannelLike,
  HistoryOptions,
  InboundMessage,
  MessageAck,
  MessageListener,
  MessageMutation,
  PaginatedResult,
  PresenceEventName,
  PresenceLike,
  PresenceMember,
  PublishMessage,
  Serial,
  SubscribeOptions,
  Unsubscribe,
} from "../../client-sdks/sockudo-ai-transport-js/src/realtime/index.ts";

const APP_ID = process.env.SOCKUDO_APP_ID ?? "app-id";
const APP_KEY = process.env.SOCKUDO_APP_KEY ?? "app-key";
const APP_SECRET = process.env.SOCKUDO_APP_SECRET ?? "app-secret";
const WS_HOST = process.env.SOCKUDO_HOST ?? "127.0.0.1";
const WS_PORT = process.env.SOCKUDO_PORT ?? "6001";
const SERVER_PORT = parseInt(process.env.AUTH_PORT ?? "3457", 10);
const OLLAMA_URL = process.env.OLLAMA_URL ?? "http://127.0.0.1:11434";
const OLLAMA_MODEL = process.env.OLLAMA_MODEL ?? "llama3.2";
const SOCKUDO_HTTP_URL =
  process.env.SOCKUDO_HTTP_URL ?? `http://${WS_HOST}:${WS_PORT}`;

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
  headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type, X-Session-Id");
}

async function listOllamaModels() {
  const res = await fetch(`${OLLAMA_URL}/api/tags`);
  const data = await res.json();
  if (!res.ok) {
    throw new Error(data?.error ?? `Ollama returned HTTP ${res.status}`);
  }
  return Array.isArray(data?.models)
    ? data.models.map((model: any) => model.name).filter(Boolean)
    : [];
}

async function resolveOllamaModel(requestedModel: string) {
  const models = await listOllamaModels();
  if (models.includes(requestedModel)) return { model: requestedModel, models };
  if (models.includes(OLLAMA_MODEL)) return { model: OLLAMA_MODEL, models };
  return { model: models[0] ?? requestedModel ?? OLLAMA_MODEL, models };
}

function sleep(ms: number, signal?: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(new DOMException("Aborted", "AbortError"));
      },
      { once: true },
    );
  });
}

function md5(input: string) {
  return createHash("md5").update(input).digest("hex");
}

function hmacSha256(key: string, input: string) {
  return createHmac("sha256", key).update(input).digest("hex");
}

function compact(source: Record<string, unknown>) {
  return Object.fromEntries(
    Object.entries(source).filter(([, value]) => value !== undefined),
  );
}

function serial(value: unknown): Serial | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) return value;
  return undefined;
}

function signRequest(
  method: string,
  path: string,
  queryParams: Record<string, string> = {},
  body?: string,
) {
  const params: Record<string, string> = {
    auth_key: APP_KEY,
    auth_timestamp: Math.floor(Date.now() / 1000).toString(),
    auth_version: "1.0",
    ...queryParams,
  };
  if (body) params.body_md5 = md5(body);
  const qs = Object.keys(params)
    .sort()
    .map((key) => `${key}=${params[key]}`)
    .join("&");
  params.auth_signature = hmacSha256(APP_SECRET, `${method}\n${path}\n${qs}`);
  return params;
}

async function sockudoApiRequest(
  method: string,
  path: string,
  body?: unknown,
  queryParams: Record<string, string> = {},
) {
  const bodyStr = body === undefined ? undefined : JSON.stringify(body);
  const fullPath = `/apps/${APP_ID}${path}`;
  const params = signRequest(method, fullPath, queryParams, bodyStr);
  const url = new URL(`${SOCKUDO_HTTP_URL}${fullPath}`);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, value);
  }

  const res = await fetch(url, {
    method,
    headers: bodyStr ? { "Content-Type": "application/json" } : undefined,
    body: bodyStr,
  });
  const raw = await res.text();
  let data: any = raw;
  if (raw) {
    try {
      data = JSON.parse(raw);
    } catch {
      data = raw;
    }
  }
  if (!res.ok) {
    throw new Error(data?.error ?? data?.message ?? (raw || `Sockudo HTTP ${res.status}`));
  }
  return data;
}

function normalizeAck(data: any, channelName: string, fallbackSerial?: string): MessageAck {
  const channels = data?.channels ?? {};
  const channelAck = channels[channelName] ?? channels[decodeURIComponent(channelName)] ?? {};
  const source = Object.keys(channelAck).length > 0 ? channelAck : data;
  const messageSerial =
    source.messageSerial ?? source.message_serial ?? data?.messageSerial ?? data?.message_serial ?? fallbackSerial;
  const historySerial = serial(
    source.historySerial ?? source.history_serial ?? data?.historySerial ?? data?.history_serial,
  );
  if (typeof messageSerial !== "string" || historySerial === undefined) {
    throw new Error(`Sockudo did not return a complete acknowledgement for ${channelName}.`);
  }
  return {
    messageSerial,
    historySerial,
    deliverySerial: serial(
      source.deliverySerial ?? source.delivery_serial ?? data?.deliverySerial ?? data?.delivery_serial,
    ),
    versionSerial:
      typeof (source.versionSerial ?? source.version_serial) === "string"
        ? String(source.versionSerial ?? source.version_serial)
        : undefined,
    status: typeof source.status === "string" ? source.status : undefined,
  };
}

function mutationBody(mutation: MessageMutation | Omit<MessageMutation, "data"> = {}, data?: unknown) {
  return compact({
    data,
    name: mutation.name,
    extras: mutation.extras,
    clear_fields: mutation.clearFields,
    client_id: mutation.clientId,
    socket_id: mutation.socketId,
    description: mutation.description,
    metadata: mutation.metadata,
    op_id: mutation.opId,
  });
}

function publishData(data: unknown) {
  if (data === undefined) return undefined;
  return typeof data === "string" ? data : JSON.stringify(data);
}

const emptyPresence: PresenceLike = {
  enter: async () => undefined,
  update: async () => undefined,
  leave: async () => undefined,
  get: async (): Promise<readonly PresenceMember[]> => [],
  subscribe: (_listener: (event: PresenceEventName, member: PresenceMember) => void) =>
    () => undefined,
};

function emptyHistory(): PaginatedResult<InboundMessage> {
  return {
    items: [],
    hasNext: () => false,
    next: async () => emptyHistory(),
  };
}

function noChannelEvent<K extends keyof ChannelEvents>(
  _event: K,
  _listener: (payload: ChannelEvents[K]) => void,
): Unsubscribe {
  return () => undefined;
}

function createHttpAiChannel(channelName: string): ChannelLike {
  return {
    name: channelName,
    attachSerial: undefined,
    presence: emptyPresence,
    async publish(message: PublishMessage): Promise<MessageAck> {
      try {
        const data = await sockudoApiRequest("POST", "/events", compact({
          name: message.name,
          channel: channelName,
          data: publishData(message.data),
          info: "message_serial,history_serial,delivery_serial,version_serial",
          message_id: message.messageId ?? message.messageSerial,
          idempotency_key: message.opId,
          socket_id: message.socketId,
          extras: message.extras,
        }));
        return normalizeAck(data, channelName, message.messageSerial);
      } catch (error: any) {
        console.warn("[ai-channel] publish failed", message.name, error?.message ?? String(error));
        throw error;
      }
    },
    async appendMessage(messageSerial: string, data: string, mutation = {}) {
      const result = await sockudoApiRequest(
        "POST",
        `/channels/${encodeURIComponent(channelName)}/messages/${encodeURIComponent(messageSerial)}/append`,
        mutationBody(mutation, data),
      );
      return normalizeAck(result, channelName, messageSerial);
    },
    async updateMessage(messageSerial: string, mutation: MessageMutation = {}) {
      const result = await sockudoApiRequest(
        "POST",
        `/channels/${encodeURIComponent(channelName)}/messages/${encodeURIComponent(messageSerial)}/update`,
        mutationBody(mutation, mutation.data),
      );
      return normalizeAck(result, channelName, messageSerial);
    },
    async deleteMessage(messageSerial: string, mutation: MessageMutation = {}) {
      const result = await sockudoApiRequest(
        "POST",
        `/channels/${encodeURIComponent(channelName)}/messages/${encodeURIComponent(messageSerial)}/delete`,
        mutationBody(mutation, mutation.data),
      );
      return normalizeAck(result, channelName, messageSerial);
    },
    subscribe(_listener: MessageListener, _options?: SubscribeOptions): Unsubscribe {
      return () => undefined;
    },
    history(_options?: HistoryOptions): Promise<PaginatedResult<InboundMessage>> {
      return Promise.resolve(emptyHistory());
    },
    on: noChannelEvent,
  };
}

function textFromUiMessage(message: AI.UIMessage) {
  return message.parts
    .map((part) => {
      if (part.type === "text" || part.type === "reasoning") return part.text;
      return "";
    })
    .join("")
    .trim();
}

function latestUserPrompt(body: any) {
  const messages = Array.isArray(body?.messages) && body.messages.length
    ? body.messages
    : Array.isArray(body?.history)
      ? body.history
      : [];
  const latest = [...messages].reverse().find((message: any) => message?.role === "user");
  return latest ? textFromUiMessage(latest as AI.UIMessage) : String(body?.prompt ?? "");
}

function ollamaMessagesFromBody(body: any) {
  const history = Array.isArray(body?.history) ? body.history : [];
  const current = Array.isArray(body?.messages) ? body.messages : [];
  const source = [...history, ...current]
    .filter((message: any) => message?.role === "user" || message?.role === "assistant")
    .slice(-10);
  return [
    {
      role: "system",
      content: [
        "You are the local Sockudo demo AI agent.",
        "Answer concisely while showcasing Sockudo AI Transport, durable history, recovery, versioned appends, presence, and annotations when relevant.",
        "Do not write fake push notification banners or claim a browser notification was displayed; Sockudo push is handled out-of-band by the dashboard after the turn.",
        "Avoid markdown tables unless the user asks for one.",
      ].join(" "),
    },
    ...source.map((message: AI.UIMessage) => ({
      role: message.role === "assistant" ? "assistant" : "user",
      content: textFromUiMessage(message),
    })),
  ];
}

function demoReply(prompt: string) {
  const subject = prompt.replace(/\s+/g, " ").trim().slice(0, 120);
  return `For "${subject}", Sockudo carries the user input as an AI Transport event, streams the assistant response as versioned appends, and persists the final aggregate for rewind and recovery. If the browser reconnects, the chat rebuilds from durable history while live subscribers keep receiving the same turn.`;
}

function chunkReply(reply: string) {
  const words = reply.split(/\s+/).filter(Boolean);
  const chunks: string[] = [];
  for (let i = 0; i < words.length; i += 7) {
    chunks.push(`${i === 0 ? "" : " "}${words.slice(i, i + 7).join(" ")}`);
  }
  return chunks;
}

function demoOutputStream(prompt: string, signal: AbortSignal): ReadableStream<VercelOutput> {
  return new ReadableStream({
    async start(controller) {
      controller.enqueue({ type: "start" });
      controller.enqueue({ type: "text-start", id: "text" });
      for (const chunk of chunkReply(demoReply(prompt))) {
        if (signal.aborted) break;
        controller.enqueue({ type: "text-delta", id: "text", delta: chunk });
        await sleep(90, signal).catch(() => undefined);
      }
      if (signal.aborted) {
        controller.enqueue({ type: "abort" });
      } else {
        controller.enqueue({ type: "text-end", id: "text" });
        controller.enqueue({ type: "finish", finishReason: "stop" });
      }
      controller.close();
    },
  });
}

function ollamaOutputStream(body: any, signal: AbortSignal): ReadableStream<VercelOutput> {
  return new ReadableStream({
    async start(controller) {
      let pending = "";
      let lastFlushAt = Date.now();

      function flush(force = false) {
        if (!pending) return;
        const hasSentenceBreak = /[.!?]\s?$/.test(pending) && pending.length >= 80;
        const shouldFlush =
          force || pending.length >= 220 || Date.now() - lastFlushAt >= 650 || hasSentenceBreak;
        if (!shouldFlush) return;
        const delta = pending;
        pending = "";
        lastFlushAt = Date.now();
        controller.enqueue({ type: "text-delta", id: "text", delta });
      }

      controller.enqueue({ type: "start" });
      controller.enqueue({ type: "text-start", id: "text" });

      try {
        const requestedModel = typeof body?.model === "string" && body.model.trim()
          ? body.model.trim()
          : OLLAMA_MODEL;
        const { model } = await resolveOllamaModel(requestedModel);
        const temperature = Number.isFinite(Number(body?.temperature))
          ? Number(body.temperature)
          : 0.2;
        const ollamaRes = await fetch(`${OLLAMA_URL}/api/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model,
            messages: ollamaMessagesFromBody(body),
            stream: true,
            options: { temperature },
          }),
          signal,
        });

        if (!ollamaRes.ok || !ollamaRes.body) {
          const text = await ollamaRes.text();
          throw new Error(text || `Ollama returned HTTP ${ollamaRes.status}`);
        }

        const reader = ollamaRes.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        for (;;) {
          const { done, value } = await reader.read();
          if (done || signal.aborted) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() ?? "";
          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            const payload = JSON.parse(trimmed);
            if (payload?.error) throw new Error(String(payload.error));
            pending += payload?.message?.content ?? payload?.response ?? "";
            flush(false);
            if (payload.done) break;
          }
        }

        if (buffer.trim()) {
          const payload = JSON.parse(buffer.trim());
          if (payload?.error) throw new Error(String(payload.error));
          pending += payload?.message?.content ?? payload?.response ?? "";
        }
        flush(true);
        controller.enqueue({ type: "text-end", id: "text" });
        controller.enqueue({ type: "finish", finishReason: "stop" });
      } catch (err: any) {
        if (signal.aborted) {
          controller.enqueue({ type: "abort" });
        } else {
          controller.enqueue({
            type: "error",
            errorText: err?.message ?? "Ollama request failed",
          });
        }
      } finally {
        controller.close();
      }
    },
  });
}

function composedSignal(left: AbortSignal, right: AbortSignal) {
  if ("any" in AbortSignal && typeof AbortSignal.any === "function") {
    return AbortSignal.any([left, right]);
  }
  const controller = new AbortController();
  const abort = () => controller.abort();
  left.addEventListener("abort", abort, { once: true });
  right.addEventListener("abort", abort, { once: true });
  return controller.signal;
}

const turnAbortControllers = new Map<string, AbortController>();

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

    // ─── Ollama Helper ──────────────────────────────────────
    if (req.method === "GET" && url.pathname === "/demo/ollama/status") {
      try {
        const { model, models } = await resolveOllamaModel(OLLAMA_MODEL);
        return new Response(
          JSON.stringify({
            reachable: true,
            baseUrl: OLLAMA_URL,
            defaultModel: OLLAMA_MODEL,
            selectedModel: model,
            models,
          }),
          { status: 200, headers },
        );
      } catch (err: any) {
        return new Response(
          JSON.stringify({
            reachable: false,
            baseUrl: OLLAMA_URL,
            defaultModel: OLLAMA_MODEL,
            error: err.message,
          }),
          { status: 503, headers },
        );
      }
    }

    if (req.method === "POST" && url.pathname === "/demo/ollama/chat") {
      try {
        const body = await req.json();
        const requestedModel = typeof body?.model === "string" && body.model.trim()
          ? body.model.trim()
          : OLLAMA_MODEL;
        const { model } = await resolveOllamaModel(requestedModel);
        const messages = Array.isArray(body?.messages) && body.messages.length > 0
          ? body.messages
          : [{ role: "user", content: String(body?.prompt ?? "") }];
        const temperature = Number.isFinite(Number(body?.temperature))
          ? Number(body.temperature)
          : 0.2;

        const ollamaRes = await fetch(`${OLLAMA_URL}/api/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            model,
            messages,
            stream: true,
            options: { temperature },
          }),
          signal: req.signal,
        });

        const streamHeaders = new Headers({
          "Content-Type": "application/x-ndjson",
          "Cache-Control": "no-store",
        });
        cors(streamHeaders);

        if (!ollamaRes.ok || !ollamaRes.body) {
          const text = await ollamaRes.text();
          return new Response(
            JSON.stringify({
              error: text || `Ollama returned HTTP ${ollamaRes.status}`,
              model,
              baseUrl: OLLAMA_URL,
            }),
            { status: 502, headers },
          );
        }

        return new Response(ollamaRes.body, {
          status: 200,
          headers: streamHeaders,
        });
      } catch (err: any) {
        return new Response(
          JSON.stringify({
            error: err.message,
            model: OLLAMA_MODEL,
            baseUrl: OLLAMA_URL,
          }),
          { status: 503, headers },
        );
      }
    }

    if (req.method === "POST" && url.pathname === "/demo/ai/cancel") {
      try {
        const body = await req.json().catch(() => ({}));
        const turnId = typeof body?.turnId === "string" ? body.turnId : "";
        const controller = turnAbortControllers.get(turnId);
        controller?.abort();
        return new Response(
          JSON.stringify({ ok: true, cancelled: Boolean(controller), turnId }),
          { status: 200, headers },
        );
      } catch (err: any) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers,
        });
      }
    }

    if (req.method === "POST" && url.pathname === "/demo/ai/chat") {
      let transport: ReturnType<typeof createServerTransport> | null = null;
      let turnId = "";
      try {
        const body = await req.json();
        const channelName = typeof body?.sessionName === "string" && body.sessionName.trim()
          ? body.sessionName.trim()
          : "private-ai-demo";
        const mode = body?.mode === "demo" || body?.mode === "external" ? body.mode : "ollama";
        turnId = typeof body?.turnId === "string" && body.turnId.trim()
          ? body.turnId.trim()
          : crypto.randomUUID();

        if (mode === "external") {
          return new Response(
            JSON.stringify({ ok: true, mode, turnId, status: "waiting_for_external_agent" }),
            { status: 202, headers },
          );
        }

        const localAbort = new AbortController();
        turnAbortControllers.set(turnId, localAbort);
        const signal = composedSignal(req.signal, localAbort.signal);
        const prompt = latestUserPrompt(body);

        transport = createServerTransport({
          channel: createHttpAiChannel(channelName),
          inputEventLookupTimeoutMs: 0,
          onError(error) {
            console.warn("[ai-transport] server transport:", error.message);
          },
        });

        const turn = transport.newTurn({
          turnId,
          clientId: typeof body?.clientId === "string" ? body.clientId : undefined,
          invocationId: typeof body?.invocationId === "string" ? body.invocationId : undefined,
          parent: typeof body?.parent === "string" ? body.parent : undefined,
          forkOf: typeof body?.forkOf === "string" ? body.forkOf : undefined,
          signal,
          onAbort: async (write) => {
            await write({ type: "abort" });
          },
          onError(error) {
            console.warn("[ai-transport] turn:", error.message);
          },
        });

        await turn.start();
        const stream = mode === "demo"
          ? demoOutputStream(prompt, signal)
          : ollamaOutputStream(body, signal);
        const result = await turn.streamResponse(stream);
        await turn.end(result.reason === "error" ? "error" : result.reason);
        return new Response(
          JSON.stringify({
            ok: true,
            mode,
            turnId,
            reason: result.reason,
            error: result.error?.message,
          }),
          { status: 200, headers },
        );
      } catch (err: any) {
        return new Response(
          JSON.stringify({
            ok: false,
            turnId,
            error: err?.message ?? "AI transport helper failed",
          }),
          { status: 500, headers },
        );
      } finally {
        if (turnId) turnAbortControllers.delete(turnId);
        transport?.close();
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
            ollamaUrl: OLLAMA_URL,
            ollamaModel: OLLAMA_MODEL,
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
│  POST /demo/ai/chat      → AI Transport turn│
│  POST /demo/ai/cancel    → cancel AI turn   │
│  POST /demo/ollama/chat  → Ollama stream    │
│  GET  /health            → server status    │
│                                             │
│  Listening on http://localhost:${SERVER_PORT}        │
│                                             │
│  Sockudo:  ${WS_HOST}:${WS_PORT}                    │
│  App ID:   ${APP_ID}                            │
│  App Key:  ${APP_KEY}                          │
│  Ollama:   ${OLLAMA_URL} (${OLLAMA_MODEL})  │
└─────────────────────────────────────────────┘
`);
