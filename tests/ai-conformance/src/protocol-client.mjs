import crypto from "node:crypto";

export class AitProtocolClient {
  constructor(options = {}) {
    this.baseUrl = options.baseUrl ?? process.env.SOCKUDO_BASE_URL ?? "http://127.0.0.1:6001";
    this.wsUrl =
      options.wsUrl ??
      process.env.SOCKUDO_WS_URL ??
      "ws://127.0.0.1:6001/app/app-key?protocol=2&client=ait-conformance&version=0";
    this.appId = options.appId ?? process.env.SOCKUDO_APP_ID ?? "app-id";
    this.key = options.key ?? process.env.SOCKUDO_APP_KEY ?? "app-key";
    this.secret = options.secret ?? process.env.SOCKUDO_APP_SECRET ?? "app-secret";
    this.timeoutMs = Number(process.env.AIT_CONFORMANCE_TIMEOUT_MS ?? 5000);
  }

  async connect() {
    if (typeof WebSocket !== "function") {
      throw new Error("Node global WebSocket is required; run with Node >=22");
    }
    const socket = new WebSocket(this.wsUrl);
    const transcript = [];
    socket.addEventListener("message", (event) => {
      transcript.push(parseFrame(event.data));
    });
    await waitForEvent(socket, "open", this.timeoutMs);
    await waitUntil(
      () => transcript.some((frame) => frame.event === "sockudo:connection_established"),
      this.timeoutMs,
      "connection_established",
    );
    return new AitWsSession(socket, transcript, this.timeoutMs);
  }

  async publish({ name, channel, data, extras, messageId, idempotencyKey }) {
    return this.signedJson("POST", "/events", {
      name,
      channel,
      data,
      extras,
      ...(messageId ? { message_id: messageId } : {}),
      ...(idempotencyKey ? { idempotency_key: idempotencyKey } : {}),
    });
  }

  async append({ channel, messageSerial, data, extras, opId }) {
    return this.signedJson(
      "POST",
      `/channels/${encodeURIComponent(channel)}/messages/${encodeURIComponent(messageSerial)}/append`,
      {
        data,
        extras,
        ...(opId ? { op_id: opId } : {}),
      },
    );
  }

  async update({ channel, messageSerial, data, extras, opId }) {
    return this.signedJson(
      "POST",
      `/channels/${encodeURIComponent(channel)}/messages/${encodeURIComponent(messageSerial)}/update`,
      {
        data,
        extras,
        ...(opId ? { op_id: opId } : {}),
      },
    );
  }

  async getMessage({ channel, messageSerial }) {
    return this.signedJson(
      "GET",
      `/channels/${encodeURIComponent(channel)}/messages/${encodeURIComponent(messageSerial)}`,
    );
  }

  async signedJson(method, path, body) {
    const fullPath = `/apps/${this.appId}${path}`;
    const bodyText = body === undefined ? undefined : JSON.stringify(body);
    const query = {
      auth_key: this.key,
      auth_timestamp: `${Math.floor(Date.now() / 1000)}`,
      auth_version: "1.0",
    };
    if (bodyText !== undefined) {
      query.body_md5 = crypto.createHash("md5").update(bodyText).digest("hex");
    }
    const canonicalQuery = Object.keys(query)
      .map((key) => [key.toLowerCase(), query[key]])
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => `${key}=${value}`)
      .join("&");
    const signature = crypto
      .createHmac("sha256", this.secret)
      .update(`${method}\n${fullPath}\n${canonicalQuery}`)
      .digest("hex");

    const url = new URL(`${this.baseUrl}${fullPath}`);
    for (const [key, value] of Object.entries(query)) {
      url.searchParams.set(key, value);
    }
    url.searchParams.set("auth_signature", signature);

    const response = await fetch(url, {
      method,
      headers: bodyText === undefined ? undefined : { "content-type": "application/json" },
      body: bodyText,
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${text}`);
    }
    return text ? JSON.parse(text) : {};
  }
}

export class AitWsSession {
  constructor(socket, transcript, timeoutMs) {
    this.socket = socket;
    this.transcript = transcript;
    this.timeoutMs = timeoutMs;
  }

  send(event, data, channel) {
    this.socket.send(JSON.stringify({ event, data: JSON.stringify(data), channel }));
  }

  subscribe(channel, extra = {}) {
    this.send("pusher:subscribe", { channel, ...extra });
  }

  async waitForEvent(predicate, label) {
    return waitUntil(
      () => this.transcript.find((frame) => predicate(frame)),
      this.timeoutMs,
      label,
    );
  }

  close() {
    this.socket.close();
  }
}

export function aiExtras(transport, codec = {}) {
  return {
    ai: {
      transport,
      codec,
    },
  };
}

export function normalizeTranscript(frames) {
  return frames.map((frame) => normalizeValue(frame)).filter((frame) => frame.event !== "sockudo:connection_established");
}

function normalizeValue(value) {
  if (Array.isArray(value)) {
    return value.map(normalizeValue);
  }
  if (value && typeof value === "object") {
    const normalized = {};
    for (const [key, raw] of Object.entries(value)) {
      if (key === "socket_id") {
        normalized[key] = "<socket>";
      } else if (key === "channel" && typeof raw === "string") {
        normalized[key] = normalizeChannel(raw);
      } else if (key.endsWith("_serial") || key === "serial") {
        normalized[key] = typeof raw === "number" ? "<serial>" : normalizeValue(raw);
      } else if (key === "timestamp_ms" || key === "timestamp") {
        normalized[key] = "<timestamp>";
      } else if (key === "message_id" || key === "message_serial" || key === "version_serial") {
        normalized[key] = typeof raw === "string" && raw.length > 0 ? `<${key}>` : raw;
      } else if (key === "version" && raw && typeof raw === "object") {
        normalized[key] = { ...normalizeValue(raw), serial: "<version_serial>", timestamp_ms: "<timestamp>" };
      } else {
        normalized[key] = normalizeValue(raw);
      }
    }
    return normalized;
  }
  return value;
}

function normalizeChannel(value) {
  const runId = process.env.AIT_CONFORMANCE_RUN_ID ?? "golden";
  return value.endsWith(`-${runId}`) ? `${value.slice(0, -runId.length)}golden` : value;
}

function parseFrame(data) {
  const frame = JSON.parse(String(data));
  if (typeof frame.data === "string") {
    try {
      frame.data = JSON.parse(frame.data);
    } catch {
      // Pusher-compatible events may carry an arbitrary string payload.
    }
  }
  return frame;
}

function waitForEvent(target, event, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out waiting for ${event}`)), timeoutMs);
    target.addEventListener(event, (value) => {
      clearTimeout(timer);
      resolve(value);
    }, { once: true });
    target.addEventListener("error", (value) => {
      clearTimeout(timer);
      reject(new Error(`websocket error while waiting for ${event}: ${value.message ?? "unknown"}`));
    }, { once: true });
  });
}

async function waitUntil(check, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = check();
    if (value) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`timed out waiting for ${label}`);
}
