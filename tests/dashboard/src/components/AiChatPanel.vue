<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import {
    Bell,
    Bot,
    CheckCircle2,
    Loader2,
    MessageSquarePlus,
    RefreshCcw,
    RotateCcw,
    Send,
    Settings2,
    Square,
    Trash2,
    Wifi,
} from "lucide-vue-next";
import type { ActiveTurn, EventUnsubscribe } from "@sockudo/ai-transport";
import {
    createClientTransport,
    type AI,
    type VercelOutput,
} from "@sockudo/ai-transport/vercel";
import { useDashboardStore } from "../stores/dashboard";
import { useHttpApi, type ApiResponse } from "../composables/useHttpApi";
import { createDashboardAiChannel } from "../composables/useAiTransportChannel";
import { renderMarkdown } from "../composables/useMarkdown";
import { usePusher } from "../composables/usePusher";

type ChatRole = "user" | "assistant" | "system";
type ChatStatus = "pending" | "streaming" | "complete" | "cancelled" | "error";
type AgentMode = "ollama" | "demo" | "external";
type PushTarget = "channel" | "client" | "device";

interface ChatMessage {
    id: string;
    role: ChatRole;
    content: string;
    status: ChatStatus;
    turnId?: string;
    messageId?: string;
    messageSerial?: string;
    replyToId?: string;
    error?: string;
    createdAtMs: number;
    updatedAtMs: number;
}

type AiClientTransport = ReturnType<typeof createClientTransport>;

const store = useDashboardStore();
const { connect, subscribe } = usePusher();

const channel = ref("private-ai-demo");
const clientId = ref("demo-user");
const agentMode = ref<AgentMode>("ollama");
const ollamaModel = ref("llama3.2");
const ollamaTemperature = ref(0.2);
const ollamaStatus = ref("not checked");
const checkingOllama = ref(false);
const pushOnComplete = ref(true);
const pushTarget = ref<PushTarget>("channel");
const pushChannel = ref("private-ai-demo");
const pushDeviceId = ref("browser-device-1");
const lastPushResult = ref<ApiResponse | null>(null);
const composer = ref("");
const messages = ref<ChatMessage[]>([]);
const showSettings = ref(false);
const loadingHistory = ref(false);
const sending = ref(false);
const cancelling = ref(false);
const lastNotice = ref("");
const lastError = ref("");
const scrollEl = ref<HTMLElement | null>(null);
const activeRun = ref<{
    turnId: string;
    assistantId: string;
    cancelled: boolean;
} | null>(null);
const boundChannels = new Set<string>();
let aiTransport: AiClientTransport | null = null;
let aiTransportChannel = "";
let activeTurn: ActiveTurn<VercelOutput> | null = null;
let transportUnsubscribes: EventUnsubscribe[] = [];
let activeOllamaAbort: AbortController | null = null;

const isConnected = computed(() => store.connectionState === "connected");
const isBusy = computed(
    () =>
        sending.value ||
        messages.value.some(
            (message) =>
                message.status === "streaming" || message.status === "pending",
        ),
);
const activeAssistant = computed(() => {
    const run = activeRun.value;
    return run
        ? messages.value.find((message) => message.id === run.assistantId)
        : undefined;
});

const emptyPrompts = [
    "Show how Sockudo recovers an interrupted AI stream.",
    "Explain how push notifications can follow an AI turn.",
    "Describe why append rollup is safe for durable history.",
];

function now() {
    return Date.now();
}

function randomSuffix() {
    return Math.random().toString(36).slice(2, 8);
}

function sleep(ms: number) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function statusLabel(status: ChatStatus) {
    if (status === "pending") return "pending";
    if (status === "streaming") return "streaming";
    if (status === "cancelled") return "stopped";
    if (status === "error") return "failed";
    return "done";
}

function chatMessage(
    role: ChatRole,
    content: string,
    status: ChatStatus,
    extra: Partial<ChatMessage> = {},
): ChatMessage {
    const timestamp = now();
    return {
        id: `${role}-${timestamp}-${randomSuffix()}`,
        role,
        content,
        status,
        createdAtMs: timestamp,
        updatedAtMs: timestamp,
        ...extra,
    };
}

function aiExtras(
    runId: string,
    status: "streaming" | "complete" | "cancelled",
    role: "user" | "assistant",
) {
    return {
        ai: {
            transport: {
                "run-id": runId,
                status,
                role,
            },
            codec: {
                "content-type": "application/json",
            },
        },
    };
}

function getTransport(extras: any) {
    return extras?.ai?.transport ?? extras?.transport ?? {};
}

function getTransportRole(extras: any): "user" | "assistant" | undefined {
    const role = getTransport(extras)?.role;
    return role === "user" || role === "assistant" ? role : undefined;
}

function getTurnId(extras: any, fallback?: string) {
    return (
        getTransport(extras)?.["run-id"] ??
        getTransport(extras)?.run_id ??
        getTransport(extras)?.runId ??
        getTransport(extras)?.["turn-id"] ??
        getTransport(extras)?.turnId ??
        fallback
    );
}

function getTransportStatus(extras: any): ChatStatus {
    const status = getTransport(extras)?.status;
    if (status === "complete" || status === "cancelled") return status;
    if (status === "streaming") return "streaming";
    return "complete";
}

function textFromData(data: unknown): string {
    if (typeof data === "string") {
        try {
            const parsed = JSON.parse(data);
            return textFromData(parsed);
        } catch {
            return data;
        }
    }
    if (data && typeof data === "object") {
        const record = data as Record<string, unknown>;
        if (typeof record.content === "string") return record.content;
        if (typeof record.text === "string") return record.text;
        if (typeof record.body === "string") return record.body;
        if (typeof record.message === "string") return record.message;
        return JSON.stringify(data, null, 2);
    }
    return data == null ? "" : String(data);
}

function dataRecord(data: unknown): Record<string, unknown> | null {
    if (typeof data === "string") {
        try {
            return dataRecord(JSON.parse(data));
        } catch {
            return null;
        }
    }
    return data && typeof data === "object"
        ? (data as Record<string, unknown>)
        : null;
}

function hasDisplayText(data: unknown): boolean {
    if (typeof data === "string") {
        const record = dataRecord(data);
        return record ? hasDisplayText(record) : data.trim().length > 0;
    }
    const record = dataRecord(data);
    return Boolean(
        record &&
        (typeof record.content === "string" ||
            typeof record.text === "string" ||
            typeof record.body === "string" ||
            typeof record.message === "string"),
    );
}

function turnIdFromData(data: unknown) {
    const record = dataRecord(data);
    return record?.turn_id ?? record?.turnId;
}

function responseError(res: ApiResponse, label: string) {
    const data = res.data as any;
    if (res.status >= 200 && res.status < 300) return "";
    return (
        data?.error ??
        data?.message ??
        `${label} failed with HTTP ${res.status}`
    );
}

function apiOk(res: ApiResponse | null | undefined) {
    return Boolean(res && res.status >= 200 && res.status < 300);
}

function modeLabel(mode: AgentMode) {
    if (mode === "ollama") return "Ollama";
    if (mode === "demo") return "Demo";
    return "External";
}

function modeHint() {
    if (agentMode.value === "ollama")
        return "@sockudo/ai-transport streams Ollama through versioned appends; push publishes when configured.";
    if (agentMode.value === "demo")
        return "@sockudo/ai-transport runs the built-in demo agent through Sockudo.";
    return "Waiting for an external agent on the channel.";
}

function ollamaError(message: string) {
    const error = new Error(message);
    const tagged = error as Error & { source?: string };
    tagged.source = "ollama";
    return tagged;
}

function extractAckSerial(res: ApiResponse, targetChannel: string) {
    const data = res.data as any;
    const channels = data?.channels ?? {};
    const ack =
        channels[targetChannel] ?? channels[decodeURIComponent(targetChannel)];
    return ack?.message_serial ?? ack?.messageSerial ?? "";
}

function upsertMessage(message: ChatMessage) {
    const bySerial = message.messageSerial
        ? messages.value.findIndex(
              (existing) => existing.messageSerial === message.messageSerial,
          )
        : -1;
    const byId = messages.value.findIndex(
        (existing) => existing.id === message.id,
    );
    const index = bySerial >= 0 ? bySerial : byId;
    if (index >= 0) {
        messages.value[index] = {
            ...messages.value[index],
            ...message,
            id: messages.value[index].id,
            createdAtMs: messages.value[index].createdAtMs,
            updatedAtMs: now(),
        };
    } else {
        messages.value.push(message);
    }
    sortMessages();
}

function updateMessage(id: string, patch: Partial<ChatMessage>) {
    const index = messages.value.findIndex((message) => message.id === id);
    if (index < 0) return;
    messages.value[index] = {
        ...messages.value[index],
        ...patch,
        updatedAtMs: now(),
    };
}

function sortMessages() {
    messages.value = [...messages.value].sort(
        (left, right) => left.createdAtMs - right.createdAtMs,
    );
}

function uiMessageText(message: AI.UIMessage) {
    return message.parts
        .map((part) => {
            if (part.type === "text" || part.type === "reasoning")
                return part.text;
            if (part.type === "dynamic-tool") {
                if (part.state === "output-error") return part.errorText ?? "";
                if (part.output !== undefined)
                    return `\n\n\`\`\`json\n${JSON.stringify(part.output, null, 2)}\n\`\`\``;
            }
            if (part.type === "source-url")
                return part.title ? `[${part.title}](${part.url})` : part.url;
            if (part.type === "file") return part.filename ?? part.url;
            return "";
        })
        .filter(Boolean)
        .join("");
}

function metadataStatus(
    status: string | undefined,
    role: ChatRole,
): ChatStatus {
    if (status === "cancelled") return "cancelled";
    if (status === "error") return "error";
    if (status === "streaming" || status === "active" || status === "suspended")
        return "streaming";
    if (status === "complete") return "complete";
    return role === "assistant" ? "streaming" : "complete";
}

function syncMessagesFromTransport() {
    if (!aiTransport) return;
    const previous = new Map(
        messages.value.map((message) => [
            message.messageId ?? message.id,
            message,
        ]),
    );
    const viewMessages = aiTransport.view.getMessages();
    messages.value = viewMessages
        .map((message) => {
            const role: ChatRole =
                message.role === "assistant"
                    ? "assistant"
                    : message.role === "user"
                      ? "user"
                      : "system";
            const id = `${role}-${message.id}`;
            const existing = previous.get(message.id) ?? previous.get(id);
            const metadata = aiTransport?.view.getMessageMetadata(message.id);
            const timestamp = existing?.createdAtMs ?? now();
            return {
                id,
                role,
                content: uiMessageText(message),
                status: metadataStatus(metadata?.status, role),
                turnId: metadata?.turnId,
                messageId: message.id,
                messageSerial: metadata?.codecMessageId,
                replyToId:
                    role === "assistant" && metadata?.turnId
                        ? `user-${metadata.turnId}`
                        : existing?.replyToId,
                createdAtMs: timestamp,
                updatedAtMs: now(),
            } satisfies ChatMessage;
        })
        .filter((message) => {
            if (message.role === "system") return Boolean(message.content.trim());
            if (message.role !== "assistant") return true;
            return (
                Boolean(message.content.trim()) ||
                activeRun.value?.turnId === message.turnId
            );
        });
    sortMessages();
}

async function closeAiTransport() {
    for (const unsubscribe of transportUnsubscribes) unsubscribe();
    transportUnsubscribes = [];
    activeTurn = null;
    const transport = aiTransport;
    aiTransport = null;
    aiTransportChannel = "";
    if (transport) await transport.close().catch(() => undefined);
}

async function ensureAiTransport() {
    const targetChannel = channel.value.trim();
    if (!targetChannel) throw new Error("Choose a channel first.");
    await ensureConnection();
    subscribe(targetChannel);
    if (aiTransport && aiTransportChannel === targetChannel) return aiTransport;

    await closeAiTransport();
    const transport = createClientTransport({
        channel: createDashboardAiChannel(targetChannel),
        api: "/demo/ai/chat",
        clientId: clientId.value.trim(),
        body: () => ({
            mode: agentMode.value,
            model: ollamaModel.value.trim(),
            temperature: Number(ollamaTemperature.value) || 0.2,
            clientId: clientId.value.trim(),
        }),
    });
    aiTransport = transport;
    aiTransportChannel = targetChannel;
    transportUnsubscribes = [
        transport.view.on("update", syncMessagesFromTransport),
        transport.on("error", (error) => {
            lastNotice.value = error.message;
        }),
    ];
    syncMessagesFromTransport();
    return transport;
}

async function consumeTurnStream(
    stream: ReadableStream<VercelOutput>,
    stopAfter?: Promise<unknown>,
) {
    const reader = stream.getReader();
    stopAfter?.then(() => reader.cancel().catch(() => undefined));
    let content = "";
    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (value.type === "text-delta") content += value.delta;
            if (value.type === "error") throw new Error(value.errorText);
            if (value.type === "abort") break;
        }
    } finally {
        reader.releaseLock();
    }
    return content;
}

async function cancelServerTurn(turnId: string) {
    await fetch("/demo/ai/cancel", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ turnId }),
    }).catch(() => undefined);
}

async function scrollToBottom() {
    await nextTick();
    if (scrollEl.value) scrollEl.value.scrollTop = scrollEl.value.scrollHeight;
}

async function ensureConnection() {
    if (isConnected.value) return;
    connect();
    const start = now();
    while (!isConnected.value && now() - start < 5000) {
        await sleep(100);
    }
    if (!isConnected.value) {
        throw new Error("Could not connect to Sockudo.");
    }
}

function bindChatEvents(targetChannel: string) {
    boundChannels.add(targetChannel);
}

async function ensureSubscribed() {
    const transport = await ensureAiTransport();
    bindChatEvents(channel.value.trim());
    return transport;
}

function mergeAssistantFrame(data: any, eventName = "") {
    const envelope = data?.message ? data : (data?.item ?? data);
    const message = envelope?.message ?? envelope;
    const extras = message?.extras ?? envelope?.extras;
    const name = message?.name ?? envelope?.name;
    const serial =
        envelope?.message_serial ??
        envelope?.messageSerial ??
        message?.message_serial ??
        message?.messageSerial;
    const run = activeRun.value;
    const payload = message?.data ?? envelope?.data ?? data;
    const turnId = getTurnId(
        extras,
        String(turnIdFromData(payload) ?? run?.turnId ?? ""),
    );
    const role = getTransportRole(extras);
    const incomingText = textFromData(payload);
    const isAiInput =
        name === "ai-input" ||
        eventName === "ai-input" ||
        (role === "user" && hasDisplayText(payload));

    if (isAiInput) {
        const id = `user-${turnId || serial || now()}`;
        upsertMessage(
            chatMessage("user", incomingText, "complete", {
                id,
                turnId,
                messageId: `ai-input-${turnId || serial || now()}`,
                messageSerial: serial ? String(serial) : undefined,
            }),
        );
        return;
    }

    const isAiOutput =
        name === "ai-output" ||
        eventName === "ai-output" ||
        role === "assistant" ||
        Boolean(extras?.ai);
    if (!isAiOutput && !serial) return;

    const activeAssistantId =
        run && turnId && run.turnId === turnId ? run.assistantId : undefined;
    const fallbackId = `assistant-${turnId ?? now()}`;
    const existing = messages.value.find(
        (entry) =>
            (serial && entry.messageSerial === serial) ||
            (activeAssistantId && entry.id === activeAssistantId) ||
            entry.id === fallbackId,
    );
    const id = serial
        ? `assistant-${serial}`
        : (existing?.id ?? activeAssistantId ?? fallbackId);
    const status = eventName.includes("delete")
        ? "cancelled"
        : getTransportStatus(extras);
    const nextStatus =
        existing?.status === "complete" && status === "streaming"
            ? existing.status
            : status;
    const content =
        eventName === "ai-output" && !incomingText && existing
            ? existing.content
            : eventName.includes("append") &&
                existing &&
                incomingText.length < existing.content.length
              ? `${existing.content}${incomingText}`
              : incomingText;

    upsertMessage(
        chatMessage("assistant", content, nextStatus, {
            id,
            turnId,
            messageSerial: serial ? String(serial) : existing?.messageSerial,
            replyToId: turnId ? `user-${turnId}` : existing?.replyToId,
        }),
    );

    if (
        activeRun.value?.turnId === turnId &&
        (nextStatus === "complete" ||
            nextStatus === "cancelled" ||
            nextStatus === "error")
    ) {
        activeRun.value = null;
        sending.value = false;
        lastNotice.value = "";
    }
}

function demoReply(prompt: string) {
    const subject = prompt.replace(/\s+/g, " ").trim().slice(0, 120);
    return `For "${subject}", Sockudo carries the user input as an AI event, streams the assistant response as versioned appends, and persists the final aggregate for rewind and recovery. If the browser reconnects, the chat can rebuild from durable history while live subscribers keep receiving the same turn.`;
}

function chunkReply(reply: string) {
    const words = reply.split(/\s+/).filter(Boolean);
    const chunks: string[] = [];
    for (let i = 0; i < words.length; i += 7) {
        chunks.push(`${i === 0 ? "" : " "}${words.slice(i, i + 7).join(" ")}`);
    }
    return chunks;
}

async function publishAiInput(turnId: string, text: string) {
    const res = await useHttpApi().publishAdvancedEvent({
        name: "ai-input",
        channel: channel.value.trim(),
        data: JSON.stringify({
            role: "user",
            content: text,
            turn_id: turnId,
            client_id: clientId.value.trim(),
        }),
        message_id: `ai-input-${turnId}`,
        info: "message_serial,history_serial,delivery_serial,version_serial",
        extras: aiExtras(turnId, "complete", "user"),
    });
    const error = responseError(res, "ai-input");
    if (error) throw new Error(error);
}

async function createAiOutput(turnId: string) {
    const messageId = `ai-output-${turnId}`;
    const res = await useHttpApi().publishAdvancedEvent({
        name: "ai-output",
        channel: channel.value.trim(),
        data: "",
        message_id: messageId,
        info: "message_serial,history_serial,delivery_serial,version_serial",
        extras: aiExtras(turnId, "streaming", "assistant"),
    });
    const error = responseError(res, "ai-output");
    if (error) throw new Error(error);
    const serial = extractAckSerial(res, channel.value.trim());
    if (!serial)
        throw new Error(
            "Sockudo did not return a message serial for ai-output.",
        );
    return { messageId, messageSerial: String(serial) };
}

async function appendAiChunk(
    turnId: string,
    messageSerial: string,
    chunk: string,
    index: number,
) {
    const res = await useHttpApi().appendMessage(
        channel.value.trim(),
        messageSerial,
        {
            data: chunk,
            extras: aiExtras(turnId, "streaming", "assistant"),
            op_id: `${turnId}-append-${index + 1}`,
            description: `chat chunk ${index + 1}`,
        },
    );
    const error = responseError(res, "message.append");
    if (error) throw new Error(error);
}

async function finishAiOutput(
    turnId: string,
    messageSerial: string,
    status: "complete" | "cancelled",
) {
    const res = await useHttpApi().updateMessage(
        channel.value.trim(),
        messageSerial,
        {
            extras: aiExtras(turnId, status, "assistant"),
            op_id: `${turnId}-${status}`,
            description: `chat ${status}`,
        },
    );
    const error = responseError(res, "message.update");
    if (error) throw new Error(error);
}

async function appendAndRenderChunk(
    turnId: string,
    messageSerial: string,
    chunk: string,
    index: number,
) {
    if (!chunk) return "";
    await appendAiChunk(turnId, messageSerial, chunk, index);
    const current = messages.value.find(
        (message) => message.messageSerial === messageSerial,
    );
    updateMessage(current?.id ?? `assistant-${messageSerial}`, {
        content: `${current?.content ?? ""}${chunk}`,
        status: "streaming",
    });
    await scrollToBottom();
    return chunk;
}

function buildOllamaMessages(prompt: string, turnId: string) {
    const prior = messages.value
        .filter(
            (message) => message.turnId !== turnId && message.content.trim(),
        )
        .filter(
            (message) =>
                message.role === "user" || message.role === "assistant",
        )
        .slice(-8)
        .map((message) => ({
            role: message.role === "assistant" ? "assistant" : "user",
            content: message.content,
        }));

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
        ...prior,
        { role: "user", content: prompt },
    ];
}

function ollamaContent(payload: any) {
    if (payload?.error) throw ollamaError(String(payload.error));
    return payload?.message?.content ?? payload?.response ?? "";
}

async function checkOllama() {
    checkingOllama.value = true;
    try {
        const res = await fetch("/demo/ollama/status");
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data?.reachable) {
            ollamaStatus.value =
                data?.error ?? `unreachable at ${data?.baseUrl ?? "Ollama"}`;
            return;
        }
        const models = Array.isArray(data.models) ? data.models : [];
        if (data.selectedModel && data.selectedModel !== ollamaModel.value) {
            ollamaModel.value = data.selectedModel;
            ollamaStatus.value = `${data.selectedModel} selected`;
            return;
        }
        ollamaStatus.value = models.includes(ollamaModel.value)
            ? `${ollamaModel.value} ready`
            : `reachable; default ${data.defaultModel ?? ollamaModel.value}`;
    } catch (error: any) {
        ollamaStatus.value = error?.message ?? "Ollama status failed";
    } finally {
        checkingOllama.value = false;
    }
}

async function streamDemoOutput(
    turnId: string,
    messageSerial: string,
    prompt: string,
) {
    let content = "";
    const reply = demoReply(prompt);
    for (const [index, chunk] of chunkReply(reply).entries()) {
        if (activeRun.value?.cancelled) break;
        content += await appendAndRenderChunk(
            turnId,
            messageSerial,
            chunk,
            index,
        );
        await sleep(90);
    }
    return content;
}

async function streamOllamaOutput(
    turnId: string,
    messageSerial: string,
    prompt: string,
) {
    activeOllamaAbort?.abort();
    const controller = new AbortController();
    activeOllamaAbort = controller;

    let content = "";
    let pending = "";
    let appendIndex = 0;
    let lastFlushAt = now();

    async function flush(force = false) {
        if (!pending) return;
        const hasSentenceBreak =
            /[.!?]\s?$/.test(pending) && pending.length >= 80;
        const shouldFlush =
            force ||
            pending.length >= 220 ||
            now() - lastFlushAt >= 650 ||
            hasSentenceBreak;
        if (!shouldFlush) return;
        const chunk = pending;
        pending = "";
        lastFlushAt = now();
        content += await appendAndRenderChunk(
            turnId,
            messageSerial,
            chunk,
            appendIndex++,
        );
    }

    try {
        const res = await fetch("/demo/ollama/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                model: ollamaModel.value.trim(),
                temperature: Number(ollamaTemperature.value) || 0.2,
                messages: buildOllamaMessages(prompt, turnId),
            }),
            signal: controller.signal,
        });

        if (!res.ok || !res.body) {
            const data = await res.json().catch(async () => ({
                error: await res.text().catch(() => ""),
            }));
            throw ollamaError(
                data?.error ?? `Ollama helper returned HTTP ${res.status}`,
            );
        }

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (activeRun.value?.cancelled) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop() ?? "";

            for (const line of lines) {
                const trimmed = line.trim();
                if (!trimmed) continue;
                const payload = JSON.parse(trimmed);
                pending += ollamaContent(payload);
                await flush(false);
                if (payload.done) break;
            }
        }

        if (buffer.trim()) {
            const payload = JSON.parse(buffer.trim());
            pending += ollamaContent(payload);
        }

        await flush(true);
        ollamaStatus.value = `${ollamaModel.value || "Ollama"} streamed ${appendIndex} appends`;
        return content;
    } catch (error: any) {
        if (error?.name === "AbortError" || activeRun.value?.cancelled)
            return content;
        if (error?.source === "ollama") throw error;
        throw ollamaError(error?.message ?? "Ollama request failed");
    } finally {
        if (activeOllamaAbort === controller) activeOllamaAbort = null;
    }
}

async function streamAssistantOutput(
    turnId: string,
    messageSerial: string,
    prompt: string,
) {
    if (agentMode.value === "demo")
        return streamDemoOutput(turnId, messageSerial, prompt);
    if (agentMode.value !== "ollama") return "";

    try {
        return await streamOllamaOutput(turnId, messageSerial, prompt);
    } catch (error: any) {
        if (error?.source !== "ollama") throw error;
        lastNotice.value = `${error.message}. Using the built-in demo reply.`;
        return streamDemoOutput(turnId, messageSerial, prompt);
    }
}

function aiPushRecipients() {
    if (pushTarget.value === "client") {
        return [{ type: "client", client_id: clientId.value.trim() }];
    }
    if (pushTarget.value === "device") {
        return [{ type: "device", device_id: pushDeviceId.value.trim() }];
    }
    return [
        {
            type: "channel",
            channel: pushChannel.value.trim() || channel.value.trim(),
        },
    ];
}

async function publishAiPushNotification(
    turnId: string,
    status: "complete" | "cancelled",
    content: string,
) {
    if (!pushOnComplete.value) return;
    const body = content.trim().replace(/\s+/g, " ").slice(0, 180);
    const res = await useHttpApi().publishPush(
        {
            publishId: `ai-${turnId}-${status}`,
            recipients: aiPushRecipients(),
            payload: {
                title:
                    status === "complete"
                        ? "AI response ready"
                        : "AI turn stopped",
                body: body || `AI turn ${status} on ${channel.value.trim()}`,
                templateData: {
                    turn_id: turnId,
                    channel: channel.value.trim(),
                    status,
                    client_id: clientId.value.trim(),
                },
                collapseKey: `ai-${turnId}`,
            },
            sync: true,
        },
        "sync",
    );
    lastPushResult.value = res;
    if (!apiOk(res)) {
        const error = responseError(res, "push publish");
        lastNotice.value = error
            ? `AI completed; push publish reported: ${error}`
            : "AI completed; push publish did not return success.";
    }
}

async function sendMessage(textOverride?: string) {
    const text = (textOverride ?? composer.value).trim();
    if (!text || sending.value) return;
    lastError.value = "";
    lastNotice.value = "";
    sending.value = true;

    composer.value = "";
    await scrollToBottom();

    try {
        const transport = await ensureSubscribed();
        const userMessage: AI.UIMessage = {
            id: `user-${now()}-${randomSuffix()}`,
            role: "user",
            parts: [{ type: "text", text }],
        };

        if (agentMode.value === "external") {
            await transport.view.send(userMessage, {
                waitForTurnStart: false,
                body: { mode: "external", clientId: clientId.value.trim() },
            });
            lastNotice.value = "Waiting for an agent response...";
            sending.value = false;
            syncMessagesFromTransport();
            return;
        }

        const turn = (await transport.view.send(userMessage, {
            body: {
                mode: agentMode.value,
                model: ollamaModel.value.trim(),
                temperature: Number(ollamaTemperature.value) || 0.2,
                clientId: clientId.value.trim(),
            },
        })) as ActiveTurn<VercelOutput>;
        activeTurn = turn;
        const run = {
            turnId: turn.turnId,
            assistantId: `assistant-${turn.turnId}`,
            cancelled: false,
        };
        activeRun.value = run;
        const turnDone = transport.waitForTurn({ turnId: turn.turnId });
        let finalContent = "";
        let streamError: Error | null = null;
        const streamDone = consumeTurnStream(turn.stream, turnDone)
            .then((content) => {
                finalContent = content;
            })
            .catch((error: Error) => {
                streamError = error;
            });

        await turnDone;
        await Promise.race([streamDone, sleep(250)]);
        if (streamError && !run.cancelled) throw streamError;
        syncMessagesFromTransport();
        const finalStatus = run.cancelled ? "cancelled" : "complete";
        await publishAiPushNotification(
            turn.turnId,
            finalStatus,
            finalContent || (activeAssistant.value?.content ?? ""),
        );
    } catch (error: any) {
        if (!activeRun.value?.cancelled) {
            lastError.value = error.message ?? "The chat turn failed.";
            syncMessagesFromTransport();
        }
    } finally {
        sending.value = false;
        activeTurn = null;
        if (agentMode.value !== "external") activeRun.value = null;
        await scrollToBottom();
    }
}

async function cancelTurn() {
    const run = activeRun.value;
    if (!run || cancelling.value) return;
    cancelling.value = true;
    run.cancelled = true;
    lastNotice.value = "Stopping...";

    try {
        await activeTurn?.cancel();
        if (!activeTurn && aiTransport)
            await aiTransport.cancel({ turnId: run.turnId });
        await cancelServerTurn(run.turnId);
        messages.value = messages.value.map((message) =>
            message.turnId === run.turnId && message.role === "assistant"
                ? { ...message, status: "cancelled", updatedAtMs: now() }
                : message,
        );
        if (!sending.value) activeRun.value = null;
        lastNotice.value = "";
    } catch (error: any) {
        lastError.value = error.message ?? "Could not stop the turn.";
    } finally {
        cancelling.value = false;
    }
}

async function retryAssistant(message: ChatMessage) {
    const user = messages.value.find((entry) => entry.id === message.replyToId);
    if (!user) return;
    await sendMessage(user.content);
}

async function newChat() {
    if (activeRun.value) await cancelTurn();
    await closeAiTransport();
    messages.value = [];
    lastError.value = "";
    lastNotice.value = "";
}

function normalizeHistoryItem(item: any) {
    const envelope = item?.message ?? {};
    const message = envelope?.message ?? envelope;
    const event = item?.event_name ?? message?.event ?? envelope?.event;
    const extras = message?.extras ?? envelope?.extras;
    const data = message?.data ?? envelope?.data;
    const role = getTransportRole(extras);
    const turnId = getTurnId(
        extras,
        String(turnIdFromData(data) ?? item?.message_id ?? ""),
    );
    const name = message?.name ?? envelope?.name;
    const serial =
        envelope?.message_serial ?? envelope?.messageSerial ?? item?.message_id;

    if (
        event === "ai-input" ||
        name === "ai-input" ||
        (role === "user" && hasDisplayText(data))
    ) {
        return chatMessage("user", textFromData(data), "complete", {
            id: `user-${turnId ?? item.serial}`,
            turnId,
            messageId: item?.message_id,
            messageSerial: serial ? String(serial) : undefined,
            createdAtMs: item?.published_at_ms ?? now(),
        });
    }

    if (name === "ai-output" || role === "assistant" || Boolean(extras?.ai)) {
        return chatMessage(
            "assistant",
            textFromData(data),
            getTransportStatus(extras),
            {
                id: `assistant-${serial ?? item.serial}`,
                turnId,
                messageSerial: serial ? String(serial) : undefined,
                messageId: item?.message_id,
                replyToId: turnId ? `user-${turnId}` : undefined,
                createdAtMs: item?.published_at_ms ?? now(),
            },
        );
    }

    return null;
}

async function loadHistory() {
    if (!channel.value.trim() || loadingHistory.value) return;
    loadingHistory.value = true;
    lastError.value = "";
    lastNotice.value = "";

    try {
        const transport = await ensureAiTransport();
        await transport.view.loadOlder(100);
        syncMessagesFromTransport();
        lastNotice.value = messages.value.length
            ? `Recovered ${messages.value.length} messages.`
            : "No retained chat history.";
    } catch (error: any) {
        lastError.value = error.message ?? "Could not load chat history.";
    } finally {
        loadingHistory.value = false;
        await scrollToBottom();
    }
}

function formatTime(ms: number) {
    return new Date(ms).toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
    });
}

watch(messages, scrollToBottom, { deep: true });
watch(channel, (next) => {
    pushChannel.value = next;
    void closeAiTransport();
});
watch(
    () => store.socketId,
    () => {
        boundChannels.clear();
        void closeAiTransport();
        if (isConnected.value && channel.value.trim()) {
            ensureSubscribed().catch(() => {});
        }
    },
);

onMounted(async () => {
    try {
        checkOllama().catch(() => {});
        if (isConnected.value) await ensureSubscribed();
        await loadHistory();
    } catch {
        // The visible chat state already reports connection/history errors.
    }
});
</script>

<template>
    <div
        class="h-[calc(100vh-4rem)] min-h-[620px] animate-fade-in flex flex-col"
    >
        <div class="panel flex-1 min-h-0 overflow-hidden flex flex-col">
            <header
                class="px-4 py-3 border-b border-surface-700/40 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between"
            >
                <div class="flex items-center gap-3 min-w-0">
                    <div
                        class="w-9 h-9 rounded-lg bg-brand-500/15 text-brand-300 flex items-center justify-center"
                    >
                        <Bot class="w-5 h-5" />
                    </div>
                    <div class="min-w-0">
                        <h2 class="text-sm font-semibold text-surface-50">
                            AI Chat
                        </h2>
                        <div
                            class="flex items-center gap-2 text-xs text-surface-500"
                        >
                            <span
                                :class="[
                                    'w-2 h-2 rounded-full',
                                    isConnected
                                        ? 'bg-emerald-400'
                                        : 'bg-surface-500',
                                ]"
                            />
                            <span>{{
                                isConnected
                                    ? "connected"
                                    : store.connectionState
                            }}</span>
                            <span class="text-surface-700">/</span>
                            <span class="font-mono truncate max-w-[220px]">{{
                                channel
                            }}</span>
                        </div>
                    </div>
                </div>

                <div class="flex items-center gap-2">
                    <div class="flex gap-1 p-1 bg-surface-800/70 rounded-lg">
                        <button
                            v-for="mode in [
                                'ollama',
                                'demo',
                                'external',
                            ] as const"
                            :key="mode"
                            @click="agentMode = mode"
                            :class="[
                                agentMode === mode
                                    ? 'tab-btn-active'
                                    : 'tab-btn',
                            ]"
                        >
                            {{ modeLabel(mode) }}
                        </button>
                    </div>
                    <button
                        @click="showSettings = !showSettings"
                        class="btn-icon"
                        title="Chat settings"
                    >
                        <Settings2 class="w-4 h-4" />
                    </button>
                    <button
                        @click="loadHistory"
                        :disabled="loadingHistory"
                        class="btn-icon"
                        title="Recover history"
                    >
                        <RefreshCcw
                            :class="[
                                'w-4 h-4',
                                loadingHistory ? 'animate-spin' : '',
                            ]"
                        />
                    </button>
                    <button @click="newChat" class="btn-icon" title="New chat">
                        <Trash2 class="w-4 h-4" />
                    </button>
                </div>
            </header>

            <div
                v-if="showSettings"
                class="px-4 py-3 border-b border-surface-700/40 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-6 gap-3 bg-surface-950/25"
            >
                <div>
                    <label class="text-[10px] text-surface-500 mb-1 block"
                        >Channel</label
                    >
                    <input
                        v-model="channel"
                        class="input-field font-mono text-xs"
                    />
                </div>
                <div>
                    <label class="text-[10px] text-surface-500 mb-1 block"
                        >Client ID</label
                    >
                    <input
                        v-model="clientId"
                        class="input-field font-mono text-xs"
                    />
                </div>
                <div>
                    <label class="text-[10px] text-surface-500 mb-1 block"
                        >Rollup</label
                    >
                    <select
                        v-model.number="store.config.appendRollupWindow"
                        class="input-field font-mono text-xs"
                    >
                        <option :value="0">0 ms</option>
                        <option :value="20">20 ms</option>
                        <option :value="40">40 ms</option>
                        <option :value="100">100 ms</option>
                        <option :value="500">500 ms</option>
                    </select>
                </div>
                <div>
                    <label class="text-[10px] text-surface-500 mb-1 block"
                        >Append Mode</label
                    >
                    <select
                        v-model="store.config.appendMode"
                        class="input-field font-mono text-xs"
                    >
                        <option value="delta">delta</option>
                        <option value="full">full</option>
                    </select>
                </div>
                <div>
                    <label class="text-[10px] text-surface-500 mb-1 block"
                        >Ollama Model</label
                    >
                    <input
                        v-model="ollamaModel"
                        class="input-field font-mono text-xs"
                    />
                </div>
                <div>
                    <label class="text-[10px] text-surface-500 mb-1 block"
                        >Temperature</label
                    >
                    <input
                        v-model.number="ollamaTemperature"
                        type="number"
                        min="0"
                        max="1"
                        step="0.1"
                        class="input-field font-mono text-xs"
                    />
                </div>
                <div class="space-y-1">
                    <label class="text-[10px] text-surface-500 block"
                        >Ollama</label
                    >
                    <button
                        @click="checkOllama"
                        :disabled="checkingOllama"
                        class="btn-secondary btn-sm w-full flex items-center justify-center gap-2"
                    >
                        <Loader2
                            v-if="checkingOllama"
                            class="w-3.5 h-3.5 animate-spin"
                        />
                        <Bot v-else class="w-3.5 h-3.5" />
                        Check
                    </button>
                    <p
                        class="text-[10px] text-surface-500 truncate"
                        :title="ollamaStatus"
                    >
                        {{ ollamaStatus }}
                    </p>
                </div>
                <div
                    class="xl:col-span-2 space-y-2 rounded-lg border border-surface-700/35 bg-surface-800/30 p-3"
                >
                    <label
                        class="flex items-center gap-2 text-xs text-surface-300"
                    >
                        <input
                            v-model="pushOnComplete"
                            type="checkbox"
                            class="accent-brand-500"
                        />
                        Publish push after each AI turn
                    </label>
                    <div class="grid grid-cols-2 gap-2">
                        <select
                            v-model="pushTarget"
                            class="input-field font-mono text-xs"
                        >
                            <option value="channel">channel</option>
                            <option value="client">client</option>
                            <option value="device">device</option>
                        </select>
                        <input
                            v-if="pushTarget === 'channel'"
                            v-model="pushChannel"
                            class="input-field font-mono text-xs"
                            placeholder="channel"
                        />
                        <input
                            v-else-if="pushTarget === 'client'"
                            v-model="clientId"
                            class="input-field font-mono text-xs"
                            placeholder="client id"
                        />
                        <input
                            v-else
                            v-model="pushDeviceId"
                            class="input-field font-mono text-xs"
                            placeholder="device id"
                        />
                    </div>
                </div>
            </div>

            <div v-if="lastError || lastNotice" class="px-4 pt-3">
                <div
                    :class="[
                        'rounded-lg border px-3 py-2 text-xs',
                        lastError
                            ? 'bg-red-500/10 border-red-500/25 text-red-300'
                            : 'bg-brand-500/10 border-brand-500/25 text-brand-300',
                    ]"
                >
                    {{ lastError || lastNotice }}
                </div>
            </div>

            <div
                ref="scrollEl"
                class="flex-1 min-h-0 overflow-y-auto px-4 py-5"
            >
                <div
                    v-if="messages.length === 0"
                    class="h-full flex items-center justify-center"
                >
                    <div class="max-w-xl w-full text-center space-y-4">
                        <div
                            class="w-12 h-12 mx-auto rounded-xl bg-brand-500/15 text-brand-300 flex items-center justify-center"
                        >
                            <MessageSquarePlus class="w-6 h-6" />
                        </div>
                        <div>
                            <h3
                                class="text-base font-semibold text-surface-100"
                            >
                                Start an Ollama-backed AI Transport chat
                            </h3>
                            <p class="text-sm text-surface-500 mt-1">
                                Pick a prompt or type your own.
                            </p>
                        </div>
                        <div class="grid grid-cols-1 gap-2">
                            <button
                                v-for="prompt in emptyPrompts"
                                :key="prompt"
                                @click="sendMessage(prompt)"
                                :disabled="sending"
                                class="text-left rounded-lg border border-surface-700/40 bg-surface-800/50 hover:bg-surface-800 px-3 py-2 text-sm text-surface-300 transition-colors"
                            >
                                {{ prompt }}
                            </button>
                        </div>
                    </div>
                </div>

                <div v-else class="space-y-4">
                    <div
                        v-for="message in messages"
                        :key="message.id"
                        :class="[
                            'flex gap-3',
                            message.role === 'user'
                                ? 'justify-end'
                                : 'justify-start',
                        ]"
                    >
                        <div
                            v-if="message.role !== 'user'"
                            class="w-8 h-8 rounded-lg bg-brand-500/15 text-brand-300 flex items-center justify-center shrink-0 mt-1"
                        >
                            <Bot class="w-4 h-4" />
                        </div>
                        <div
                            :class="[
                                'max-w-[78%] space-y-1',
                                message.role === 'user'
                                    ? 'items-end'
                                    : 'items-start',
                            ]"
                        >
                            <div
                                :class="[
                                    'rounded-xl px-4 py-3 text-sm leading-relaxed',
                                    message.role === 'user'
                                        ? 'bg-brand-600 text-white'
                                        : message.status === 'error'
                                          ? 'bg-red-500/10 text-red-200 border border-red-500/25'
                                          : 'bg-surface-800/80 text-surface-100 border border-surface-700/35',
                                ]"
                            >
                                <div
                                    v-if="message.content"
                                    :class="[
                                        'markdown-content',
                                        message.role === 'user'
                                            ? 'markdown-content-user'
                                            : '',
                                    ]"
                                    v-html="renderMarkdown(message.content)"
                                />
                                <span
                                    v-else
                                    class="inline-flex items-center gap-2 text-surface-400"
                                >
                                    <Loader2 class="w-3.5 h-3.5 animate-spin" />
                                    Thinking
                                </span>
                            </div>
                            <div
                                :class="[
                                    'flex items-center gap-2 text-[10px] text-surface-500',
                                    message.role === 'user'
                                        ? 'justify-end'
                                        : 'justify-start',
                                ]"
                            >
                                <span>{{
                                    formatTime(message.createdAtMs)
                                }}</span>
                                <span
                                    v-if="message.status !== 'complete'"
                                    :class="
                                        message.status === 'error'
                                            ? 'text-red-400'
                                            : message.status === 'cancelled'
                                              ? 'text-amber-400'
                                              : 'text-brand-400'
                                    "
                                >
                                    {{ statusLabel(message.status) }}
                                </span>
                                <button
                                    v-if="
                                        message.role === 'assistant' &&
                                        (message.status === 'error' ||
                                            message.status === 'cancelled')
                                    "
                                    @click="retryAssistant(message)"
                                    class="inline-flex items-center gap-1 text-brand-300 hover:text-brand-200"
                                >
                                    <RotateCcw class="w-3 h-3" /> Retry
                                </button>
                            </div>
                            <p
                                v-if="message.error"
                                class="text-[11px] text-red-300"
                            >
                                {{ message.error }}
                            </p>
                        </div>
                        <div
                            v-if="message.role === 'user'"
                            class="w-8 h-8 rounded-lg bg-surface-700 text-surface-200 flex items-center justify-center shrink-0 mt-1"
                        >
                            {{ clientId.charAt(0).toUpperCase() || "U" }}
                        </div>
                    </div>
                </div>
            </div>

            <footer class="border-t border-surface-700/40 p-4">
                <div class="flex items-end gap-2">
                    <button
                        v-if="!isConnected"
                        @click="connect"
                        class="btn-secondary flex items-center gap-2"
                    >
                        <Wifi class="w-4 h-4" /> Connect
                    </button>
                    <textarea
                        v-model="composer"
                        class="input-field min-h-[48px] max-h-[140px] resize-none text-sm"
                        placeholder="Message Sockudo AI..."
                        :disabled="sending"
                        @keydown.enter.exact.prevent="sendMessage()"
                    />
                    <button
                        v-if="activeRun"
                        @click="cancelTurn"
                        :disabled="cancelling"
                        class="btn-secondary h-12 w-12 flex items-center justify-center"
                        title="Stop"
                    >
                        <Square class="w-4 h-4" />
                    </button>
                    <button
                        v-else
                        @click="sendMessage()"
                        :disabled="!composer.trim() || sending"
                        class="btn-primary h-12 w-12 flex items-center justify-center"
                        title="Send"
                    >
                        <Send class="w-4 h-4" />
                    </button>
                </div>
                <div
                    class="mt-2 flex items-center justify-between text-[10px] text-surface-600"
                >
                    <span class="inline-flex items-center gap-1">
                        <Bell
                            v-if="pushOnComplete"
                            class="w-3 h-3 text-pink-400"
                        />
                        {{ modeHint() }}
                    </span>
                    <span
                        v-if="isBusy"
                        class="inline-flex items-center gap-1 text-brand-400"
                    >
                        <Loader2 class="w-3 h-3 animate-spin" /> active
                    </span>
                    <span
                        v-else
                        class="inline-flex items-center gap-1 text-emerald-400"
                    >
                        <CheckCircle2 class="w-3 h-3" /> ready
                    </span>
                </div>
                <div
                    v-if="lastPushResult"
                    class="mt-1 text-[10px] text-surface-600"
                >
                    Push publish:
                    <span
                        class="font-mono"
                        :class="
                            apiOk(lastPushResult)
                                ? 'text-emerald-400'
                                : 'text-amber-400'
                        "
                        >HTTP {{ lastPushResult.status }}</span
                    >
                </div>
            </footer>
        </div>
    </div>
</template>
