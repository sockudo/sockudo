<script setup lang="ts">
import { Chat } from "@ai-sdk/vue";
import {
  provideChatTransport,
  useActiveTurns,
  useCreateView,
  useSockudoMessages,
  useTree,
  useView,
} from "@sockudo/ai-transport/vercel/vue";
import type { AI } from "@sockudo/ai-transport/vercel";
import type { ChatTransport as AiSdkChatTransport, UIMessage } from "ai";
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { createBrowserSockudoClient } from "~/lib/browser-proxy";

interface DemoConfig {
  appKey: string;
  channelName: string;
  host: string;
  model: string;
  port: number;
  usingGatewayKey: boolean;
}

interface PresencePeer {
  id: string;
  data: unknown;
}

const config = await $fetch<DemoConfig>("/api/config");
const clientId = `nuxt-${crypto.randomUUID().slice(0, 8)}`;
const sessionChannel = useCookie<string>("sockudo-ai-demo-channel", {
  default: () => `private-ai-e2e-${crypto.randomUUID().slice(0, 8)}`,
  sameSite: "lax",
});
const channelName = sessionChannel.value;
const { raw, realtime } = createBrowserSockudoClient(config, clientId);
// @docs-snippet usechat-provider
const transportProvider = provideChatTransport({
  api: "/api/chat",
  channelName,
  client: realtime,
  clientId,
  chatOptions: {
    prepareSendMessagesRequest(context) {
      return {
        headers: {
          "x-demo-client-id": clientId,
          "x-demo-trigger": context.trigger,
        },
        body: {
          channelName,
          demoHistoryCount: context.history.length,
          demoMessageCount: context.messages.length,
          demoParent: context.parent,
          demoForkOf: context.forkOf,
        },
      };
    },
  },
});
// @docs-snippet-end

if (!transportProvider.chatTransport.value || !transportProvider.transport.value) {
  throw createError({
    statusCode: 500,
    statusMessage:
      transportProvider.chatTransportError.value?.message ??
      transportProvider.transportError.value?.message ??
      "Sockudo AI Transport failed to initialize",
  });
}

const chat = reactive(
  new Chat<UIMessage>({
    id: channelName,
    transport: transportProvider.chatTransport.value as unknown as AiSdkChatTransport<UIMessage>,
    onError(error) {
      timeline.value.unshift({
        kind: "error",
        text: error.message,
        at: new Date().toLocaleTimeString(),
      });
    },
    onFinish({ finishReason }) {
      timeline.value.unshift({
        kind: "finish",
        text: `assistant finished: ${finishReason ?? "complete"}`,
        at: new Date().toLocaleTimeString(),
      });
    },
    onToolCall({ toolCall }) {
      timeline.value.unshift({
        kind: "tool",
        text: `tool call observed: ${"toolName" in toolCall ? String(toolCall.toolName) : "dynamic"}`,
        at: new Date().toLocaleTimeString(),
      });
    },
  }),
) as Chat<UIMessage>;

const transport = transportProvider.transport.value;
// @docs-snippet core-branch-views
const primaryView = useView({ transport });
const compareView = useCreateView({ transport });
const rawMessages = useSockudoMessages({ transport });
const activeTurns = useActiveTurns({ transport });
const tree = useTree({ transport });
const channel = realtime.channels.get(channelName, {
  params: { rewind: { count: 100 } },
});
// @docs-snippet-end

const prompt = ref(
  "Show a realtime incident-room assistant that streams tokens, reasons briefly, uses a tool, supports cancellation, and survives refresh through Sockudo history.",
);
const editPrompt = ref("Rewrite the last user request as a sharper production-debugging prompt.");
const model = ref(config.model);
const selectedDemo = ref("capability-scan");
const composer = ref<HTMLElement | null>(null);
const messagesPanel = ref<HTMLElement | null>(null);
const presencePeers = ref<PresencePeer[]>([]);
const selectedMessageId = ref<string | undefined>();
const historyFallback = ref<{ prompt: string; message: UIMessage } | undefined>();
const timeline = ref<
  Array<{
    kind: "send" | "error" | "finish" | "tool" | "history" | "presence";
    text: string;
    at: string;
  }>
>([]);

const messages = computed(() => safeArray(chat.messages));
const viewMessages = computed(() => safeArray(primaryView.messages.value));
const allConversationMessages = computed<UIMessage[]>(() =>
  viewMessages.value.length > 0 ? (viewMessages.value as UIMessage[]) : messages.value,
);
const transportConversationMessages = computed<UIMessage[]>(() =>
  allConversationMessages.value.filter(shouldShowInChat),
);
const offlinePreviewMessage = computed<UIMessage | undefined>(() => {
  if (config.usingGatewayKey) {
    return undefined;
  }
  const lastUserIndex = findLastUserIndex(allConversationMessages.value);
  if (lastUserIndex < 0) {
    return undefined;
  }
  const hasReadableAssistant = allConversationMessages.value
    .slice(lastUserIndex + 1)
    .some((message) => message.role === "assistant" && hasTextPart(message));
  if (hasReadableAssistant) {
    return undefined;
  }
  const userMessage = allConversationMessages.value[lastUserIndex];
  const text = demoAnswerForPrompt(userMessage ? messageText(userMessage) : "");
  return {
    id: `offline-preview-${userMessage?.id ?? "latest"}`,
    role: "assistant",
    parts: [
      { type: "text", text },
      {
        type: "source-url",
        sourceId: "vercel-ai-gateway-docs",
        title: "Vercel AI Gateway",
        url: "https://vercel.com/docs/ai-gateway",
      },
    ],
  };
});
const transportFallbackMessage = computed<UIMessage | undefined>(() => {
  const lastUserIndex = findLastUserIndex(allConversationMessages.value);
  if (lastUserIndex < 0) {
    return undefined;
  }
  const userMessage = allConversationMessages.value[lastUserIndex];
  const text = userMessage ? messageText(userMessage) : "";
  return (
    assistantFromRawTransport(text) ??
    (historyFallback.value?.prompt === text ? historyFallback.value.message : undefined)
  );
});
const conversationMessages = computed<UIMessage[]>(() =>
  withReadableAssistantFallback(
    transportConversationMessages.value.slice(-6),
    transportFallbackMessage.value ?? offlinePreviewMessage.value,
  ),
);
const compareMessages = computed(() => safeArray(compareView.messages.value));
const status = computed(() => chat.status);
const rawRecent = computed(() => safeArray(rawMessages.value).slice(-24).reverse());
const activeTurnCount = computed(() =>
  Array.from((activeTurns.value ?? new Map()).values()).reduce(
    (count, turns) => count + turns.size,
    0,
  ),
);
const activeTurnRows = computed(() =>
  Array.from((activeTurns.value ?? new Map()).entries()).flatMap(([owner, turns]) =>
    Array.from(turns).map((turnId) => ({ owner, turnId })),
  ),
);
const lastAssistantId = computed(() => {
  for (let index = conversationMessages.value.length - 1; index >= 0; index -= 1) {
    const message = conversationMessages.value[index];
    if (message?.role === "assistant") {
      return message.id;
    }
  }
  return undefined;
});
const lastUserId = computed(() => {
  for (let index = conversationMessages.value.length - 1; index >= 0; index -= 1) {
    const message = conversationMessages.value[index];
    if (message?.role === "user") {
      return message.id;
    }
  }
  return undefined;
});
const selectedNode = computed(() =>
  selectedMessageId.value === undefined ? undefined : tree.getNode(selectedMessageId.value),
);
const selectedSiblings = computed(() =>
  selectedMessageId.value === undefined ? [] : tree.getSiblings(selectedMessageId.value),
);
const capabilityRows = computed(() => [
  {
    name: "Vercel AI SDK chat transport",
    detail: "Chat class sends through Sockudo ChatTransport",
    active: Boolean(transportProvider.chatTransport.value),
  },
  {
    name: "Token and reasoning streaming",
    detail: "UI chunks are encoded as mutable message append streams",
    active: rawMessages.value.some((message) => message.action === "update"),
  },
  {
    name: "Tool calls and human approval",
    detail: "Dynamic tool chunks, approval responses, and tool outputs are folded into the view",
    active: messages.value.some((message) =>
      messageParts(message).some((part) => part.type === "dynamic-tool"),
    ),
  },
  {
    name: "Branch, edit, regenerate",
    detail: "Branch metadata is derived by the transport and visible in the tree panel",
    active: viewMessages.value.length > 1,
  },
  {
    name: "History and recovery",
    detail: "Load older replays channel history through Sockudo durable history",
    active: rawMessages.value.length > 0,
  },
  {
    name: "Multi-device presence",
    detail: "Presence enter/update/leave runs on the same channel",
    active: presencePeers.value.length > 0,
  },
  {
    name: "Cancellation",
    detail: "Stop publishes a scoped AI cancel event and aborts the local stream",
    active:
      activeTurnCount.value > 0 || timeline.value.some((event) => event.text.includes("cancel")),
  },
]);

// @docs-snippet usechat-sync
watch(
  () => primaryView.messages.value,
  (next) => {
    const nextMessages = safeArray(next);
    if (nextMessages.length > messages.value.length && chat.status !== "streaming") {
      chat.messages = [...nextMessages] as typeof chat.messages;
    }
  },
);
// @docs-snippet-end

watch(
  () =>
    conversationMessages.value
      .map((message) => `${message.id}:${messageText(message)}`)
      .join("|"),
  async () => {
    await nextTick();
    if (messagesPanel.value) {
      messagesPanel.value.scrollTop = messagesPanel.value.scrollHeight;
    }
  },
  { flush: "post" },
);

onMounted(async () => {
  channel.presence.subscribe((_event, member) => {
    void refreshPresence();
    timeline.value.unshift({
      kind: "presence",
      text: `presence ${member.id}`,
      at: new Date().toLocaleTimeString(),
    });
  });
  try {
    await channel.presence.enter({
      clientId,
      surface: "nuxt-demo",
      model: model.value,
    });
    await refreshPresence();
    setTimeout(() => {
      void refreshHistoryFallback();
    }, 1500);
  } catch (error) {
    timeline.value.unshift({
      kind: "error",
      text: `presence unavailable: ${error instanceof Error ? error.message : String(error)}`,
      at: new Date().toLocaleTimeString(),
    });
  }
});

onBeforeUnmount(() => {
  void channel.presence.leave({ clientId });
  raw.disconnect();
});

async function sendPrompt(text = prompt.value): Promise<void> {
  const trimmed = text.trim();
  if (!trimmed) {
    return;
  }
  timeline.value.unshift({
    kind: "send",
    text: `sent: ${trimmed.slice(0, 80)}`,
    at: new Date().toLocaleTimeString(),
  });
  await chat.sendMessage(
    { text: trimmed },
    {
      body: {
        model: model.value,
        demo: selectedDemo.value,
      },
    },
  );
  void refreshHistoryFallback(trimmed, 12);
  prompt.value = "";
  await nextTick();
  composer.value?.focus();
}

async function stopTurn(): Promise<void> {
  await Promise.allSettled([chat.stop(), transport.cancel({ own: true })]);
  timeline.value.unshift({
    kind: "send",
    text: "cancel requested for owned active turn",
    at: new Date().toLocaleTimeString(),
  });
}

async function regenerateLast(): Promise<void> {
  const target = lastAssistantId.value;
  if (!target) {
    timeline.value.unshift({
      kind: "error",
      text: "no assistant message to regenerate",
      at: new Date().toLocaleTimeString(),
    });
    return;
  }
  const parent = previousMessageId(target) ?? lastUserId.value ?? target;
  try {
    await regenerateFromView(target, parent, {
      body: { model: model.value },
    });
    timeline.value.unshift({
      kind: "send",
      text: `regenerate requested for ${shortId(target)}`,
      at: new Date().toLocaleTimeString(),
    });
  } catch (error) {
    timeline.value.unshift({
      kind: "error",
      text: `regenerate failed: ${error instanceof Error ? error.message : String(error)}`,
      at: new Date().toLocaleTimeString(),
    });
  }
}

async function editLastUser(): Promise<void> {
  if (!lastUserId.value || !editPrompt.value.trim()) {
    return;
  }
  const replacement = makeUserMessage(editPrompt.value.trim());
  replacement.id = lastUserId.value;
  try {
    await editFromView(lastUserId.value, replacement as unknown as AI.UIMessage, {
      body: { model: model.value },
    });
    timeline.value.unshift({
      kind: "send",
      text: `edited: ${editPrompt.value.trim().slice(0, 80)}`,
      at: new Date().toLocaleTimeString(),
    });
  } catch (error) {
    timeline.value.unshift({
      kind: "error",
      text: `edit failed: ${error instanceof Error ? error.message : String(error)}`,
      at: new Date().toLocaleTimeString(),
    });
  }
}

function regenerateFromView(
  target: string,
  parent: string,
  options: { body: Record<string, unknown> },
): Promise<unknown> {
  return (
    primaryView.regenerate as unknown as (
      target: string,
      parent: string,
      options?: { body: Record<string, unknown> },
    ) => Promise<unknown>
  )(target, parent, options);
}

function editFromView(
  messageId: string,
  message: AI.UIMessage,
  options: { body: Record<string, unknown> },
): Promise<unknown> {
  return (
    primaryView.edit as unknown as (
      messageId: string,
      message: AI.UIMessage,
      options?: { body: Record<string, unknown> },
    ) => Promise<unknown>
  )(messageId, message, options);
}

async function doubleText(): Promise<void> {
  const first = makeUserMessage("Double-text A: start an investigation branch.");
  const second = makeUserMessage("Double-text B: add a concurrent follow-up before A finishes.");
  void primaryView.send(first as unknown as AI.UIMessage);
  await new Promise((resolve) => setTimeout(resolve, 120));
  void primaryView.send(second as unknown as AI.UIMessage);
}

async function loadOlder(): Promise<void> {
  await Promise.all([primaryView.loadOlder(80), compareView.loadOlder(40)]);
  timeline.value.unshift({
    kind: "history",
    text: "loaded older history into both views",
    at: new Date().toLocaleTimeString(),
  });
}

async function approveFirstTool(): Promise<void> {
  const tool = firstDynamicTool();
  if (!tool) {
    timeline.value.unshift({
      kind: "tool",
      text: "no pending dynamic tool part found yet",
      at: new Date().toLocaleTimeString(),
    });
    return;
  }
  await chat.addToolApprovalResponse({
    id: tool.toolCallId,
    approved: true,
  });
  await chat.addToolOutput({
    tool: tool.toolName,
    toolCallId: tool.toolCallId,
    output: {
      approvedBy: clientId,
      observedAt: new Date().toISOString(),
      message: "Approved from the Nuxt human-in-the-loop panel.",
    },
  } as unknown as Parameters<typeof chat.addToolOutput>[0]);
}

function selectMessage(message: UIMessage): void {
  selectedMessageId.value = message.id;
}

async function refreshPresence(): Promise<void> {
  try {
    presencePeers.value = [...(await channel.presence.get())];
  } catch {
    presencePeers.value = [];
  }
}

function usePreset(kind: string): void {
  selectedDemo.value = kind;
  const presets: Record<string, string> = {
    "capability-scan":
      "Run the full Sockudo AI Transport capability scan with reasoning, tool use, history replay notes, and multi-device guidance.",
    "branch-lab":
      "Create two possible remediation branches for a realtime fanout outage and explain how edit/regenerate should preserve the tree.",
    "recovery-drill":
      "Simulate a reconnect after network loss and explain what durable history and continuity cursors recover.",
    "human-loop":
      "Ask for human approval before running a diagnostic tool, then continue once the tool result is available.",
  };
  prompt.value = presets[kind] ?? presets["capability-scan"]!;
}

function partText(part: AI.UIMessagePart): string {
  if (part.type === "text" || part.type === "reasoning") {
    return part.text;
  }
  if (part.type === "dynamic-tool") {
    return `${toolLabel(part)} ${toolStateLabel(part)}`;
  }
  if (part.type === "source-url") {
    return part.title ?? part.url;
  }
  if (part.type.startsWith("data-") && "data" in part) {
    return JSON.stringify(part.data);
  }
  return part.type;
}

function messageText(message: UIMessage): string {
  return readableParts(message)
    .map((part) => partText(part))
    .join(" ");
}

function messageParts(message: UIMessage | AI.UIMessage): AI.UIMessagePart[] {
  const parts = (message as { parts?: unknown }).parts;
  return Array.isArray(parts) ? (parts as AI.UIMessagePart[]) : [];
}

function readableParts(message: UIMessage | AI.UIMessage): AI.UIMessagePart[] {
  return messageParts(message).filter(isReadablePart).sort(readablePartSort);
}

function isReadablePart(part: AI.UIMessagePart): boolean {
  if ((part.type === "text" || part.type === "reasoning") && part.text.trim().length === 0) {
    return false;
  }
  return (
    part.type === "text" ||
    part.type === "reasoning" ||
    part.type === "dynamic-tool" ||
    part.type === "source-url"
  );
}

function readablePartSort(left: AI.UIMessagePart, right: AI.UIMessagePart): number {
  return readablePartPriority(left) - readablePartPriority(right);
}

function readablePartPriority(part: AI.UIMessagePart): number {
  if (part.type === "text") {
    return 0;
  }
  if (part.type === "reasoning") {
    return 1;
  }
  if (part.type === "dynamic-tool") {
    return 2;
  }
  if (part.type === "source-url") {
    return 3;
  }
  return 4;
}

function shouldShowInChat(message: UIMessage): boolean {
  if (message.role === "user") {
    return true;
  }
  return hasTextPart(message);
}

function hasTextPart(message: UIMessage): boolean {
  return readableParts(message).some((part) => part.type === "text");
}

function findLastUserIndex(items: readonly UIMessage[]): number {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    if (items[index]?.role === "user") {
      return index;
    }
  }
  return -1;
}

function withReadableAssistantFallback(
  visibleMessages: UIMessage[],
  fallback: UIMessage | undefined,
): UIMessage[] {
  if (!fallback) {
    return visibleMessages;
  }
  const lastUserIndex = findLastUserIndex(visibleMessages);
  const hasVisibleAssistant =
    lastUserIndex >= 0 &&
    visibleMessages
      .slice(lastUserIndex + 1)
      .some((message) => message.role === "assistant" && hasTextPart(message));
  if (hasVisibleAssistant) {
    return visibleMessages;
  }
  const tail = lastUserIndex >= 0 ? visibleMessages.slice(lastUserIndex) : visibleMessages;
  return [...tail, fallback].slice(-6);
}

function previousMessageId(messageId: string): string | undefined {
  const items = conversationMessages.value;
  const index = items.findIndex((message) => message.id === messageId);
  return index > 0 ? items[index - 1]?.id : undefined;
}

function demoAnswerForPrompt(text: string): string {
  return `Offline mode received: "${text || "your prompt"}". No real model is being called because AI_GATEWAY_API_KEY is not set for this Nuxt process. Set AI_GATEWAY_API_KEY, restart the demo, then send the prompt again to get a live Vercel AI Gateway response. The right-side panels still show that Sockudo carried the turn, raw frames, active state, history, branches, cancellation, and sync metadata.`;
}

function assistantFromRawTransport(promptText: string): UIMessage | undefined {
  const raw = safeArray(rawMessages.value) as Array<{
    name?: string;
    data?: unknown;
    getTransportHeaders?: () => Record<string, string>;
    getCodecHeaders?: () => Record<string, string>;
  }>;
  let turnId: string | undefined;
  for (let index = raw.length - 1; index >= 0; index -= 1) {
    const message = raw[index];
    const transport = message?.getTransportHeaders?.() ?? {};
    if (
      message?.name === "ai-input" &&
      dataText(message.data).includes(promptText) &&
      transport["turn-id"]
    ) {
      turnId = transport["turn-id"];
      break;
    }
  }
  if (!turnId) {
    return undefined;
  }
  for (let index = raw.length - 1; index >= 0; index -= 1) {
    const message = raw[index];
    const transport = message?.getTransportHeaders?.() ?? {};
    const codec = message?.getCodecHeaders?.() ?? {};
    if (
      message?.name === "ai-output" &&
      transport["turn-id"] === turnId &&
      codec.type?.startsWith("text-") &&
      typeof message.data === "string" &&
      message.data.trim().length > 0
    ) {
      return {
        id: codec["message-id"] ?? `raw-assistant-${turnId}`,
        role: "assistant",
        parts: [{ type: "text", text: message.data.trimStart() }],
      };
    }
  }
  return undefined;
}

async function refreshHistoryFallback(promptText?: string, attempts = 1): Promise<void> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    const fallback = await readHistoryFallback(promptText);
    if (fallback) {
      historyFallback.value = fallback;
      return;
    }
    if (attempt < attempts - 1) {
      await new Promise((resolve) => setTimeout(resolve, 900));
    }
  }
}

async function readHistoryFallback(
  promptText?: string,
): Promise<{ prompt: string; message: UIMessage } | undefined> {
  const payload = await $fetch<{ items?: unknown[] }>("/api/versioned-messages", {
    method: "POST",
    body: {
      action: "channel_history",
      channel: channelName,
      params: { limit: "100", direction: "newest_first" },
    },
  });
  const items = Array.isArray(payload.items) ? payload.items : [];
  const target = findHistoryPrompt(items, promptText);
  if (!target) {
    return undefined;
  }
  const text = findHistoryAssistantText(items, target.turnId);
  if (!text) {
    return undefined;
  }
  return {
    prompt: target.prompt,
    message: {
      id: text.messageId,
      role: "assistant",
      parts: [{ type: "text", text: text.text }],
    },
  };
}

function findHistoryPrompt(
  items: readonly unknown[],
  promptText?: string,
): { prompt: string; turnId: string } | undefined {
  for (const item of items) {
    const message = historyMessage(item);
    const transport = historyTransport(message);
    if (message.name !== "ai-input" && message.event !== "ai-input") {
      continue;
    }
    const prompt = promptFromHistoryData(message.data);
    if (!prompt || (promptText && prompt !== promptText) || !transport["turn-id"]) {
      continue;
    }
    return { prompt, turnId: transport["turn-id"] };
  }
  return undefined;
}

function findHistoryAssistantText(
  items: readonly unknown[],
  turnId: string,
): { messageId: string; text: string } | undefined {
  for (const item of items) {
    const message = historyMessage(item);
    const transport = historyTransport(message);
    const codec = historyCodec(message);
    if (
      (message.name === "ai-output" || message.event === "ai-output") &&
      transport["turn-id"] === turnId &&
      codec.type?.startsWith("text-") &&
      typeof message.data === "string" &&
      message.data.trim().length > 0
    ) {
      return {
        messageId: codec["message-id"] ?? `history-assistant-${turnId}`,
        text: message.data.trimStart(),
      };
    }
  }
  return undefined;
}

function promptFromHistoryData(value: unknown): string | undefined {
  const parsed = typeof value === "string" ? parseJson(value) : value;
  const parts = parsed && typeof parsed === "object" ? (parsed as { parts?: unknown }).parts : [];
  if (!Array.isArray(parts)) {
    return undefined;
  }
  const text = parts
    .map((part) =>
      part && typeof part === "object" && "text" in part
        ? String((part as { text?: unknown }).text ?? "")
        : "",
    )
    .join("")
    .trim();
  return text || undefined;
}

function historyMessage(item: unknown): Record<string, unknown> {
  const wrapper = item && typeof item === "object" ? (item as Record<string, unknown>) : {};
  const message = wrapper.message;
  return message && typeof message === "object" ? (message as Record<string, unknown>) : wrapper;
}

function historyTransport(message: Record<string, unknown>): Record<string, string> {
  return historyHeaders(message, "transport");
}

function historyCodec(message: Record<string, unknown>): Record<string, string> {
  return historyHeaders(message, "codec");
}

function historyHeaders(
  message: Record<string, unknown>,
  tier: "transport" | "codec",
): Record<string, string> {
  const extras = message.extras;
  const ai =
    extras && typeof extras === "object" ? (extras as { ai?: unknown }).ai : undefined;
  const headers =
    ai && typeof ai === "object"
      ? (ai as Record<string, unknown>)[tier]
      : undefined;
  if (!headers || typeof headers !== "object") {
    return {};
  }
  return Object.fromEntries(
    Object.entries(headers as Record<string, unknown>).map(([key, value]) => [
      key,
      String(value),
    ]),
  );
}

function parseJson(value: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function dataText(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return "";
  }
}

function toolLabel(part: Extract<AI.UIMessagePart, { type: "dynamic-tool" }>): string {
  if (part.toolName === "scanCapabilities") {
    return "Capability scan";
  }
  return part.toolName.replace(/([a-z])([A-Z])/g, "$1 $2");
}

function toolStateLabel(part: Extract<AI.UIMessagePart, { type: "dynamic-tool" }>): string {
  const labels: Record<string, string> = {
    "input-streaming": "collecting input",
    "input-available": "waiting for approval",
    "output-available": "completed",
    "output-error": "failed",
  };
  return labels[part.state] ?? part.state.replaceAll("-", " ");
}

function roleLabel(role: string): string {
  if (role === "user") {
    return "You";
  }
  if (role === "assistant") {
    return "Assistant";
  }
  return role;
}

function shortId(id: string): string {
  return id.length > 18 ? `${id.slice(0, 8)}...${id.slice(-6)}` : id;
}

function firstDynamicTool(): Extract<AI.UIMessagePart, { type: "dynamic-tool" }> | undefined {
  for (const message of conversationMessages.value) {
    for (const part of messageParts(message)) {
      if (part.type === "dynamic-tool") {
        return part as Extract<AI.UIMessagePart, { type: "dynamic-tool" }>;
      }
    }
  }
  return undefined;
}

function makeUserMessage(text: string): UIMessage {
  return {
    id: `user-${crypto.randomUUID()}`,
    role: "user",
    parts: [{ type: "text", text }],
  };
}

function safeArray<T>(value: readonly T[] | undefined): T[] {
  return Array.isArray(value) ? [...value] : [];
}
</script>

<template>
  <main class="app-shell">
    <section class="hero-band">
      <div class="hero-copy">
        <p class="eyebrow">Nuxt / Vue / Vercel AI SDK / Sockudo Protocol V2</p>
        <h1>Sockudo AI Transport Command Center</h1>
        <p class="hero-subtitle">
          A live, branch-aware AI transport lab for streaming, cancellation, history, tool
          approvals, presence, recovery, and multi-device fanout.
        </p>
      </div>
      <div class="status-rack" aria-label="transport status">
        <div>
          <span>Channel</span>
          <strong>{{ channelName }}</strong>
        </div>
        <div>
          <span>Client</span>
          <strong>{{ clientId }}</strong>
        </div>
        <div>
          <span>Gateway</span>
          <strong>{{ config.usingGatewayKey ? "live" : "missing API key" }}</strong>
        </div>
        <div>
          <span>Status</span>
          <strong>{{ status }} / {{ activeTurnCount }} active</strong>
        </div>
      </div>
    </section>

    <section class="workspace-grid">
      <aside class="control-rail">
        <div class="panel compact">
          <div class="panel-title">
            <span>Model</span>
            <strong>{{ model }}</strong>
          </div>
          <input v-model="model" class="text-input" aria-label="model id" />
          <div class="segmented">
            <button
              v-for="kind in ['capability-scan', 'branch-lab', 'recovery-drill', 'human-loop']"
              :key="kind"
              type="button"
              :class="{ active: selectedDemo === kind }"
              @click="usePreset(kind)"
            >
              {{ kind.replace("-", " ") }}
            </button>
          </div>
        </div>

        <div class="panel compact">
          <div class="panel-title">
            <span>Capabilities</span>
            <strong
              >{{ capabilityRows.filter((row) => row.active).length }}/{{
                capabilityRows.length
              }}</strong
            >
          </div>
          <ul class="capability-list">
            <li v-for="row in capabilityRows" :key="row.name" :class="{ lit: row.active }">
              <span class="dot" />
              <div>
                <strong>{{ row.name }}</strong>
                <small>{{ row.detail }}</small>
              </div>
            </li>
          </ul>
        </div>

        <div class="panel compact">
          <div class="panel-title">
            <span>Presence</span>
            <button
              type="button"
              class="small-button"
              title="Refresh presence"
              @click="refreshPresence"
            >
              Refresh
            </button>
          </div>
          <div class="presence-grid">
            <span v-for="peer in presencePeers" :key="peer.id">{{ peer.id }}</span>
          </div>
        </div>
      </aside>

      <section class="chat-column">
        <div class="chat-toolbar">
          <button type="button" data-testid="toolbar-send" @click="sendPrompt()">Send</button>
          <button type="button" data-testid="toolbar-stop" @click="stopTurn">Stop</button>
          <button type="button" data-testid="toolbar-regenerate" @click="regenerateLast">
            Regenerate
          </button>
          <button type="button" data-testid="toolbar-edit" @click="editLastUser">Edit last</button>
          <button type="button" data-testid="toolbar-double" @click="doubleText">Double text</button>
          <button type="button" data-testid="toolbar-approve" @click="approveFirstTool">
            Approve tool
          </button>
          <button type="button" data-testid="toolbar-history" @click="loadOlder">
            Load history
          </button>
        </div>

        <section ref="messagesPanel" class="messages-panel" aria-live="polite">
          <article
            v-for="message in conversationMessages"
            :key="message.id"
            class="message-row"
            :class="message.role"
            @click="selectMessage(message)"
          >
            <header>
              <span>{{ roleLabel(message.role) }}</span>
              <code>{{ shortId(message.id) }}</code>
            </header>
            <div class="parts">
              <template
                v-for="(part, index) in readableParts(message)"
                :key="`${message.id}-${index}`"
              >
                <p v-if="part.type === 'text'" class="text-part">{{ part.text }}</p>
                <p
                  v-else-if="part.type === 'reasoning' && part.text.trim().length > 0"
                  class="reasoning-part"
                >
                  Reasoning note: {{ part.text }}
                </p>
                <div v-else-if="part.type === 'dynamic-tool'" class="tool-part">
                  <strong>{{ toolLabel(part) }}</strong>
                  <span>{{ toolStateLabel(part) }}</span>
                </div>
                <a
                  v-else-if="part.type === 'source-url'"
                  class="source-part"
                  :href="part.url"
                  target="_blank"
                >
                  Reference: {{ part.title ?? part.url }}
                </a>
              </template>
              <p v-if="readableParts(message).length === 0" class="empty-state">
                Preparing a readable assistant response...
              </p>
              <p
                v-if="messageParts(message).length > readableParts(message).length"
                class="transport-note"
              >
                Transport metadata captured in Raw Sockudo Frames.
              </p>
            </div>
          </article>
          <p v-if="conversationMessages.length === 0" class="empty-state">
            Send a prompt to start a realtime AI turn.
          </p>
        </section>

        <form class="composer" @submit.prevent="sendPrompt()">
          <label class="field-block">
            <span>New prompt</span>
            <textarea
              ref="composer"
              v-model="prompt"
              aria-label="New prompt"
              rows="4"
              placeholder="Ask the assistant anything, then click Send prompt."
            />
          </label>
          <div class="composer-footer">
            <label class="field-block">
              <span>Edit instruction</span>
              <input
                v-model="editPrompt"
                class="text-input"
                aria-label="Edit instruction"
                placeholder="Only used when you click Edit last."
              />
            </label>
            <button type="submit">Send prompt</button>
          </div>
        </form>
      </section>

      <aside class="inspector-rail">
        <div class="panel">
          <div class="panel-title">
            <span>Branch Tree</span>
            <strong>{{ viewMessages.length }} messages</strong>
          </div>
          <div class="tree-list">
            <button
              v-for="message in viewMessages"
              :key="message.id"
              type="button"
              :class="{ selected: selectedMessageId === message.id }"
              @click="selectMessage(message as UIMessage)"
            >
              <span>{{ message.role }}</span>
              <strong>{{ messageText(message as UIMessage).slice(0, 74) || "event" }}</strong>
            </button>
          </div>
          <div class="tree-meta">
            <span>Selected node</span>
            <code>{{ selectedNode?.turnId ?? selectedMessageId ?? "none" }}</code>
            <span>Siblings</span>
            <strong>{{ selectedSiblings.length }}</strong>
          </div>
        </div>

        <div class="panel">
          <div class="panel-title">
            <span>Active Turns</span>
            <strong>{{ activeTurnCount }}</strong>
          </div>
          <ul class="turn-list">
            <li v-for="turn in activeTurnRows" :key="turn.turnId">
              <code>{{ turn.turnId }}</code>
              <span>{{ turn.owner }}</span>
            </li>
          </ul>
        </div>

        <div class="panel">
          <div class="panel-title">
            <span>Raw Sockudo Frames</span>
            <strong>{{ rawMessages.length }}</strong>
          </div>
          <div class="raw-list">
            <article
              v-for="message in rawRecent"
              :key="`${message.messageSerial}-${message.historySerial}`"
            >
              <header>
                <strong>{{ message.name }}</strong>
                <span>{{ message.action }}</span>
              </header>
              <code>{{ message.messageSerial }}</code>
              <pre>{{ JSON.stringify(message.getTransportHeaders(), null, 2) }}</pre>
            </article>
          </div>
        </div>
      </aside>
    </section>

    <section class="lower-grid">
      <div class="panel">
        <div class="panel-title">
          <span>Comparison View</span>
          <strong>{{ compareMessages.length }} loaded</strong>
        </div>
        <div class="comparison-list">
          <article v-for="message in compareMessages" :key="message.id">
            <strong>{{ message.role }}</strong>
            <p>{{ messageText(message as UIMessage).slice(0, 180) }}</p>
          </article>
        </div>
      </div>

      <div class="panel">
        <div class="panel-title">
          <span>Timeline</span>
          <strong>{{ timeline.length }}</strong>
        </div>
        <ol class="timeline-list">
          <li v-for="event in timeline.slice(0, 10)" :key="`${event.at}-${event.text}`">
            <span>{{ event.at }}</span>
            <strong>{{ event.kind }}</strong>
            <p>{{ event.text }}</p>
          </li>
        </ol>
      </div>
    </section>
  </main>
</template>
