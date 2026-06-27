export { version } from "../version.js";

import { vercelTurnEndReason } from "../vercel/transport/index.js";
import type { StreamResult, Turn, TurnEndReason } from "../core/transport/index.js";
import type { AI, VercelOutput, VercelProjection } from "../vercel/codec/index.js";

/**
 * Well-known OpenAI-compatible provider identifiers.
 *
 * These providers expose a Chat Completions-compatible streaming endpoint and
 * can be used through {@link streamOpenAICompatibleText} without a provider SDK.
 */
export type OpenAICompatibleProviderName =
  | "openai"
  | "openrouter"
  | "groq"
  | "togetherai"
  | "fireworks"
  | "deepseek"
  | "perplexity"
  | "mistral"
  | "xai"
  | "ollama"
  | "lmstudio";

/**
 * Default base URLs for high-traffic OpenAI-compatible providers.
 */
export const OPENAI_COMPATIBLE_PROVIDER_BASE_URLS = {
  openai: "https://api.openai.com/v1",
  openrouter: "https://openrouter.ai/api/v1",
  groq: "https://api.groq.com/openai/v1",
  togetherai: "https://api.together.xyz/v1",
  fireworks: "https://api.fireworks.ai/inference/v1",
  deepseek: "https://api.deepseek.com",
  perplexity: "https://api.perplexity.ai",
  mistral: "https://api.mistral.ai/v1",
  xai: "https://api.x.ai/v1",
  ollama: "http://127.0.0.1:11434/v1",
  lmstudio: "http://127.0.0.1:1234/v1",
} as const satisfies Record<OpenAICompatibleProviderName, string>;

/**
 * Chat message shape accepted by OpenAI-compatible chat completion endpoints.
 */
export interface OpenAICompatibleChatMessage {
  /** Message role. */
  role: "system" | "user" | "assistant" | "tool";
  /** Provider-specific message content. */
  content?: unknown;
  /** Optional tool call id for tool result messages. */
  tool_call_id?: string;
  /** Provider-specific extra fields. */
  [key: string]: unknown;
}

/**
 * Shared text-generation request accepted by direct provider adapters.
 */
export interface ProviderTextRequest {
  /** Provider model id. */
  model: string;
  /** Simple user prompt. Ignored when `messages` is supplied. */
  prompt?: string;
  /** OpenAI-compatible chat history. */
  messages?: readonly OpenAICompatibleChatMessage[];
  /** Maximum generated tokens. */
  maxOutputTokens?: number;
  /** Sampling temperature. */
  temperature?: number;
  /** Nucleus sampling value. */
  topP?: number;
  /** Provider-specific request fields. */
  body?: Record<string, unknown>;
  /** Provider-specific headers. */
  headers?: Record<string, string | undefined>;
  /** Abort signal. */
  signal?: AbortSignal;
  /** Stable assistant message id for emitted UI chunks. */
  messageId?: string;
}

/**
 * OpenAI-compatible HTTP streaming options.
 */
export interface OpenAICompatibleStreamOptions extends ProviderTextRequest {
  /** Provider preset. Ignored when `baseURL` is supplied. */
  provider?: OpenAICompatibleProviderName;
  /** Base URL ending before `/chat/completions`. */
  baseURL?: string;
  /** Bearer token. Optional for local providers such as Ollama and LM Studio. */
  apiKey?: string;
  /** Endpoint path.
   *
   * @defaultValue `"/chat/completions"`.
   */
  path?: string;
  /** Fetch implementation.
   *
   * @defaultValue `globalThis.fetch`.
   */
  fetch?: typeof globalThis.fetch;
}

/**
 * Structural subset of the official OpenAI SDK used for Chat Completions.
 */
export interface OpenAIChatCompletionsClient {
  /** Chat completions namespace. */
  chat: {
    completions: {
      /** Creates a streaming chat completion. */
      create(request: Record<string, unknown> & { stream: true }): Promise<AsyncIterable<unknown>>;
    };
  };
}

/**
 * Structural subset of the official OpenAI SDK used for Responses.
 */
export interface OpenAIResponsesClient {
  /** Responses namespace. */
  responses: {
    /** Creates a streaming response. */
    create(request: Record<string, unknown> & { stream: true }): Promise<AsyncIterable<unknown>>;
  };
}

/**
 * OpenAI SDK Chat Completions stream options.
 */
export interface OpenAIChatCompletionStreamOptions extends ProviderTextRequest {
  /** Official OpenAI SDK client or structural equivalent. */
  client: OpenAIChatCompletionsClient;
}

/**
 * OpenAI SDK Responses stream options.
 */
export interface OpenAIResponseStreamOptions extends ProviderTextRequest {
  /** Official OpenAI SDK client or structural equivalent. */
  client: OpenAIResponsesClient;
  /** Raw Responses API `input` override. */
  input?: unknown;
}

/**
 * Structural subset of the official Anthropic SDK messages client.
 */
export interface AnthropicMessagesClient {
  /** Messages namespace. */
  messages: {
    /** Creates a streaming Anthropic messages response. */
    create(request: Record<string, unknown> & { stream: true }): Promise<AsyncIterable<unknown>>;
  };
}

/**
 * Anthropic SDK message stream options.
 */
export interface AnthropicMessageStreamOptions extends ProviderTextRequest {
  /** Official Anthropic SDK client or structural equivalent. */
  client: AnthropicMessagesClient;
  /** Anthropic system prompt. */
  system?: string;
}

/**
 * Minimal direct LLM provider contract.
 */
export interface DirectLlmProvider {
  /** Streams a provider response as Vercel UI message chunks for Sockudo. */
  streamText(request: ProviderTextRequest): Promise<ReadableStream<VercelOutput>>;
}

/**
 * Provider registry returned by {@link createDirectLlmProviderRegistry}.
 */
export interface DirectLlmProviderRegistry {
  /** Resolves a provider by name. */
  get(name: string): DirectLlmProvider | undefined;
  /** Streams with a named provider. */
  streamText(name: string, request: ProviderTextRequest): Promise<ReadableStream<VercelOutput>>;
}

/**
 * Result returned by {@link runDirectLlmTurn}.
 */
export interface RunDirectLlmTurnResult {
  /** Pipe result from `turn.streamResponse`. */
  pipeResult: StreamResult;
  /** Published turn end reason. */
  turnEndReason: TurnEndReason;
}

/**
 * Streams text through a Chat Completions-compatible HTTP endpoint.
 */
export async function streamOpenAICompatibleText(
  options: OpenAICompatibleStreamOptions,
): Promise<ReadableStream<VercelOutput>> {
  const fetchFn = options.fetch ?? globalThis.fetch.bind(globalThis);
  const requestInit: RequestInit = {
    method: "POST",
    headers: stripUndefinedHeaders({
      ...(options.apiKey ? { Authorization: `Bearer ${options.apiKey}` } : {}),
      "Content-Type": "application/json",
      ...options.headers,
    }),
    body: JSON.stringify(openAICompatibleBody(options)),
    ...(options.signal !== undefined ? { signal: options.signal } : {}),
  };
  const response = await fetchFn(openAICompatibleUrl(options), requestInit);
  if (!response.ok || response.body === null) {
    throw new Error(
      `OpenAI-compatible provider request failed with status ${String(response.status)}`,
    );
  }
  return openAIChatCompletionEventsToUIMessageStream(
    streamSseJson(response.body),
    optionalMessageId(options.messageId),
  );
}

/**
 * Streams with the official OpenAI SDK Chat Completions API.
 */
export async function streamOpenAIChatCompletion(
  options: OpenAIChatCompletionStreamOptions,
): Promise<ReadableStream<VercelOutput>> {
  const stream = await options.client.chat.completions.create({
    ...openAICompatibleBody(options),
    stream: true,
  });
  return openAIChatCompletionEventsToUIMessageStream(stream, {
    ...optionalMessageId(options.messageId),
  });
}

/**
 * Streams with the official OpenAI SDK Responses API.
 */
export async function streamOpenAIResponse(
  options: OpenAIResponseStreamOptions,
): Promise<ReadableStream<VercelOutput>> {
  const stream = await options.client.responses.create({
    model: options.model,
    input: options.input ?? options.prompt ?? options.messages ?? "",
    ...(options.maxOutputTokens !== undefined
      ? { max_output_tokens: options.maxOutputTokens }
      : {}),
    ...(options.temperature !== undefined ? { temperature: options.temperature } : {}),
    ...(options.topP !== undefined ? { top_p: options.topP } : {}),
    ...options.body,
    stream: true,
  });
  return openAIResponseEventsToUIMessageStream(stream, {
    ...optionalMessageId(options.messageId),
  });
}

/**
 * Streams with the official Anthropic SDK Messages API.
 */
export async function streamAnthropicMessage(
  options: AnthropicMessageStreamOptions,
): Promise<ReadableStream<VercelOutput>> {
  const stream = await options.client.messages.create({
    model: options.model,
    messages: anthropicMessages(options),
    ...(options.system !== undefined ? { system: options.system } : {}),
    max_tokens: options.maxOutputTokens ?? 1024,
    ...(options.temperature !== undefined ? { temperature: options.temperature } : {}),
    ...(options.topP !== undefined ? { top_p: options.topP } : {}),
    ...options.body,
    stream: true,
  });
  return anthropicMessageEventsToUIMessageStream(stream, {
    ...optionalMessageId(options.messageId),
  });
}

/**
 * Creates a reusable OpenAI-compatible HTTP provider.
 */
export function createOpenAICompatibleProvider(
  defaults: Omit<OpenAICompatibleStreamOptions, "model"> & { model?: string },
): DirectLlmProvider {
  return {
    streamText(request) {
      return streamOpenAICompatibleText({
        ...defaults,
        ...request,
        headers: { ...defaults.headers, ...request.headers },
        body: { ...defaults.body, ...request.body },
        model: resolveModel(request.model, defaults.model, "gpt-4.1-mini"),
      });
    },
  };
}

/**
 * Creates a reusable OpenAI SDK provider.
 */
export function createOpenAISdkProvider(
  defaults:
    | (Omit<OpenAIChatCompletionStreamOptions, "model"> & {
        mode?: "chat";
        model?: string;
      })
    | (Omit<OpenAIResponseStreamOptions, "model"> & {
        mode: "responses";
        model?: string;
      }),
): DirectLlmProvider {
  return {
    streamText(request) {
      const model = resolveModel(request.model, defaults.model, "gpt-4.1-mini");
      if (defaults.mode === "responses") {
        return streamOpenAIResponse({
          ...defaults,
          ...request,
          headers: { ...defaults.headers, ...request.headers },
          body: { ...defaults.body, ...request.body },
          model,
        });
      }
      return streamOpenAIChatCompletion({
        ...defaults,
        ...request,
        headers: { ...defaults.headers, ...request.headers },
        body: { ...defaults.body, ...request.body },
        model,
      });
    },
  };
}

/**
 * Creates a reusable Anthropic SDK provider.
 */
export function createAnthropicSdkProvider(
  defaults: Omit<AnthropicMessageStreamOptions, "model"> & { model?: string },
): DirectLlmProvider {
  return {
    streamText(request) {
      return streamAnthropicMessage({
        ...defaults,
        ...request,
        headers: { ...defaults.headers, ...request.headers },
        body: { ...defaults.body, ...request.body },
        model: resolveModel(request.model, defaults.model, "claude-sonnet-4-5"),
      });
    },
  };
}

/**
 * Creates a named direct-provider registry.
 */
export function createDirectLlmProviderRegistry(
  providers: Record<string, DirectLlmProvider>,
): DirectLlmProviderRegistry {
  return {
    get(name) {
      return providers[name];
    },
    streamText(name, request) {
      const provider = providers[name];
      if (!provider) {
        throw new Error(`unknown direct LLM provider ${name}`);
      }
      return provider.streamText(request);
    },
  };
}

/**
 * Runs a Sockudo server turn from a direct provider stream.
 *
 * This helper starts the turn, streams provider chunks through
 * `turn.streamResponse`, maps completion to a turn end reason, publishes
 * `ai-turn-end`, and returns the evidence.
 */
export async function runDirectLlmTurn(
  turn: Turn<VercelOutput, VercelProjection, AI.UIMessage>,
  provider: DirectLlmProvider,
  request: ProviderTextRequest,
): Promise<RunDirectLlmTurnResult> {
  await turn.start();
  const stream = await provider.streamText({
    ...request,
    signal: turn.abortSignal,
  });
  const pipeResult = await turn.streamResponse(stream);
  const turnEndReason = await vercelTurnEndReason(
    pipeResult,
    Promise.resolve(finishReasonFromPipe(pipeResult)),
  );
  await turn.end(turnEndReason);
  return { pipeResult, turnEndReason };
}

/**
 * Maps OpenAI Chat Completions stream events into UI message chunks.
 */
export function openAIChatCompletionEventsToUIMessageStream(
  events: AsyncIterable<unknown>,
  options: { messageId?: string } = {},
): ReadableStream<VercelOutput> {
  return readableFromAsync(async function* () {
    const state = createStreamState(options.messageId);
    for await (const event of events) {
      const chunk = record(event);
      const choices = Array.isArray(chunk.choices) ? chunk.choices : [];
      for (const choiceValue of choices) {
        const choice = record(choiceValue);
        const delta = record(choice.delta);
        const content = stringValue(delta.content);
        if (content !== undefined && content.length > 0) {
          yield* emitTextDelta(state, content);
        }
        yield* emitOpenAIToolDeltas(state, delta.tool_calls);
        const finish = stringValue(choice.finish_reason);
        if (finish !== undefined) {
          yield* closeOpenAITools(state);
          yield* closeText(state);
          yield finishChunk(mapOpenAIFinishReason(finish));
        }
      }
    }
    yield* closeOpenAITools(state);
    yield* closeText(state);
  });
}

/**
 * Maps OpenAI Responses stream events into UI message chunks.
 */
export function openAIResponseEventsToUIMessageStream(
  events: AsyncIterable<unknown>,
  options: { messageId?: string } = {},
): ReadableStream<VercelOutput> {
  return readableFromAsync(async function* () {
    const state = createStreamState(options.messageId);
    for await (const event of events) {
      const item = record(event);
      const type = stringValue(item.type);
      if (type === "response.output_text.delta") {
        const delta = stringValue(item.delta);
        if (delta !== undefined) {
          yield* emitTextDelta(state, delta);
        }
        continue;
      }
      if (type === "response.output_text.done") {
        yield* closeText(state);
        continue;
      }
      if (type === "response.function_call_arguments.delta") {
        const callId = stringValue(item.item_id) ?? "tool-call";
        const name = stringValue(record(item.output_item).name) ?? "tool";
        yield* emitToolDelta(state, callId, name, stringValue(item.delta) ?? "");
        continue;
      }
      if (type === "response.function_call_arguments.done") {
        const callId = stringValue(item.item_id) ?? "tool-call";
        yield* closeTool(state, callId);
        continue;
      }
      if (type === "response.completed") {
        yield* closeOpenAITools(state);
        yield* closeText(state);
        yield finishChunk("stop");
        continue;
      }
      if (type === "error" || type === "response.failed") {
        yield errorChunk(errorMessage(item));
      }
    }
    yield* closeOpenAITools(state);
    yield* closeText(state);
  });
}

/**
 * Maps Anthropic Messages stream events into UI message chunks.
 */
export function anthropicMessageEventsToUIMessageStream(
  events: AsyncIterable<unknown>,
  options: { messageId?: string } = {},
): ReadableStream<VercelOutput> {
  return readableFromAsync(async function* () {
    const state = createStreamState(options.messageId);
    let finishReason: string | undefined;
    for await (const event of events) {
      const item = record(event);
      const type = stringValue(item.type);
      if (type === "content_block_start") {
        const index = numberValue(item.index) ?? 0;
        const block = record(item.content_block);
        if (block.type === "text") {
          yield* emitTextDelta(state, stringValue(block.text) ?? "");
        } else if (block.type === "thinking") {
          yield* emitReasoningDelta(state, stringValue(block.thinking) ?? "");
        } else if (block.type === "tool_use") {
          const callId = stringValue(block.id) ?? `tool-${String(index)}`;
          const name = stringValue(block.name) ?? "tool";
          ensureTool(state, callId, name);
        }
        continue;
      }
      if (type === "content_block_delta") {
        const index = numberValue(item.index) ?? 0;
        const delta = record(item.delta);
        if (delta.type === "text_delta") {
          yield* emitTextDelta(state, stringValue(delta.text) ?? "");
        } else if (delta.type === "thinking_delta") {
          yield* emitReasoningDelta(state, stringValue(delta.thinking) ?? "");
        } else if (delta.type === "input_json_delta") {
          const callId = `tool-${String(index)}`;
          yield* emitToolDelta(
            state,
            callId,
            state.tools.get(callId)?.name ?? "tool",
            stringValue(delta.partial_json) ?? "",
          );
        }
        continue;
      }
      if (type === "content_block_stop") {
        const callId = `tool-${String(numberValue(item.index) ?? 0)}`;
        yield* closeTool(state, callId);
        continue;
      }
      if (type === "message_delta") {
        finishReason = stringValue(record(item.delta).stop_reason);
        continue;
      }
      if (type === "message_stop") {
        yield* closeOpenAITools(state);
        yield* closeReasoning(state);
        yield* closeText(state);
        yield finishChunk(mapAnthropicFinishReason(finishReason));
        continue;
      }
      if (type === "error") {
        yield errorChunk(errorMessage(item));
      }
    }
    yield* closeOpenAITools(state);
    yield* closeReasoning(state);
    yield* closeText(state);
  });
}

interface StreamState {
  messageId?: string;
  started: boolean;
  textOpen: boolean;
  reasoningOpen: boolean;
  tools: Map<string, ToolState>;
}

interface ToolState {
  id: string;
  name: string;
  started: boolean;
  input: string;
}

interface SseEvent {
  event?: string;
  data: string;
}

function createStreamState(messageId?: string): StreamState {
  return {
    ...(messageId !== undefined ? { messageId } : {}),
    started: false,
    textOpen: false,
    reasoningOpen: false,
    tools: new Map(),
  };
}

function* startMessage(state: StreamState): Iterable<VercelOutput> {
  if (state.started) {
    return;
  }
  state.started = true;
  yield {
    type: "start",
    ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
  };
}

function* emitTextDelta(state: StreamState, delta: string): Iterable<VercelOutput> {
  yield* startMessage(state);
  if (!state.textOpen) {
    state.textOpen = true;
    yield {
      type: "text-start",
      id: "text",
      ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
    };
  }
  if (delta.length > 0) {
    yield {
      type: "text-delta",
      id: "text",
      delta,
      ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
    };
  }
}

function* closeText(state: StreamState): Iterable<VercelOutput> {
  if (!state.textOpen) {
    return;
  }
  state.textOpen = false;
  yield {
    type: "text-end",
    id: "text",
    ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
  };
}

function* emitReasoningDelta(state: StreamState, delta: string): Iterable<VercelOutput> {
  yield* startMessage(state);
  if (!state.reasoningOpen) {
    state.reasoningOpen = true;
    yield {
      type: "reasoning-start",
      id: "reasoning",
      ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
    };
  }
  if (delta.length > 0) {
    yield {
      type: "reasoning-delta",
      id: "reasoning",
      delta,
      ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
    };
  }
}

function* closeReasoning(state: StreamState): Iterable<VercelOutput> {
  if (!state.reasoningOpen) {
    return;
  }
  state.reasoningOpen = false;
  yield {
    type: "reasoning-end",
    id: "reasoning",
    ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
  };
}

function* emitOpenAIToolDeltas(state: StreamState, toolCalls: unknown): Iterable<VercelOutput> {
  if (!Array.isArray(toolCalls)) {
    return;
  }
  for (const callValue of toolCalls) {
    const call = record(callValue);
    const fn = record(call.function);
    const index = numberValue(call.index) ?? 0;
    const callId = stringValue(call.id) ?? `tool-${String(index)}`;
    const name = stringValue(fn.name) ?? state.tools.get(callId)?.name ?? "tool";
    yield* emitToolDelta(state, callId, name, stringValue(fn.arguments) ?? "");
  }
}

function* emitToolDelta(
  state: StreamState,
  callId: string,
  name: string,
  delta: string,
): Iterable<VercelOutput> {
  const tool = ensureTool(state, callId, name);
  yield* startMessage(state);
  if (!tool.started) {
    tool.started = true;
    yield {
      type: "tool-input-start",
      toolCallId: callId,
      toolName: tool.name,
      ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
    };
  }
  if (delta.length > 0) {
    tool.input += delta;
    yield {
      type: "tool-input-delta",
      toolCallId: callId,
      inputTextDelta: delta,
      ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
    };
  }
}

function* closeTool(state: StreamState, callId: string): Iterable<VercelOutput> {
  const tool = state.tools.get(callId);
  if (!tool?.started) {
    return;
  }
  tool.started = false;
  yield {
    type: "tool-input-available",
    toolCallId: callId,
    toolName: tool.name,
    input: parseJsonish(tool.input),
    ...(state.messageId !== undefined ? { messageId: state.messageId } : {}),
  };
}

function* closeOpenAITools(state: StreamState): Iterable<VercelOutput> {
  for (const callId of state.tools.keys()) {
    yield* closeTool(state, callId);
  }
}

function ensureTool(state: StreamState, callId: string, name: string): ToolState {
  let tool = state.tools.get(callId);
  if (!tool) {
    tool = { id: callId, name, started: false, input: "" };
    state.tools.set(callId, tool);
  }
  if (tool.name === "tool" && name !== "tool") {
    tool.name = name;
  }
  return tool;
}

function finishChunk(finishReason: string): VercelOutput {
  return { type: "finish", finishReason };
}

function errorChunk(message: string): VercelOutput {
  return { type: "error", errorText: message };
}

function openAICompatibleUrl(options: OpenAICompatibleStreamOptions): string {
  const base =
    options.baseURL ?? OPENAI_COMPATIBLE_PROVIDER_BASE_URLS[options.provider ?? "openai"];
  const path = options.path ?? "/chat/completions";
  return `${base.replace(/\/+$/u, "")}/${path.replace(/^\/+/u, "")}`;
}

function openAICompatibleBody(options: ProviderTextRequest): Record<string, unknown> {
  return {
    model: options.model,
    messages: options.messages ?? [{ role: "user", content: options.prompt ?? "" }],
    stream: true,
    ...(options.maxOutputTokens !== undefined ? { max_tokens: options.maxOutputTokens } : {}),
    ...(options.temperature !== undefined ? { temperature: options.temperature } : {}),
    ...(options.topP !== undefined ? { top_p: options.topP } : {}),
    ...options.body,
  };
}

function anthropicMessages(options: ProviderTextRequest): readonly Record<string, unknown>[] {
  if (options.messages !== undefined) {
    return options.messages
      .filter((message) => message.role !== "system")
      .map((message) => ({
        role: message.role === "assistant" ? "assistant" : "user",
        content: message.content ?? "",
      }));
  }
  return [{ role: "user", content: options.prompt ?? "" }];
}

async function* streamSseJson(body: ReadableStream<Uint8Array>): AsyncIterable<unknown> {
  for await (const event of streamSse(body)) {
    if (event.data === "[DONE]") {
      return;
    }
    try {
      yield JSON.parse(event.data) as unknown;
    } catch {
      yield { type: event.event ?? "message", data: event.data };
    }
  }
}

async function* streamSse(body: ReadableStream<Uint8Array>): AsyncIterable<SseEvent> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) {
        break;
      }
      buffer += decoder.decode(next.value, { stream: true });
      let boundary = buffer.indexOf("\n\n");
      while (boundary !== -1) {
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const event = parseSseBlock(block);
        if (event !== undefined) {
          yield event;
        }
        boundary = buffer.indexOf("\n\n");
      }
    }
    buffer += decoder.decode();
    const tail = parseSseBlock(buffer);
    if (tail !== undefined) {
      yield tail;
    }
  } finally {
    reader.releaseLock();
  }
}

function parseSseBlock(block: string): SseEvent | undefined {
  const data: string[] = [];
  let event: string | undefined;
  for (const line of block.split(/\r?\n/u)) {
    if (line.startsWith("event:")) {
      event = line.slice(6).trim();
    } else if (line.startsWith("data:")) {
      data.push(line.slice(5).trimStart());
    }
  }
  if (data.length === 0) {
    return undefined;
  }
  return event === undefined ? { data: data.join("\n") } : { event, data: data.join("\n") };
}

function readableFromAsync<T>(create: () => AsyncIterable<T>): ReadableStream<T> {
  const iterator = create()[Symbol.asyncIterator]();
  return new ReadableStream<T>({
    async pull(controller) {
      const next = await iterator.next();
      if (next.done) {
        controller.close();
      } else {
        controller.enqueue(next.value);
      }
    },
    async cancel() {
      await iterator.return?.();
    },
  });
}

function stripUndefinedHeaders(
  headers: Record<string, string | undefined>,
): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (value !== undefined) {
      result[key] = value;
    }
  }
  return result;
}

function mapOpenAIFinishReason(reason: string): string {
  if (reason === "stop") {
    return "stop";
  }
  if (reason === "length") {
    return "length";
  }
  if (reason === "content_filter") {
    return "content-filter";
  }
  if (reason === "tool_calls" || reason === "function_call") {
    return "tool-calls";
  }
  return "other";
}

function mapAnthropicFinishReason(reason: string | undefined): string {
  if (reason === "max_tokens") {
    return "length";
  }
  if (reason === "tool_use") {
    return "tool-calls";
  }
  if (reason === "stop_sequence" || reason === "end_turn") {
    return "stop";
  }
  return reason === undefined ? "stop" : "other";
}

function finishReasonFromPipe(pipeResult: StreamResult): string | undefined {
  if (pipeResult.reason === "complete") {
    return "stop";
  }
  if (pipeResult.reason === "cancelled") {
    return undefined;
  }
  return "error";
}

function resolveModel(
  requested: string,
  fallback: string | undefined,
  defaultModel: string,
): string {
  return requested.length > 0 ? requested : (fallback ?? defaultModel);
}

function parseJsonish(value: string): unknown {
  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }
  try {
    return JSON.parse(trimmed) as unknown;
  } catch {
    return trimmed;
  }
}

function optionalMessageId(messageId: string | undefined): {
  messageId?: string;
} {
  return messageId === undefined ? {} : { messageId };
}

function errorMessage(value: unknown): string {
  const item = record(value);
  const error = record(item.error);
  return (
    stringValue(error.message) ??
    stringValue(item.message) ??
    stringValue(item.error) ??
    "provider stream error"
  );
}

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function numberValue(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}
