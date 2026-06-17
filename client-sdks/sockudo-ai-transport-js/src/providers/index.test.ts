import { describe, expect, it, vi } from "vitest";

import type { AI } from "../vercel/codec/index.js";
import {
  anthropicMessageEventsToUIMessageStream,
  createDirectLlmProviderRegistry,
  createOpenAICompatibleProvider,
  openAIChatCompletionEventsToUIMessageStream,
  streamOpenAIChatCompletion,
  streamOpenAICompatibleText,
  streamOpenAIResponse,
} from "./index.js";

describe("direct provider adapters", () => {
  it("maps OpenAI Chat Completions text chunks to UI message chunks", async () => {
    const chunks = await collect(
      openAIChatCompletionEventsToUIMessageStream(
        asyncIterable([
          {
            choices: [
              {
                delta: { content: "hello" },
              },
            ],
          },
          {
            choices: [
              {
                delta: { content: " world" },
                finish_reason: "stop",
              },
            ],
          },
        ]),
        { messageId: "assistant-1" },
      ),
    );

    expect(chunks.map((chunk) => chunk.type)).toEqual([
      "start",
      "text-start",
      "text-delta",
      "text-delta",
      "text-end",
      "finish",
    ]);
    expect(chunks[2]).toMatchObject({
      type: "text-delta",
      delta: "hello",
      messageId: "assistant-1",
    });
  });

  it("maps OpenAI tool-call deltas into tool input chunks", async () => {
    const chunks = await collect(
      openAIChatCompletionEventsToUIMessageStream(
        asyncIterable([
          {
            choices: [
              {
                delta: {
                  tool_calls: [
                    {
                      index: 0,
                      id: "call-1",
                      function: { name: "lookup", arguments: '{"q"' },
                    },
                  ],
                },
              },
            ],
          },
          {
            choices: [
              {
                delta: {
                  tool_calls: [
                    {
                      index: 0,
                      id: "call-1",
                      function: { arguments: ':"x"}' },
                    },
                  ],
                },
                finish_reason: "tool_calls",
              },
            ],
          },
        ]),
      ),
    );

    expect(chunks.map((chunk) => chunk.type)).toEqual([
      "start",
      "tool-input-start",
      "tool-input-delta",
      "tool-input-delta",
      "tool-input-available",
      "finish",
    ]);
    expect(chunks.at(-2)).toMatchObject({
      type: "tool-input-available",
      toolCallId: "call-1",
      input: { q: "x" },
    });
    expect(chunks.at(-1)).toMatchObject({
      type: "finish",
      finishReason: "tool-calls",
    });
  });

  it("calls OpenAI-compatible fetch endpoints and parses SSE", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(() =>
      Promise.resolve(
        new Response(
          sse([
            {
              choices: [{ delta: { content: "ok" }, finish_reason: "stop" }],
            },
            "[DONE]",
          ]),
          { status: 200 },
        ),
      ),
    );

    const chunks = await collect(
      await streamOpenAICompatibleText({
        provider: "groq",
        apiKey: "token",
        model: "llama",
        prompt: "hi",
        fetch,
      }),
    );

    const fetchCall = fetch.mock.calls[0];
    expect(fetchCall?.[0]).toBe(
      "https://api.groq.com/openai/v1/chat/completions",
    );
    expect(fetchCall?.[1]?.method).toBe("POST");
    expect(fetchCall?.[1]?.headers).toMatchObject({
      Authorization: "Bearer token",
    });
    expect(chunks.map((chunk) => chunk.type)).toContain("text-delta");
  });

  it("uses structural OpenAI SDK clients", async () => {
    const chatClient = {
      chat: {
        completions: {
          create: vi.fn(() =>
            Promise.resolve(
              asyncIterable([
                {
                  choices: [
                    {
                      delta: { content: "sdk" },
                      finish_reason: "stop",
                    },
                  ],
                },
              ]),
            ),
          ),
        },
      },
    };
    const chunks = await collect(
      await streamOpenAIChatCompletion({
        client: chatClient,
        model: "gpt-test",
        prompt: "hello",
      }),
    );

    expect(chatClient.chat.completions.create).toHaveBeenCalledWith(
      expect.objectContaining({ model: "gpt-test", stream: true }),
    );
    expect(chunks.some((chunk) => chunk.type === "text-delta")).toBe(true);
  });

  it("maps OpenAI Responses and Anthropic SDK streams", async () => {
    const responseClient = {
      responses: {
        create: vi.fn(() =>
          Promise.resolve(
            asyncIterable([
              { type: "response.output_text.delta", delta: "r" },
              { type: "response.completed" },
            ]),
          ),
        ),
      },
    };
    const responseChunks = await collect(
      await streamOpenAIResponse({
        client: responseClient,
        model: "gpt-test",
        prompt: "hello",
      }),
    );
    const anthropicChunks = await collect(
      anthropicMessageEventsToUIMessageStream(
        asyncIterable([
          {
            type: "content_block_delta",
            delta: { type: "text_delta", text: "a" },
          },
          {
            type: "message_delta",
            delta: { stop_reason: "end_turn" },
          },
          { type: "message_stop" },
        ]),
      ),
    );

    expect(responseChunks.map((chunk) => chunk.type)).toContain("text-delta");
    expect(anthropicChunks.map((chunk) => chunk.type)).toContain("text-delta");
  });

  it("creates reusable providers and registries", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>(() =>
      Promise.resolve(
        new Response(
          sse([
            {
              choices: [{ delta: { content: "ok" }, finish_reason: "stop" }],
            },
          ]),
          { status: 200 },
        ),
      ),
    );
    const registry = createDirectLlmProviderRegistry({
      local: createOpenAICompatibleProvider({
        baseURL: "http://localhost:1234/v1",
        model: "local-model",
        fetch,
      }),
    });

    const chunks = await collect(
      await registry.streamText("local", { model: "", prompt: "hi" }),
    );

    expect(chunks.some((chunk) => chunk.type === "finish")).toBe(true);
  });
});

async function collect(
  stream: ReadableStream<AI.UIMessageChunk>,
): Promise<AI.UIMessageChunk[]> {
  const reader = stream.getReader();
  const chunks: AI.UIMessageChunk[] = [];
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) {
        return chunks;
      }
      chunks.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }
}

async function* asyncIterable(
  items: readonly unknown[],
): AsyncIterable<unknown> {
  for (const item of items) {
    await Promise.resolve();
    yield item;
  }
}

function sse(items: readonly unknown[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const item of items) {
        const data = item === "[DONE]" ? item : JSON.stringify(item);
        controller.enqueue(encoder.encode(`data: ${data}\n\n`));
      }
      controller.close();
    },
  });
}
