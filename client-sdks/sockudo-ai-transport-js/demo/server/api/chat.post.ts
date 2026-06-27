import { createServerTransport } from "@sockudo/ai-transport/vercel";
import type { VercelOutput } from "@sockudo/ai-transport/vercel";
import { streamText, toUIMessageStream } from "ai";
import { demoConfig } from "../utils/config";
import { isAllowedDemoChannel, realtimeClient } from "../utils/sockudo";

export default defineEventHandler(async (event) => {
  const body = (await readBody(event)) as Record<string, unknown>;
  const config = demoConfig();
  const turnId = requireString(body.turnId, "turnId");
  const invocationId = requireString(body.invocationId, "invocationId");
  const inputEventId = requireString(body.inputEventId, "inputEventId");
  const channelName = optionalString(body.channelName) ?? config.channelName;
  if (!isAllowedDemoChannel(channelName)) {
    throw createError({ statusCode: 400, statusMessage: "unexpected channel" });
  }
  const clientId = optionalString(body.clientId);
  // @docs-snippet usechat-route
  const transport = createServerTransport({
    client: realtimeClient(),
    channelName,
  });
  const turn = transport.newTurn({
    turnId,
    invocationId,
    inputEventId,
    ...(clientId === undefined ? {} : { clientId }),
    onCancel(request) {
      return request.filter.all === true || request.turnOwners.get(turnId) === clientId;
    },
    onError(error) {
      console.error("[sockudo-ai-transport-demo] turn failed", error.message);
    },
  });
  // @docs-snippet-end
  const work = runTurn(turn, body, config.model).finally(() => {
    transport.close();
  });
  event.waitUntil?.(work);
  void work;
  setResponseStatus(event, 202);
  return null;
});

// @docs-snippet core-route
async function runTurn(
  turn: ReturnType<ReturnType<typeof createServerTransport>["newTurn"]>,
  body: Record<string, unknown>,
  model: string,
): Promise<void> {
  await turn.start();
  try {
    const stream = hasGatewayKey()
      ? await liveGatewayStream(body, model, turn.abortSignal)
      : demoUiMessageStream(latestText(body));
    await turn.streamResponse(stream);
    await turn.end("complete");
  } catch (error) {
    await turn.streamResponse(errorStream(error));
    await turn.end("error");
  }
}
// @docs-snippet-end

async function liveGatewayStream(
  body: Record<string, unknown>,
  model: string,
  abortSignal: AbortSignal,
): Promise<ReadableStream<VercelOutput>> {
  const common = {
    model: model as Parameters<typeof streamText>[0]["model"],
    abortSignal,
    instructions:
      "You are the live model inside the Sockudo AI Transport Nuxt demo. Be concise, show token streaming clearly, and mention realtime fanout, durable history, cancellation, edits, regeneration, branches, and multi-device sync when relevant.",
    maxOutputTokens: 700,
  };
  const result = streamText({ ...common, prompt: latestText(body) });
  return toUIMessageStream({ stream: result.stream }) as ReadableStream<VercelOutput>;
}

function demoUiMessageStream(prompt: string): ReadableStream<VercelOutput> {
  const words = offlineAnswer(prompt).split(/\s+/);
  const chunks: VercelOutput[] = [
    { type: "start", messageMetadata: { mode: "offline-demo", prompt } },
    { type: "start-step" },
    { type: "reasoning-start", id: "reasoning-demo" },
    {
      type: "reasoning-delta",
      id: "reasoning-demo",
      delta:
        "Plan: answer the prompt first, then quietly attach transport metadata, tool activity, source, and recovery hints for the demo panels.",
    },
    { type: "reasoning-end", id: "reasoning-demo" },
    { type: "text-start", id: "text-demo" },
    ...words.flatMap((word) => [
      {
        type: "text-delta",
        id: "text-demo",
        delta: `${word} `,
      } as VercelOutput,
    ]),
    {
      type: "text-delta",
      id: "text-demo",
      delta:
        "Set AI_GATEWAY_API_KEY to replace this offline answer with live Vercel AI Gateway output.",
    },
    { type: "text-end", id: "text-demo" },
    {
      type: "data-transport-metric",
      data: {
        appendRollupWindowMs: 40,
        channel: "private-ai-nuxt",
        durable: true,
        synthetic: true,
      },
      transient: false,
    },
    {
      type: "tool-input-start",
      toolCallId: "capability-scan",
      toolName: "scanCapabilities",
      dynamic: true,
    },
    {
      type: "tool-input-delta",
      toolCallId: "capability-scan",
      inputTextDelta: JSON.stringify({
        prompt,
        checks: ["history", "branches", "cancel"],
      }),
    },
    {
      type: "tool-input-available",
      toolCallId: "capability-scan",
      toolName: "scanCapabilities",
      input: { prompt, checks: ["history", "branches", "cancel"] },
    },
    {
      type: "tool-approval-request",
      toolCallId: "capability-scan",
      approvalId: "approval-demo",
    },
    {
      type: "tool-output-available",
      toolCallId: "capability-scan",
      output: {
        status: "approved-by-demo-harness",
        capabilities: [
          "token streaming",
          "branching",
          "regeneration",
          "history replay",
          "raw event inspection",
          "active-turn cancellation",
        ],
      },
    },
    {
      type: "source-url",
      sourceId: "gateway-docs",
      title: "Vercel AI Gateway",
      url: "https://vercel.com/docs/ai-gateway",
    },
    { type: "finish-step" },
    { type: "finish", finishReason: "stop", metadata: { synthetic: true } },
  ];
  return timedStream(chunks, 42);
}

function offlineAnswer(prompt: string): string {
  return `Offline mode received: "${prompt || "your prompt"}". No real model is being called because AI_GATEWAY_API_KEY is not set for this Nuxt process. Set AI_GATEWAY_API_KEY, restart the demo, then send the prompt again to get a live Vercel AI Gateway response. Sockudo still stores and streams this turn so the side panels can show raw frames, active state, branch history, cancellation, and multi-device sync. `;
}

function errorStream(error: unknown): ReadableStream<VercelOutput> {
  const message = error instanceof Error ? error.message : String(error);
  return timedStream(
    [
      { type: "start" },
      { type: "text-start", id: "error-text" },
      {
        type: "text-delta",
        id: "error-text",
        delta: `The model route failed: ${message}`,
      },
      { type: "text-end", id: "error-text" },
      { type: "error", errorText: message },
      { type: "finish", finishReason: "error" },
    ],
    0,
  );
}

function timedStream(
  chunks: readonly VercelOutput[],
  delayMs: number,
): ReadableStream<VercelOutput> {
  return new ReadableStream<VercelOutput>({
    async start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
        if (delayMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, delayMs));
        }
      }
      controller.close();
    },
  });
}

function latestText(body: Record<string, unknown>): string {
  const messages = Array.isArray(body.messages) ? body.messages : [];
  const last = messages.at(-1);
  if (!last || typeof last !== "object") {
    return "Demonstrate Sockudo AI Transport.";
  }
  const parts = (last as { parts?: unknown }).parts;
  if (!Array.isArray(parts)) {
    return "Demonstrate Sockudo AI Transport.";
  }
  return parts
    .map((part) =>
      part && typeof part === "object" && "text" in part
        ? String((part as { text?: unknown }).text ?? "")
        : "",
    )
    .join("")
    .trim();
}

function hasGatewayKey(): boolean {
  if (!process.env.AI_GATEWAY_API_KEY && process.env.VERCEL_API_KEY) {
    process.env.AI_GATEWAY_API_KEY = process.env.VERCEL_API_KEY;
  }
  return Boolean(process.env.AI_GATEWAY_API_KEY);
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw createError({ statusCode: 400, statusMessage: `missing ${field}` });
  }
  return value;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}
