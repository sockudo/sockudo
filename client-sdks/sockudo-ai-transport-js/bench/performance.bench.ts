// @vitest-environment node

import { gzipSync } from "node:zlib";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { build as viteBuild } from "vite";
import { describe, expect, it } from "vitest";

import {
  EVENT_AI_INPUT,
  EVENT_AI_OUTPUT,
  EVENT_AI_TURN_START,
  HEADER_CODEC_MESSAGE_ID,
  HEADER_INVOCATION_ID,
  HEADER_STATUS,
  HEADER_STREAM,
  HEADER_TURN_ID,
} from "../src/constants.js";
import type {
  Codec,
  DecodedBatch,
  Decoder,
  ReducerMeta,
  UserMessage,
} from "../src/core/codec/index.js";
import { createClientTransport } from "../src/core/transport/client-transport.js";
import type { ActiveTurn } from "../src/core/transport/client-transport.js";
import type { InvocationIdProvider } from "../src/core/transport/invocation.js";
import { createStreamRouter } from "../src/core/transport/stream-router.js";
import { createConversationTree } from "../src/core/transport/tree.js";
import { createView } from "../src/core/transport/view.js";
import { ErrorCode } from "../src/errors.js";
import {
  normalizeInboundMessage,
  type SockudoRawMessage,
} from "../src/realtime/adapter.js";
import type {
  ChannelEvents,
  ChannelLike,
  HistoryOptions,
  InboundMessage,
  MessageAck,
  MessageListener,
  MessageMutation,
  PaginatedResult,
  PresenceLike,
  PublishMessage,
  SubscribeOptions,
  Unsubscribe,
} from "../src/realtime/index.js";

interface BenchBaselines {
  bundle: {
    coreGzipBytes: number;
    reactIncrementalGzipBytes: number;
    vercelIncrementalGzipBytes: number;
    vercelReactIncrementalGzipBytes: number;
  };
  throughput: {
    singleStreamTokensPerSecond: number;
    concurrentFrameBudgetPercent: number;
  };
  memory: {
    tenThousandMessageTreeRetainedBytes: number;
    flatStreamRetainedGrowthBytes: number;
  };
  latency: {
    sendLocalP50Ms: number;
    sendLocalP99Ms: number;
    tokenWireToViewP50Ms: number;
  };
  react: {
    streamingCommitBudget: number;
    stableGetMessagesBudget: number;
  };
}

interface BenchReport {
  bundle?: Record<string, number | boolean>;
  throughput?: Record<string, number>;
  memory?: Record<string, number | string>;
  latency?: Record<string, number>;
  react?: Record<string, number | boolean>;
  generatedAt: string;
}

interface BundleSizes {
  core: number;
  coreReact: number;
  coreVercel: number;
  coreVercelReact: number;
}

interface BundleOutput {
  output: readonly BundleOutputEntry[];
}

interface BundleOutputEntry {
  code?: string;
  type: string;
}

interface Message {
  id: string;
  text: string;
  done?: boolean;
}

interface Projection {
  messages: Message[];
}

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const baselines = JSON.parse(
  await readFile(join(root, "bench/baselines.json"), "utf8"),
) as BenchBaselines;
const report: BenchReport = { generatedAt: new Date().toISOString() };
const codec = testCodec();

describe("P15 bundle budgets", () => {
  it("keeps entrypoint bundles under min+gzip budgets and preserves tree shaking", async () => {
    const sizes = await bundleSizes();
    const core = sizes.core;
    const reactIncremental = sizes.coreReact - sizes.core;
    const vercelIncremental = sizes.coreVercel - sizes.core;
    const vercelReactIncremental = sizes.coreVercelReact - sizes.coreVercel;
    const coreOnly = await bundledCode("core-only", [
      "import { createClientTransport } from '@sockudo/ai-transport';",
      "console.log(typeof createClientTransport);",
    ]);
    const noVercelReact = !coreOnly.includes("ChatTransportProvider");
    const noReactRuntime = !coreOnly.includes("useSyncExternalStore");
    const distImports = await runtimeBareImports();

    report.bundle = {
      coreGzipBytes: core,
      reactIncrementalGzipBytes: reactIncremental,
      vercelIncrementalGzipBytes: vercelIncremental,
      vercelReactIncrementalGzipBytes: vercelReactIncremental,
      treeShakeExcludesVercelReact: noVercelReact,
      treeShakeExcludesReactRuntime: noReactRuntime,
      runtimeBareImportCount: distImports.length,
    };

    expect(core).toBeLessThanOrEqual(baselines.bundle.coreGzipBytes);
    expect(reactIncremental).toBeLessThanOrEqual(
      baselines.bundle.reactIncrementalGzipBytes,
    );
    expect(vercelIncremental).toBeLessThanOrEqual(
      baselines.bundle.vercelIncrementalGzipBytes,
    );
    expect(vercelReactIncremental).toBeLessThanOrEqual(
      baselines.bundle.vercelReactIncrementalGzipBytes,
    );
    expect(noVercelReact).toBe(true);
    expect(noReactRuntime).toBe(true);
    expect(distImports).toEqual([]);
  });
});

describe("P15 throughput and latency budgets", () => {
  it("folds one token stream above 50k tokens/s and wire-to-view below 0.5ms p50", () => {
    const sample = measureSingleStream(20_000);
    report.throughput = {
      singleStreamTokensPerSecond: sample.tokensPerSecond,
      tokenWireToViewP50Ms: sample.tokenWireToViewP50Ms,
    };

    expect(sample.tokensPerSecond).toBeGreaterThanOrEqual(
      baselines.throughput.singleStreamTokensPerSecond,
    );
    expect(sample.tokenWireToViewP50Ms).toBeLessThanOrEqual(
      baselines.latency.tokenWireToViewP50Ms,
    );
  });

  it("keeps 16 concurrent 200 tok/s streams below 30% synthetic frame budget", () => {
    const frameBudgetMs = 16.67;
    const tokensPerFrame = 16 * Math.ceil(200 / 60);
    const { channel, transport } = connectedTransport();
    transport.view.getMessages();
    injectTurnStarts(channel, 16);

    const frameCosts: number[] = [];
    let serial = 100;
    for (let frame = 0; frame < 120; frame += 1) {
      const start = performance.now();
      for (let token = 0; token < tokensPerFrame; token += 1) {
        const stream = token % 16;
        channel.inject(
          output(stream, serial, `f${String(frame)}:t${String(token)}`),
        );
        serial += 1;
      }
      frameCosts.push(performance.now() - start);
    }
    const p95FrameMs = percentile(frameCosts, 95);
    const frameBudgetPercent = (p95FrameMs / frameBudgetMs) * 100;
    report.throughput = {
      ...(report.throughput ?? {}),
      concurrentP95FrameMs: p95FrameMs,
      concurrentFrameBudgetPercent: frameBudgetPercent,
    };

    expect(frameBudgetPercent).toBeLessThanOrEqual(
      baselines.throughput.concurrentFrameBudgetPercent,
    );
  });

  it("keeps send() local pipeline p50/p99 under budget with mocked network", async () => {
    const { transport } = connectedTransport({ turnStartDeadlineMs: 0 });
    const samples: number[] = [];
    for (let index = 0; index < 500; index += 1) {
      const start = performance.now();
      await transport.view.send(
        { id: `send-${String(index)}`, text: "hello" },
        { waitForTurnStart: false },
      );
      samples.push(performance.now() - start);
    }
    const p50 = percentile(samples, 50);
    const p99 = percentile(samples, 99);
    report.latency = { sendLocalP50Ms: p50, sendLocalP99Ms: p99 };

    expect(p50).toBeLessThanOrEqual(baselines.latency.sendLocalP50Ms);
    expect(p99).toBeLessThanOrEqual(baselines.latency.sendLocalP99Ms);
  });
});

describe("P15 memory and hostile-input bounds", () => {
  it("keeps 10k-message tree retained size under 50 MB when GC is available", () => {
    const before = heapUsedAfterGc();
    const tree = createConversationTree(codec);
    for (let index = 0; index < 10_000; index += 1) {
      tree.applyMessage(
        [
          {
            event: { id: `msg-${String(index)}`, text: `message ${String(index)}` },
            messageId: `msg-${String(index)}`,
            meta: { serial: index + 1, messageId: `msg-${String(index)}` },
          },
        ],
        headers(index, `msg-${String(index)}`),
        index + 1,
      );
    }
    const view = createView({ tree, codec, decoder: codec.createDecoder() });
    expect(view.getMessages()).toHaveLength(10_000);
    const retained = heapUsedAfterGc() - before;
    report.memory = {
      tenThousandMessageTreeRetainedBytes: Math.max(0, retained),
      gcMode: gcAvailable() ? "enforced" : "smoke",
    };
    if (gcAvailable()) {
      expect(retained).toBeLessThanOrEqual(
        baselines.memory.tenThousandMessageTreeRetainedBytes,
      );
    }
  });

  it("streams high-volume tokens with flat memory after close when GC is available", async () => {
    const tokenCount = process.env.BENCH_EXTENDED === "1" ? 1_000_000 : 100_000;
    const before = heapUsedAfterGc();
    const { channel, transport } = connectedTransport({
      streamQueueLimit: tokenCount + 16,
    });
    const active = (await transport.view.send(
      { id: "user-flat", text: "flat" },
      { waitForTurnStart: false },
    )) as ActiveTurn<Message>;
    channel.inject(lifecycle(EVENT_AI_TURN_START, 1, "turn-1", "inv-1"));
    for (let index = 0; index < tokenCount; index += 1) {
      channel.inject(output(0, index + 2, "x"));
    }
    channel.inject(output(0, tokenCount + 3, "", true));
    await cancelStream(active.stream);
    await transport.close();
    const retained = heapUsedAfterGc() - before;
    report.memory = {
      ...(report.memory ?? {}),
      flatStreamRetainedGrowthBytes: Math.max(0, retained),
      flatStreamTokenCount: tokenCount,
    };
    if (gcAvailable()) {
      expect(retained).toBeLessThanOrEqual(
        baselines.memory.flatStreamRetainedGrowthBytes,
      );
    }
  });

  it("fails closed at router queue caps under hostile slow consumers", async () => {
    const router = createStreamRouter<Message>({
      isTerminal: (message) => message.done === true,
      maxQueuedChunks: 1,
    });
    const stream = router.createStream("turn-hostile", "inv-hostile");
    expect(router.route("turn-hostile", "inv-hostile", { id: "m", text: "a" }))
      .toBe(true);
    expect(router.route("turn-hostile", "inv-hostile", { id: "m", text: "b" }))
      .toBe(false);
    await expect(stream.getReader().read()).rejects.toMatchObject({
      code: ErrorCode.StreamError,
    });
  });
});

describe("P15 React/view rerender and reference stability budgets", () => {
  it("keeps streaming view commits scoped and getMessages reference stable", () => {
    const { channel, transport } = connectedTransport();
    let commits = 0;
    let stableReferenceHits = 0;
    let previous = transport.view.getMessages();
    transport.view.on("update", () => {
      commits += 1;
      const current = transport.view.getMessages();
      if (current === previous) {
        stableReferenceHits += 1;
      }
      previous = current;
    });
    channel.inject(lifecycle(EVENT_AI_TURN_START, 1, "turn-1", "inv-1"));
    for (let index = 0; index < 64; index += 1) {
      channel.inject(output(0, index + 2, String(index)));
    }
    const final = transport.view.getMessages();
    const second = transport.view.getMessages();
    report.react = {
      streamingCommits: commits,
      getMessagesStableAfterRead: final === second,
      stableReferenceHits,
    };

    expect(commits).toBeLessThanOrEqual(
      baselines.react.streamingCommitBudget,
    );
    expect(final).toBe(second);
  });
});

it("writes latest benchmark evidence", async () => {
  await mkdir(join(root, "bench/results"), { recursive: true });
  await writeFile(
    join(root, "bench/results/latest.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );
  expect(report.bundle).toBeDefined();
  expect(report.throughput).toBeDefined();
  expect(report.memory).toBeDefined();
  expect(report.latency).toBeDefined();
  expect(report.react).toBeDefined();
});

async function bundleSizes(): Promise<BundleSizes> {
  return {
    core: gzipSize(await readFile(join(root, "dist/index.js"), "utf8")),
    coreReact: gzipSize(await readFile(join(root, "dist/react/index.js"), "utf8")),
    coreVercel: gzipSize(await readFile(join(root, "dist/vercel/index.js"), "utf8")),
    coreVercelReact: gzipSize(
      await readFile(join(root, "dist/vercel/react/index.js"), "utf8"),
    ),
  };
}

async function bundledCode(name: string, lines: readonly string[]): Promise<string> {
  const result = await viteBuild({
    configFile: false,
    logLevel: "silent",
    root,
    resolve: {
      alias: [
        {
          find: "@sockudo/ai-transport/react",
          replacement: join(root, "dist/react/index.js"),
        },
        {
          find: "@sockudo/ai-transport/vercel/react",
          replacement: join(root, "dist/vercel/react/index.js"),
        },
        {
          find: "@sockudo/ai-transport/vercel",
          replacement: join(root, "dist/vercel/index.js"),
        },
        { find: "@sockudo/ai-transport", replacement: join(root, "dist/index.js") },
      ],
    },
    build: {
      write: false,
      minify: "esbuild",
      sourcemap: false,
      rollupOptions: {
        input: await writeBundleEntry(name, lines),
        external: ["@sockudo/client", "@sockudo/client/react", "react", "ai"],
        output: { format: "esm", inlineDynamicImports: true },
        treeshake: { moduleSideEffects: false },
      },
    },
  });
  return bundleOutputs(result)
    .filter(isCodeChunk)
    .map((chunk) => chunk.code)
    .join("\n");
}

async function writeBundleEntry(
  name: string,
  lines: readonly string[],
): Promise<string> {
  const directory = join(root, "bench/.generated");
  await mkdir(directory, { recursive: true });
  const path = join(directory, `${name}.mjs`);
  await writeFile(path, `${lines.join("\n")}\n`);
  return path;
}

function bundleOutputs(
  result: Awaited<ReturnType<typeof viteBuild>>,
): readonly BundleOutputEntry[] {
  const outputs = Array.isArray(result) ? result : [result];
  for (const output of outputs) {
    if (isBundleOutput(output)) {
      return output.output;
    }
  }
  throw new Error("vite build did not return an in-memory bundle");
}

function isBundleOutput(value: unknown): value is BundleOutput {
  if (value === null || typeof value !== "object" || !("output" in value)) {
    return false;
  }
  return Array.isArray(value.output);
}

function isCodeChunk(
  entry: BundleOutputEntry,
): entry is BundleOutputEntry & { code: string; type: "chunk" } {
  return entry.type === "chunk" && typeof entry.code === "string";
}

async function runtimeBareImports(): Promise<string[]> {
  const files = [
    "dist/index.js",
    "dist/react/index.js",
    "dist/vercel/index.js",
    "dist/vercel/react/index.js",
  ];
  const allowed = new Set(["@sockudo/client", "@sockudo/client/react", "react", "ai"]);
  const imports = new Set<string>();
  for (const file of files) {
    const text = await readFile(join(root, file), "utf8");
    for (const match of text.matchAll(/\bfrom\s+["']([^."'][^"']*)["']/gu)) {
      const specifier = match[1];
      if (specifier !== undefined && !allowed.has(specifier)) {
        imports.add(specifier);
      }
    }
    for (const match of text.matchAll(/\bimport\(\s*["']([^."'][^"']*)["']\s*\)/gu)) {
      const specifier = match[1];
      if (specifier !== undefined && !allowed.has(specifier)) {
        imports.add(specifier);
      }
    }
  }
  return [...imports].sort();
}

function measureSingleStream(tokens: number): {
  tokensPerSecond: number;
  tokenWireToViewP50Ms: number;
} {
  const { channel, transport } = connectedTransport();
  transport.view.getMessages();
  channel.inject(lifecycle(EVENT_AI_TURN_START, 1, "turn-1", "inv-1"));
  const latencies: number[] = [];
  let lastStart = 0;
  transport.view.on("message", () => {
    latencies.push(performance.now() - lastStart);
  });
  const start = performance.now();
  for (let index = 0; index < tokens; index += 1) {
    lastStart = performance.now();
    channel.inject(output(0, index + 2, String(index)));
  }
  const elapsedMs = performance.now() - start;
  return {
    tokensPerSecond: tokens / (elapsedMs / 1000),
    tokenWireToViewP50Ms: percentile(latencies, 50),
  };
}

function connectedTransport(options: {
  turnStartDeadlineMs?: number;
  streamQueueLimit?: number;
} = {}): {
  channel: BenchChannel;
  transport: ReturnType<
    typeof createClientTransport<Message, Message, Projection, Message>
  >;
} {
  const channel = new BenchChannel("chat");
  const transport = createClientTransport({
    channel,
    codec,
    api: "https://agent.test/run",
    clientId: "client-1",
    idProvider: fixedIds(),
    fetch: okFetch(),
    turnStartDeadlineMs: options.turnStartDeadlineMs ?? 0,
    ...(options.streamQueueLimit === undefined
      ? {}
      : { streamQueueLimit: options.streamQueueLimit }),
  });
  transport.on("message", () => undefined);
  return { channel, transport };
}

function injectTurnStarts(channel: BenchChannel, count: number): void {
  for (let index = 0; index < count; index += 1) {
    channel.inject(
      lifecycle(
        EVENT_AI_TURN_START,
        index + 1,
        `turn-${String(index + 1)}`,
        `inv-${String(index + 1)}`,
      ),
    );
  }
}

class BenchChannel implements ChannelLike {
  private readonly listeners = new Set<MessageListener>();
  private readonly channelEvents = new Map<
    keyof ChannelEvents,
    Set<(payload: ChannelEvents[keyof ChannelEvents]) => void>
  >();
  private serial = 0;

  public readonly attachSerial = undefined;
  public readonly presence: PresenceLike = {
    get: () => Promise.resolve([]),
    enter: () => Promise.resolve(),
    update: () => Promise.resolve(),
    leave: () => Promise.resolve(),
    subscribe: () => () => undefined,
  };

  public constructor(public readonly name: string) {}

  public publish(message: PublishMessage): Promise<MessageAck> {
    return Promise.resolve(this.nextAck(message.messageSerial));
  }

  public appendMessage(
    messageSerial: string,
    data: string,
    options: Omit<MessageMutation, "data"> = {},
  ): Promise<MessageAck> {
    void data;
    void options;
    return Promise.resolve(this.nextAck(messageSerial));
  }

  public updateMessage(
    messageSerial: string,
    options: MessageMutation = {},
  ): Promise<MessageAck> {
    void options;
    return Promise.resolve(this.nextAck(messageSerial));
  }

  public deleteMessage(
    messageSerial: string,
    options: MessageMutation = {},
  ): Promise<MessageAck> {
    void options;
    return Promise.resolve(this.nextAck(messageSerial));
  }

  public subscribe(
    listener: MessageListener,
    options?: SubscribeOptions,
  ): Unsubscribe {
    void options;
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  public history(
    options: HistoryOptions = {},
  ): Promise<PaginatedResult<InboundMessage>> {
    void options;
    return Promise.resolve({
      items: [],
      hasNext: () => false,
      next: () => Promise.reject(new Error("no next page")),
    });
  }

  public on<K extends keyof ChannelEvents>(
    event: K,
    listener: (payload: ChannelEvents[K]) => void,
  ): Unsubscribe {
    let listeners = this.channelEvents.get(event);
    if (!listeners) {
      listeners = new Set();
      this.channelEvents.set(event, listeners);
    }
    listeners.add(listener as (payload: ChannelEvents[keyof ChannelEvents]) => void);
    return () => {
      listeners.delete(
        listener as (payload: ChannelEvents[keyof ChannelEvents]) => void,
      );
    };
  }

  public inject(raw: SockudoRawMessage): void {
    const message = normalizeInboundMessage(raw);
    for (const listener of this.listeners) {
      listener(message);
    }
  }

  private nextAck(messageSerial?: string): MessageAck {
    this.serial += 1;
    return {
      messageSerial: messageSerial ?? `msg-${String(this.serial)}`,
      historySerial: this.serial,
      deliverySerial: this.serial,
      versionSerial: `ver-${String(this.serial)}`,
    };
  }
}

function output(
  streamIndex: number,
  serial: number,
  delta: string,
  done = false,
): SockudoRawMessage {
  const suffix = String(streamIndex + 1);
  return {
    event: "sockudo:message.append",
    name: EVENT_AI_OUTPUT,
    channel: "chat",
    data: { id: `assistant-${suffix}`, text: delta, ...(done ? { done } : {}) },
    message_serial: `assistant-${suffix}`,
    history_serial: serial,
    delivery_serial: serial,
    serial,
    extras: {
      ai: {
        transport: {
          [HEADER_TURN_ID]: `turn-${suffix}`,
          [HEADER_INVOCATION_ID]: `inv-${suffix}`,
          [HEADER_CODEC_MESSAGE_ID]: `assistant-${suffix}`,
          [HEADER_STREAM]: "true",
          [HEADER_STATUS]: done ? "complete" : "streaming",
        },
      },
    },
  };
}

function lifecycle(
  event: string,
  serial: number,
  turnId: string,
  invocationId: string,
): SockudoRawMessage {
  return {
    event,
    name: event,
    channel: "chat",
    data: {},
    message_serial: `${turnId}:lifecycle`,
    history_serial: serial,
    delivery_serial: serial,
    serial,
    extras: {
      ai: {
        transport: {
          [HEADER_TURN_ID]: turnId,
          [HEADER_INVOCATION_ID]: invocationId,
        },
      },
    },
  };
}

function headers(index: number, messageId: string): Record<string, string> {
  return {
    [HEADER_TURN_ID]: `turn-${String(index)}`,
    [HEADER_CODEC_MESSAGE_ID]: messageId,
  };
}

function testCodec(): Codec<Message, Message, Projection, Message> {
  return {
    init: () => ({ messages: [] }),
    fold(projection, event) {
      const index = projection.messages.findIndex((message) => message.id === event.id);
      if (index === -1) {
        projection.messages.push(event);
      } else {
        projection.messages[index] = event;
      }
      return projection;
    },
    getMessages: (projection) => projection.messages,
    createUserMessage(message): UserMessage<Message> {
      return { message };
    },
    createRegenerate(target, parent) {
      return { target, parent };
    },
    resolveToolTarget: () => undefined,
    isTerminal: (output) => output.done === true,
    createEncoder() {
      throw new Error("encoder is not used by performance bench");
    },
    createDecoder(): Decoder<Message, Message> {
      return {
        decode(message): DecodedBatch<Message, Message> {
          const data = message.data as Message;
          const decodedMessageId =
            message.getTransportHeaders()[HEADER_CODEC_MESSAGE_ID] ??
            message.messageSerial;
          const decoded = {
            event: data,
            messageId: decodedMessageId,
            meta: {
              serial: message.deliverySerial ?? message.historySerial,
              messageId: decodedMessageId,
            } satisfies ReducerMeta,
          };
          if (message.name === EVENT_AI_OUTPUT) {
            return { inputs: [], outputs: [decoded] };
          }
          if (message.name === EVENT_AI_INPUT) {
            return { inputs: [decoded], outputs: [] };
          }
          return { inputs: [], outputs: [] };
        },
      };
    },
  };
}

function fixedIds(): InvocationIdProvider {
  let index = 0;
  return {
    turnId() {
      index += 1;
      return `turn-${String(index)}`;
    },
    invocationId() {
      return `inv-${String(index)}`;
    },
    inputEventId() {
      return `evt-${String(index)}`;
    },
    messageId() {
      return `msg-${String(index)}`;
    },
  };
}

function okFetch(): typeof fetch {
  return () => Promise.resolve(new Response(null, { status: 200 }));
}

async function cancelStream<T>(stream: ReadableStream<T>): Promise<void> {
  const reader = stream.getReader();
  await reader.cancel();
  reader.releaseLock();
}

function gzipSize(code: string): number {
  return gzipSync(code, { level: 9 }).byteLength;
}

function percentile(values: readonly number[], percentileValue: number): number {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((percentileValue / 100) * sorted.length) - 1),
  );
  return sorted[index] ?? 0;
}

function gcAvailable(): boolean {
  return typeof globalThis.gc === "function";
}

function heapUsedAfterGc(): number {
  globalThis.gc?.();
  globalThis.gc?.();
  return process.memoryUsage().heapUsed;
}
