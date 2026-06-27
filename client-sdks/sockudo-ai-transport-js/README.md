# `@sockudo/ai-transport`

TypeScript SDK for Sockudo AI Transport.

This package is a 1:1 API-parity port of `@ably/ai-transport` targeting the Sockudo AI Transport
wire protocol. The realtime substrate is the existing `@sockudo/client` SDK peer dependency; this
package does not duplicate connection, protocol, recovery, history, or mutation logic that belongs
there. Only `src/realtime/` imports `@sockudo/client`.

## Install

For apps, install the published packages:

```bash
pnpm add @sockudo/client @sockudo/ai-transport
```

For contributors working inside this repository, build this SDK and `@sockudo/client` from their
package directories:

```bash
cd client-sdks/sockudo-js
bun install
bun run build:all

cd ../sockudo-ai-transport-js
pnpm install
pnpm build
```

React and Vercel helpers use optional peers:

```bash
pnpm add react react-dom ai @ai-sdk/react
```

Vue and Svelte helpers use optional peers:

```bash
pnpm add vue @ai-sdk/vue
pnpm add svelte @ai-sdk/svelte
```

Direct provider helpers are structural. Install the SDK you use:

```bash
pnpm add openai
pnpm add @anthropic-ai/sdk
```

## Quickstart: Vercel AI SDK transport

The Vercel path mirrors Ably's AI Transport flow with Sockudo channel auth, versioned-message
proxying, and AI SDK `toUIMessageStream` output in a route handler. The Nuxt demo uses the workspace
`@sockudo/client` package from this monorepo.

```tsx
import { ChatTransportProvider } from "@sockudo/ai-transport/vercel/react";
```

See `demo/` for the Nuxt command-center demo.

## Quickstart: Vue And Svelte

The Vercel-compatible chat transport can be passed to AI SDK Vue/Svelte `useChat` just like the
React adapter, while Sockudo owns realtime fanout, history, recovery, and branching.

```ts
import { provideChatTransport } from "@sockudo/ai-transport/vercel/vue";
import { createChatTransportStore } from "@sockudo/ai-transport/vercel/svelte";
```

## Quickstart: Core SDK

The core path uses `TransportProvider`, `useView`, `send`, `edit`, `regenerate`, and multiple branch
views without Vercel `useChat`.

```tsx
import { TransportProvider, useView } from "@sockudo/ai-transport/react";
```

See `demo/` for branch-tree, history, cancellation, and raw-frame examples.

## Quickstart: Direct Providers

Use `@sockudo/ai-transport/providers` when your agent should call provider SDKs or OpenAI-compatible
endpoints directly instead of going through AI SDK provider packages.

```ts
import {
  createOpenAICompatibleProvider,
  createOpenAISdkProvider,
  createAnthropicSdkProvider,
  runDirectLlmTurn,
} from "@sockudo/ai-transport/providers";
```

Built-in OpenAI-compatible presets include OpenAI, OpenRouter, Groq, Together AI, Fireworks,
DeepSeek, Perplexity, Mistral, xAI, Ollama, and LM Studio. OpenAI SDK Chat Completions, OpenAI SDK
Responses, and Anthropic SDK Messages streams are adapted into Sockudo/Vercel UI message chunks.

## AI SDK 7 Notes

The Vercel codec supports AI SDK 6 and 7 UI message chunks. AI SDK 7 agent features such as
reasoning controls, tool approvals, WorkflowAgent output, realtime voice transcripts, and generated
media references are carried as durable Sockudo turn state when they are exposed as UI message
chunks. Keep provider sessions, workflow checkpoints, tool execution context, uploaded media bytes,
and secrets in the trusted AI runtime or external storage.

## Run Demos

```bash
make demo       # Nuxt AI Transport app + Sockudo compose
```

The cold path is designed to reach a working chat in under 15 minutes on a machine with Docker, Node
20+, pnpm, and a model provider key available as `AI_GATEWAY_API_KEY` or `VERCEL_API_KEY`.

## Entry Points

- `@sockudo/ai-transport`
- `@sockudo/ai-transport/react`
- `@sockudo/ai-transport/vue`
- `@sockudo/ai-transport/svelte`
- `@sockudo/ai-transport/vercel`
- `@sockudo/ai-transport/vercel/react`
- `@sockudo/ai-transport/vercel/vue`
- `@sockudo/ai-transport/vercel/svelte`
- `@sockudo/ai-transport/providers`

Each entry ships ESM, UMD-for-CommonJS, and declaration files.

## Support and test lanes

These lanes are part of CI for the `0.1.x` release line.

| Surface        | Supported range                                                      | CI evidence                                                              |
| -------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| Node.js        | 20, 22, 24                                                           | `validate` and `integration` lanes; AI SDK 7 peers require Node 22+      |
| Browsers       | Last 2 evergreen families via Playwright engines                     | `browser-smoke` on Chromium, Firefox, WebKit against `demo/`             |
| React          | 18, 19                                                               | `react-compat` lane                                                      |
| Vue            | 3.x                                                                  | Vue composable unit tests and typed package exports                      |
| Svelte         | 5.x                                                                  | Svelte store unit tests and typed package exports                        |
| Vercel AI SDK  | `ai` v6 or v7                                                        | locked v7 dev lane plus peer range `^6 \|\| ^7`                          |
| Direct LLMs    | OpenAI SDK 6.x, Anthropic SDK 0.103.x, compatible HTTP/SSE endpoints | provider adapter unit tests                                              |
| Sockudo server | 4.x with AI Transport wire protocol v1                               | pinned integration server SHA `f66434eab44e688d3df42e56d8ebaf9aba6b1575` |
| Sockudo client | `@sockudo/client` `^2.0.0`                                           | peer dependency and integration adapter tests                            |

When `@sockudo/client` exposes server handshake feature flags, the SDK checks for `ai-transport`
during client transport construction. If a server explicitly advertises feature flags without
`ai-transport`, construction throws `ErrorInfo` code `104007` with a clear configuration error.

## Protocol Sources

- `sockudo/plans/ai-transport/02-sdk-prompts.md`
- `sockudo/docs/specs/ai-transport-wire-protocol.md`

Wire names, header keys, error codes, defaults, and public type names are frozen by those sources.

## Documentation

- `docs/README.md` for guides and quickstarts.
- `FEATURE_PARITY.md` for the generated feature parity receipt.
- `docs/api/reference` after running `pnpm docs:build`.
- `docs/release.md` for release, provenance, CDN/UMD artifact, and dependency policy notes.
- `SECURITY.md` for vulnerability disclosure.
