# Direct Providers

`@sockudo/ai-transport/providers` adapts direct provider streams into Sockudo AI Transport UI
message chunks. Use it when your agent calls provider SDKs or OpenAI-compatible endpoints directly.

## OpenAI-Compatible HTTP

```ts
import { createOpenAICompatibleProvider, runDirectLlmTurn } from "@sockudo/ai-transport/providers";

const provider = createOpenAICompatibleProvider({
  provider: "groq",
  apiKey: process.env.GROQ_API_KEY,
  model: "llama-3.3-70b-versatile",
});

await runDirectLlmTurn(turn, provider, {
  model: "llama-3.3-70b-versatile",
  prompt: "Answer over Sockudo AI Transport.",
});
```

Provider presets include OpenAI, OpenRouter, Groq, Together AI, Fireworks, DeepSeek, Perplexity,
Mistral, xAI, Ollama, and LM Studio. Override `baseURL` for any other compatible endpoint.

## OpenAI SDK

```ts
import OpenAI from "openai";
import { createOpenAISdkProvider } from "@sockudo/ai-transport/providers";

const provider = createOpenAISdkProvider({
  client: new OpenAI({ apiKey: process.env.OPENAI_API_KEY }),
  mode: "responses",
  model: "gpt-4.1-mini",
});
```

Use `mode: "chat"` for Chat Completions streams or `mode: "responses"` for Responses streams.

## Anthropic SDK

```ts
import Anthropic from "@anthropic-ai/sdk";
import { createAnthropicSdkProvider } from "@sockudo/ai-transport/providers";

const provider = createAnthropicSdkProvider({
  client: new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY }),
  model: "claude-sonnet-4-5",
});
```

The adapters are structural, so compatible SDK client objects can be supplied without this package
importing those SDKs directly.
