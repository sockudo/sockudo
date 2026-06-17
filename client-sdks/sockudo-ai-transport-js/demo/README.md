# Sockudo AI Transport Nuxt Demo

This is a full Nuxt/Vue demo for `@sockudo/ai-transport` and the Vercel AI SDK transport surface. It
showcases streaming, branch-aware views, edit/regenerate, cancellation, history replay, raw Sockudo
event inspection, active turn tracking, versioned message proxying, recovery-friendly channel
history, and multi-device synchronization.

The demo consumes the monorepo-local `client-sdks/sockudo-js` workspace package for
`@sockudo/client`; it does not depend on the last published npm package.

## Environment

For live Vercel AI Gateway calls, set:

```bash
export AI_GATEWAY_API_KEY="your_vercel_ai_gateway_key"
```

The route also accepts `VERCEL_API_KEY` as a legacy fallback because older Sockudo demo docs
mentioned it, but `AI_GATEWAY_API_KEY` is the current Vercel AI Gateway variable and is preferred.
Without a key, the demo still runs a deterministic local stream through the same Sockudo transport.

Optional configuration:

```bash
export SOCKUDO_DEMO_MODEL="openai/gpt-5-mini"
export SOCKUDO_APP_ID="demo-app"
export SOCKUDO_APP_KEY="demo-key"
export SOCKUDO_APP_SECRET="demo-secret"
export SOCKUDO_CHANNEL_NAME="private-ai-nuxt"
export SOCKUDO_HOST="127.0.0.1"
export SOCKUDO_PORT="6001"
```

## Run

From `client-sdks/sockudo-ai-transport-js`:

```bash
pnpm install
pnpm build
pnpm demo
```

Open `http://127.0.0.1:5174`. Open a second browser tab at the same URL to see multi-device fanout,
recovery, and shared branch state.
