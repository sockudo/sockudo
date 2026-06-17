# Sockudo AI Transport Docs

`@sockudo/ai-transport` follows the `@ably/ai-transport` public API shape while targeting Sockudo's
AI Transport wire protocol. Parity status is tracked in
[`../FEATURE_PARITY.md`](../FEATURE_PARITY.md).

## Quickstarts

- Nuxt command-center demo: [`../demo`](../demo)
- Vercel AI SDK transport: `@sockudo/ai-transport/vercel`
- Core branch/tree API: `@sockudo/ai-transport`
- Vue composables: `@sockudo/ai-transport/vue` and `@sockudo/ai-transport/vercel/vue`
- Svelte stores: `@sockudo/ai-transport/svelte` and `@sockudo/ai-transport/vercel/svelte`
- Direct providers: `@sockudo/ai-transport/providers`

Run the default chat demo:

```bash
make demo
```

## Concepts

- [Sessions](concepts/sessions.md)
- [Turns](concepts/turns.md)
- [Transport](concepts/transport.md)
- [Codec](concepts/codec.md)
- [Conversation tree](concepts/tree.md)

## Guides

- [Custom codec](guides/custom-codec.md)
- [Direct providers](guides/direct-providers.md)
- [Errors](guides/errors.md)
- [Troubleshooting](guides/troubleshooting.md)
- [Generated snippets](snippets/generated.md)

Snippet references used by these docs:

- snippet:usechat-provider
- snippet:usechat-sync
- snippet:usechat-route
- snippet:core-provider
- snippet:core-branch-views
- snippet:core-route
