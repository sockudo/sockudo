# @sockudo/ai-transport

## 2.1.0 - 2026-06-27

- Hardened adapter-level forward-compatibility over `@sockudo/client` by replaying the shared E1
  fixtures, preserving `extras.ai`, keeping unsafe serials as `number | string`, skipping event-less
  frames, and treating future internal mutable actions as no-op summary frames.

## 0.1.0

Initial GA release candidate for Sockudo AI Transport wire protocol v1.

- Core client/server transport APIs with Ably AI Transport parity.
- React, Vue, Svelte, Vercel framework, and direct provider subpath exports.
- Direct OpenAI SDK, OpenAI-compatible HTTP/SSE, Anthropic SDK, and common compatible provider
  presets.
- Versioned-message streaming, cancellation, history/recovery, branching, and view helpers.
- Public API snapshot, bundle budgets, benchmark guard, and dry-run release checks.
