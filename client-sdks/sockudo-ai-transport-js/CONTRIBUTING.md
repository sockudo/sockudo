# Contributing

## Architecture

```text
src/
  constants.ts errors.ts event-emitter.ts logger.ts utils.ts version.ts index.ts
  realtime/            # Thin adapter over @sockudo/client; the only layer allowed to import it.
    adapter.ts types.ts mocks.ts
  core/
    codec/             # Domain event <-> channel operation mapping.
    transport/         # Turns, lifecycle, cancel, history, sync, tree, and views.
  react/               # React hooks and providers.
  vercel/              # Vercel AI SDK codec and transport.
    react/             # Vercel React hooks/providers.
```

The transport layer manages turns, lifecycle, cancel, history, and sync. It does not inspect domain
payloads. The codec layer maps domain events to channel operations. It does not manage turns.

## Boundaries

- `@sockudo/client` is a required peer dependency and may only be imported from `src/realtime/`.
- `src/core/` must not import React or Vercel entry layers.
- `src/realtime/` must not import `src/core/transport`.
- Source imports use ESM `.js` extensions.

## Verification

Run these before sending a change:

```bash
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm smoke:exports
pnpm size
pnpm api:snapshot
```

Integration tests live under `test/integration` and are skipped unless the Sockudo server
environment is configured.
