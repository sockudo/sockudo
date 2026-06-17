# 0.1 Public API Parity Diff

Source of truth: `plans/ai-transport/02-sdk-prompts.md` section 1 and
`docs/specs/ai-transport-wire-protocol.md`.

| Surface                  | Sockudo 0.1 status | Notes                                                          |
| ------------------------ | ------------------ | -------------------------------------------------------------- |
| Entry points             | ✅                 | `.`, `/react`, `/vue`, `/svelte`, `/vercel/*`, `/providers`    |
| Realtime substrate       | ✅                 | `@sockudo/client` peer dependency only through `src/realtime/` |
| Client factory           | ✅                 | `createClientTransport`                                        |
| Server factory           | ✅                 | `createServerTransport`                                        |
| Server turn API          | ✅                 | `newTurn`, `start`, `addMessages`, `streamResponse`, `end`     |
| Turn reasons             | ✅                 | `complete`, `cancelled`, `error`, `suspended`                  |
| Client transport surface | ✅                 | tree/view/cancel/wait/stage/close/error                        |
| View surface             | ✅                 | send/edit/regenerate/branch navigation/history                 |
| ActiveTurn               | ✅                 | stream, ids, optimistic ids, cancel                            |
| CancelFilter             | ✅                 | `turnId`, `own`, `clientId`, `all`; default own                |
| Codec core               | ✅                 | encoder/decoder/accumulator/lifecycle helpers                  |
| Vercel helpers           | ✅                 | UIMessage codec, chat transport, turn-end reason               |
| React helpers            | ✅                 | providers, view hooks, active turns, message sync              |
| Vue helpers              | ✅                 | composables, view refs, active turns, Vercel chat transport    |
| Svelte helpers           | ✅                 | stores, view stores, active turns, Vercel chat transport       |
| Direct provider helpers  | ✅                 | OpenAI SDK, OpenAI-compatible HTTP/SSE, Anthropic SDK          |
| Error surface            | ✅                 | `ErrorInfo`, `ErrorCode`, `errorInfoIs`                        |

Public API freeze is enforced by `pnpm api:snapshot`.
