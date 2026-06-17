# Turns

A turn starts with `ai-turn-start`, streams output through mutable `sockudo:message.*` operations,
and ends with `ai-turn-end`. End reasons are `complete`, `cancelled`, `error`, and `suspended`.

Agents must call `end()` in a `finally` block or map stream results through `vercelTurnEndReason` so
clients do not keep turns active forever.
