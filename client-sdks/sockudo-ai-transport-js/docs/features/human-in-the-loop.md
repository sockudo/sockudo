# Human-In-The-Loop

Agents can end a turn with `suspended`. A later approval continues the same `turn-id` with
`turn-continue=true` and a new invocation id. First accepted approval wins.
