# Transport

The transport layer owns turns, cancellation, history, and tree synchronization. It does not own
WebSocket protocol logic; `@sockudo/client` supplies connection, recovery, channel auth, history,
presence, and mutable-message helpers.

Client sends publish `ai-input`, then poke the application API. Agents publish `ai-output`,
`ai-turn-start`, and `ai-turn-end` with trusted credentials.
