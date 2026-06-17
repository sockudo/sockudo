# Cancellation

Clients publish `ai-cancel` with `{ turnId }`, `{ own: true }`, `{ clientId }`, or `{ all: true }`.
The default filter is `{ own: true }`; agents should authorize cancellation using verified server
identity.
