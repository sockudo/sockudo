# Troubleshooting

| Symptom                          | Likely cause                                              | Fix                                                                       |
| -------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------- |
| Empty assistant message          | Mutable messages disabled; server returns `93002`.        | Enable the mutable-message namespace for the AI channel.                  |
| History load fails               | Missing `history` capability.                             | Issue a token with `history` for the session channel.                     |
| Turn never ends                  | Agent did not publish `ai-turn-end`.                      | Call `end()` in `finally`; Vercel paths should use `vercelTurnEndReason`. |
| Duplicate turns                  | React strict mode or edit-mid-stream double send.         | Keep transport handles stable and cancel before editing active output.    |
| Cross-client cancellation denied | Local client id was trusted instead of verified identity. | Authorize against server-verified `clientId`.                             |
| Suspended state does not resume  | Continuation was published without `turn-continue=true`.  | Continue the same `turn-id` with a new `invocation-id`.                   |
