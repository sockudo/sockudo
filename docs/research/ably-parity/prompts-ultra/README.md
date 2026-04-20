# Ultra Prompt System

This is the most granular execution pack in the Ably parity roadmap.

Use this pack when you want:

- maximum implementation control
- explicit migration and backfill prompts
- explicit cluster/distributed correctness prompts
- explicit SDK or release-engineering prompts
- stronger separation between runtime work, docs work, ops work, and release sign-off

Prompt count:

- `12` prompts per release
- `4` releases total (4.3 completed, 3 remaining)
- `36` prompts remaining

Prompt structure per release:

1. architecture and release contract
2. domain model and invariants
3. storage schema, migrations, and backfill
4. protocol/API/wire contract
5. auth, capabilities, and security
6. server/runtime implementation
7. cluster and distributed correctness
8. client and SDK behavior
9. framework integration and release engineering
10. test matrix and automation
11. observability, docs, and operator guidance
12. final verification and release audit

## Release map

| File | Feature | Status |
|---|---|---|
| `release-4.3.md` | Versioned Durable Messages | Completed |
| `release-4.4.md` | Message Annotations | Next |
| `release-4.5.md` | Push Notification Platform | — |
| `release-4.6.md` | AI Transport (core + interaction + SDK + hardening) | — |

## Dependencies

- 4.3 must be complete before starting 4.4 or 4.6
- 4.4 must be complete before starting 4.6 (annotation channel rule enables message persistence that AI sessions depend on)
- 4.5 must be complete before finishing 4.6 (push re-engagement for offline AI session continuation)

## Execution order

Run prompts within a release in sequence (01 → 12). Do not skip prompt 01 on any release —
it contains the release contract that binds the rest of the implementation.

Do not advance to the next release until the final verification prompt (12) delivers a go decision.
