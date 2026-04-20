# Production Prompt System

This folder contains the expanded production-grade prompt system for the Ably parity roadmap.

It is intentionally more granular than the original `prompts/` pack.

Use this pack when you want:

- finer execution control
- better separation of architecture, storage, protocol, auth, SDK, docs, and ops work
- higher confidence that implementation details are not skipped
- prompts that are suitable for long multi-step release execution

Prompt count:

- `10` prompts per release
- `7` releases
- `70` prompts total

Releases:

- `release-4.3.md`
- `release-4.4.md`
- `release-4.5.md`
- `release-4.6.md`
- `release-4.7.md`
- `release-4.8.md`
- `release-4.9.md`

Execution recommendation:

1. Run prompts in order within a release.
2. Do not skip architecture, verification, or audit prompts.
3. Treat each prompt as mandatory unless the referenced scope is already fully implemented and verified.
4. If a prompt produces artifacts such as design notes, acceptance criteria, or parity matrices, keep them in-repo and use them as binding context for the later prompts in that same release.

Prompt shape per release:

1. architecture and release contract
2. storage/data model
3. protocol/API/wire contract
4. auth/capabilities/security
5. server/runtime implementation
6. client/SDK/framework implementation
7. test matrix and automation
8. observability/ops/admin surfaces
9. docs/examples/migration guidance
10. final verification and release audit
