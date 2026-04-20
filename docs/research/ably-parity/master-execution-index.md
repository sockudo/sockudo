# Master Execution Index

## Purpose

This is the master run order for the Sockudo Ably parity program.

Use this file when you want:

- one authoritative execution sequence
- release gates and dependency gates
- a recommended prompt order across the entire program
- clear stop/go criteria between releases

This index assumes you will use the most granular prompt system:

- `prompts-ultra/`

Fallback systems:

- `prompts-production/` for strong but less granular control
- `prompts/` for compact release-complete control

## Recommended execution mode

For highest confidence:

1. use `prompts-ultra/`
2. execute prompts in order within a release
3. do not start the next release until the current release has passed its final audit prompt
4. if a release fails its audit, loop back only to the prompt category that failed, then rerun final verification

## Global order

### Phase 1: Mutable message substrate

Run release `4.3` first.

Mandatory order:

1. `4.3-01`
2. `4.3-02`
3. `4.3-03`
4. `4.3-04`
5. `4.3-05`
6. `4.3-06`
7. `4.3-07`
8. `4.3-08`
9. `4.3-09`
10. `4.3-10`
11. `4.3-11`
12. `4.3-12`

Release gate:

- do not start `4.4` or `4.6` until `4.3-12` returns a go decision

### Phase 2: Annotation layer

Run release `4.4`.

Mandatory order:

1. `4.4-01`
2. `4.4-02`
3. `4.4-03`
4. `4.4-04`
5. `4.4-05`
6. `4.4-06`
7. `4.4-07`
8. `4.4-08`
9. `4.4-09`
10. `4.4-10`
11. `4.4-11`
12. `4.4-12`

Release gate:

- do not reuse annotations for AI transcript overlays until `4.4-12` is go

### Phase 3: Push platform

Run release `4.5`.

Mandatory order:

1. `4.5-01`
2. `4.5-02`
3. `4.5-03`
4. `4.5-04`
5. `4.5-05`
6. `4.5-06`
7. `4.5-07`
8. `4.5-08`
9. `4.5-09`
10. `4.5-10`
11. `4.5-11`
12. `4.5-12`

Release gate:

- do not rely on push for AI background completion or approval wakeups until `4.5-12` is go

### Phase 4: AI Transport core

Run release `4.6`.

Mandatory order:

1. `4.6-01`
2. `4.6-02`
3. `4.6-03`
4. `4.6-04`
5. `4.6-05`
6. `4.6-06`
7. `4.6-07`
8. `4.6-08`
9. `4.6-09`
10. `4.6-10`
11. `4.6-11`
12. `4.6-12`

Release gate:

- do not start advanced AI interaction work until `4.6-12` is go

### Phase 5: Advanced AI interaction

Run release `4.7`.

Mandatory order:

1. `4.7-01`
2. `4.7-02`
3. `4.7-03`
4. `4.7-04`
5. `4.7-05`
6. `4.7-06`
7. `4.7-07`
8. `4.7-08`
9. `4.7-09`
10. `4.7-10`
11. `4.7-11`
12. `4.7-12`

Release gate:

- do not expose broad public SDKs until `4.7-12` is go

### Phase 6: Public SDK and framework platform

Run release `4.8`.

Mandatory order:

1. `4.8-01`
2. `4.8-02`
3. `4.8-03`
4. `4.8-04`
5. `4.8-05`
6. `4.8-06`
7. `4.8-07`
8. `4.8-08`
9. `4.8-09`
10. `4.8-10`
11. `4.8-11`
12. `4.8-12`

Release gate:

- do not claim platform readiness before `4.8-12` is go

### Phase 7: Hardening and parity sign-off

Run release `4.9`.

Mandatory order:

1. `4.9-01`
2. `4.9-02`
3. `4.9-03`
4. `4.9-04`
5. `4.9-05`
6. `4.9-06`
7. `4.9-07`
8. `4.9-08`
9. `4.9-09`
10. `4.9-10`
11. `4.9-11`
12. `4.9-12`

Program gate:

- the full parity program is only complete if `4.9-12` is go and the final parity matrix is honest and complete

## Release dependencies

- `4.3` blocks `4.4`
- `4.3` blocks `4.6`
- `4.5` is independent enough to run before or alongside early `4.6` architecture, but should be completed before `4.7` if AI push wakeups matter
- `4.6` blocks `4.7`
- `4.6` blocks `4.8`
- `4.7` blocks `4.8` if branching/tool/HITL are part of the public SDK promise
- `4.8` blocks `4.9`

## Parallelization guidance

Safe partial parallelization:

- during a given release, docs/examples work can begin after protocol shape stabilizes
- observability planning can begin after runtime architecture stabilizes
- test-matrix planning can begin immediately after acceptance criteria are written

Unsafe parallelization:

- do not start client/SDK implementation before protocol and auth rules are stable
- do not start cluster hardening before the runtime data model is stable
- do not start migration/backfill work before storage shape is finalized

## Recommended artifacts per release

Every release should leave behind:

- one release contract
- one acceptance checklist
- one implementation note or ADR if architecture was non-trivial
- one migration/backfill note where applicable
- one final audit artifact

## Cadence recommendation

Two weeks between releases is only good if "between releases" means:

- implementation complete
- verification complete
- docs complete
- release audit passed

Recommended cadence:

- `4.3`: `3-4` weeks
- `4.4`: `2-3` weeks
- `4.5`: `3-5` weeks
- `4.6`: `4-6` weeks
- `4.7`: `4-6` weeks
- `4.8`: `3-4` weeks
- `4.9`: `3-5` weeks

If you force a flat two-week cadence for every release, the highest-risk failures will likely come from:

- `4.5` push platform incompleteness
- `4.6` transport durability gaps
- `4.7` transcript-tree and concurrency bugs
- `4.9` insufficient hardening

## Files to use

- Roadmap: [sockudo-ably-parity-release-plan.md](/Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/sockudo-ably-parity-release-plan.md)
- Research pack index: [README.md](/Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/README.md)
- Ultra prompt index: [prompts-ultra/README.md](/Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/research/ably-parity/prompts-ultra/README.md)
