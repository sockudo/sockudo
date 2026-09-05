# Core/server/SDK implementation evidence

**Current format decision:** the user removed internal format switches. Production has one current write path and automatic old-data readers. Earlier chronological paragraphs describing opt-in switches are superseded by the final checkpoint below. Public Pusher V1, Sockudo V2 and Ably contracts remain unchanged.

Baseline: immutable source snapshot and preserved audit diagnostic binaries (SHA256 manifest alongside). Original audit profile is release, LTO, one codegen unit, system allocator; timings run on shared Ryzen 7950X host, Rust 1.98.1. New raw measurements record variability and process CPU/RSS; no end-to-end gains inferred.

## Behavior-preserving refactor plans

- C5: retain insertion-order behavior for arbitrary imported history (including unsorted serials, duplicate serials and mixed streams). Track whether serials are strictly increasing within one stream; use deque binary partition seeks only while proven, otherwise retain the existing scan. Time filters remain candidate filters because wall clocks can skew. Separate per-channel locks without changing stream identity/reset semantics. Regression oracle compares observable pages to the old scan across both directions, gaps, unsorted records, skew, bounds and cursors.
- C1: replace each version vector with ordered version-serial records plus append/idempotency indexes; validate incoming identity and membership incrementally. Highest serial always wins regardless of import order. Maintain first-inserted idempotency outcome, including imports. Keep full validator for conformance, preserve all actor/terminal/cap checks, rebuild indices on purge where needed. Partition channel locking after indexed semantics are tested.
- C9: use Axum's configured limited-body path before authentication buffers bytes. Preserve accepted raw signing bytes, reject length overflow as HTTP 413, keep other read failures at existing error status. Test actual router layering and chunk polling.
- C10: schedule at most one expiry entry per active channel, reclaim only a bounded number of due records each maintenance call, retain stream/generation metadata and honor retention overrides. Never hold the scheduler lock while awaiting channel locks.
- C12: bound admission before spawning cryptographic work; retain permits inside blocking closures through cancellation. Keep algorithms/iteration floor and all token validation unchanged. Bound queued/active work separately and return explicit overload errors.

All C findings are still under implementation/verification, not closed.

## Current production and verification progress (not closure)

- C1 memory ordered indexes pass 19 version-store tests. Isolated baseline/C1-only release `state_stores` repeated 3 times; raw CSV and `/usr/bin/time -v` in results/core. SQL capped append count now lazily initialized under transaction, maintained by INSERT/DELETE triggers for imports, retention and old writers; actual PostgreSQL and MySQL counter/import/partial-retention regressions passed.
- C3 all five durable backends now use a bounded disposable incremental projection cache (64 entries, 16 MiB conservative metadata budget), authorized by authoritative revision plus exact public projection contents and earliest expiry. PostgreSQL uses transaction identity; MySQL installs an update revision trigger to cover older writers. Normal in-order writes update canonical engines, duplicates reuse outcomes, conflict/import/expiry/purge rebuild. PostgreSQL two-node annotation conformance passed against local PG17. All-backend test launched; global rebuild admission now rejects above four concurrent cold rebuilds before loading event payloads; additional cache regression/bench remain.
- C4 memory serial-aware eviction and durable contiguous-coverage cache pass 27 presence tests including remote-node state change, late reservation commit, and 32 concurrent duplicates. Noncontiguous retained intervals conservatively rebuild; cached misses never establish absence without coverage. Extra benchmark and real durable checks pending.
- C7 selective bounded batch overrides all five durable backends compile; added payload-free `message_count` trait/leased forwarding, SQL joins, Dynamo paginated Count, Scylla Count and Surreal aggregate for HTTP AI stats. Active-stream metadata optimization remains pending. Runtime subset/count fixtures pending.
- C9 actual router limited-body helper regression passes all 5 middleware tests, exact bytes and chunk polling checked. Full signed middleware request + resource benchmark pending.
- C11 all 51 SDK tests pass against local scripted TCP servers, including retry bytes/MD5/header equality, stable idempotency and jitter bounds. Feature combinations and isolated benchmarks still pending.
- Selected server check with v2,push,postgres,mysql,dynamodb,scylladb,surrealdb,ably-compat completed successfully after cache changes (integration-check-3.txt).

## C2 representation plan and partial implementation

Memory now stores each historical string revision as metadata plus a byte length into a shared append-run snapshot. The snapshot only grows while it still matches the exact predecessor and incoming fragment; mismatching/out-of-order imports use an independent snapshot. Latest public state remains cached. Every original append fragment, envelope field and serial remains in its record; reads materialize the exact prefix and envelope text. A reset/update starts another run. Existing 19 version-store tests pass; dedicated 512-append UTF-8 storage accounting/purge/random historical regression passed.

Durable implementation uses private `SVR2\0` storage entries referencing a growing per-append-run UTF-8 text and prefix length. Readers accept V1/V2; new writers require `[versioned_messages].compact_storage_v2=true` (default false, environment `VERSIONED_MESSAGES_COMPACT_STORAGE_V2`). SQL stores indexed snapshot rows; Dynamo uses a sparse snapshot item in the version-entry table; Scylla uses the same atomic channel LWT partition and compact legacy projection rows; Surreal uses a snapshot table in the mutation transaction. Latest-state caches remain full. SQL/Surreal garbage collection checks remaining references. Document receipts that have no retention also pin their snapshot indefinitely to preserve the preexisting receipt contract. Scylla atomic rows currently have no TTL, so their snapshots preserve that lifetime rather than expiring prematurely.

Actual PostgreSQL and MySQL tests passed, preserving exact UTF-8 historical/envelope records and counters through imported older versions, duplicate imports and partial purge. DynamoDB Local and Scylla tests passed 128 appends, full replay, selective reads, competing writers, first receipt replay and restart with the compact writer gate disabled. Scylla retained entry bytes are checked against equivalent full serialization and one snapshot per run. Live tests exposed and corrected Scylla multi-row LWT result handling, MySQL trigger simple-protocol execution, Dynamo structured transaction cancellation detection, and Surreal conditional-update/transaction-error handling. Surreal final rerun, more retention cases, codec unit tests, isolated before/after performance and rollback operational guidance remain pending. No C2 closure is claimed.

## Latest continuation and simplified upgrade path

The user explicitly requested one internal storage/queue write path, while preserving public
Pusher V1, Sockudo V2 and Ably compatibility. Removed the newly added compact-storage config/env
switch and backend builder booleans. Compact append planning is automatic; legacy full records
remain readable without operator selection. Queue, lifecycle and stats mode switches are being
removed by the push/Ably owner. Upgrade docs now require coordinated reader/writer upgrade and
compatible-reader rollback; there is no flag-based rollback or silent data conversion.

- C2: `c2-imports-live-2.txt` passed DynamoDB, MySQL, PostgreSQL, ScyllaDB and SurrealDB compact
  restart/fencing checks. Added Dynamo/Surreal older/newer raw imports, duplicate count stability,
  cap rejection at131, acceptance at132, exact replay and delivery floor1002. Surreal partial
  purge initially decoded the wrong DELETE return shape; `c2-surreal-gc-live-2.txt` passes the
  corrected actual-store test (6.85s), including original receipt replay after partial/full purge
  and restart. Final one-write-path rerun is still required. Codec3tests passed earlier.
- C3: independent review identified MySQL lock updates invalidating the cache and permits ending
  before persistence. Fixed no-op lock timestamp semantics with exact known trigger revision
  adjustment; cold rebuild permits now survive persistence. SQL readers stream; document readers
  bound retained decode input; Surreal pages128 records. Each read has128MiB conservative accounting,
  with4 concurrent rebuilds. Cache-hit entries retain admission through persistence to cap stalled
  active hits at64 per cache; explicit refill/remove saturation regression added. Final tests and
  measured reuse are still required.
- C4/C6/C8 are being finished and measured by services/fanout owners; consult their updated logs.
- C9: added actual signed middleware request with whitespace/UTF-8, chunked input, exact downstream
  bytes, spoofed content length, over-limit rejection and invalid signature. Final run pending.
- C11: default51, no-default51 and native-tls51 SDK tests passed. Identical actual HTTP allocation,
  exact body/MD5, attempt count and retry-arrival benchmark prepared against unchanged SDK and
  C11-only source variants; final release builds in progress. No C11 timing claim yet.
- C12: five isolated baseline/after pairs preserve exact accepted-result accounting. At64 offered
  and64 completed verifications, baseline elapsed median571258us [563937,663639] and maximum healthy
  timer delay median570257us [562938,662638]; after elapsed76858us [75783,82210] and timer delay1608us
  [1520,1874]. At192 offered the bounded implementation accepts64/rejects128 explicitly; unequal
  accepted counts prohibit a throughput comparison for that overload shape. Actual120k PBKDF2 cost
  is unchanged. Commands, hashes, CPU/RSS and5repetitions are in `c12-manifest.json` and raw CSV/stderr.

Remaining root gates: isolated C2/C3/C7/C9/C10/C11 benchmarks, active-stream stats metadata path,
final all-backend tests after removing internal switches, cache admission regressions, global
workspace/feature/Clippy/conformance/docs verification, and final cross-review. No finding is
closed solely from the implementation or a passing compile.

## Final isolated core measurements checkpoint (still not task closure)

`run-core-isolated.py c9 c10 c2 c3` completed five alternating process pairs for
all four findings. `results/core/{c2,c3,c9,c10}-isolated-manifest.json` records
commands, host, binary hashes and exit status; raw outputs and CPU/RSS are beside
it. `summarize-isolated-core.py` produces `isolated-core-summary.json`, including
min/median/max. Each pair has identical standalone dependency locks and release
profiles. Baseline production remains unchanged; source variants apply only the
named finding. The C2-only variant deliberately excludes C1 indexes/latest cache.

- C2 memory: 2,000 UTF-8 appends retain 131,415,582 baseline allocation bytes versus
  3,530,726 after (medians). Append elapsed is 191,161us versus 166,151us. Every one
  of the 2,001 public versions is paged in groups of37 and checked for exact text,
  original fragment, actor, message/version/history/delivery identity and latest
  result. These are live requested allocation sizes, not RSS; the quadratic public
  sum of128,064,000 text bytes remains exactly reconstructible. Durable backend
  storage/receipt/restart tests are separate evidence, not an end-to-end timing claim.
- C3 memory: at10,000 retained events, duplicate p50 median2,585,345ns
  [2,526,634,2,605,473] becomes1,623ns [1,593,1,683]. Requested allocation bytes per
  duplicate1,389,825 becomes2,134. Each sample asserts exact total and last serial;
  101 measured samples per shape per process. Five actual durable backend reuse
  probes are prepared; durable performance verification remains open.
- C9 actual middleware: a64MiB offered chunked body with64KiB configured limit
  previously polls67,108,864bytes, buffers it, then returns404 for the absent app.
  After, it polls69,632bytes (limit plus one4KiB input chunk) and returns413.
  Across35 samples, elapsed median3550us [3287,5008] becomes2us [1,3]. Under-limit
  requests preserve their404 outcome and consume exact input bytes. This is early
  rejection work, not accepted-request throughput. The separately signed route test
  passed (`single-path-c9-integration-2.txt`), checking authentic raw-body identity.
- C10:10,000 idle channels containing16KiB payloads retain all10,000 owners in the
  unchanged baseline; after bounded128-record maintenance, zero remain. Cleanup
  median5864us; baseline no-op1us is not faster equivalent work. The same stream ID
  and next serial2 survive. RSS remains approximately179MiB after free because the
  allocator retains its pages; no process-RSS reduction is claimed. The production
  weak-owner regression passes1/1 before any lazy read can evict data
  (`c10-payload-owner-regression.txt`).
- C11 actual loopback HTTP: five alternating pairs, identical64KiB/1MiB bodies and
  exact attempt/MD5/data assertions.64KiB success requested allocation bytes
  1,029,505→898,587;1MiB24,164,166→22,067,443. Success latency is nearly unchanged
  (316→313us and4682→4604us). Retry elapsed reductions mainly reflect the specified
  jitter interval; they are not serialization-speed claims.51tests passed under
  default, no-default and native-tls configurations. See`c11-manifest.json` and
  `c11-summary.json`.
- C3 authority/admission final focused runs: two-node tests passed5/5 against actual
  PostgreSQL/MySQL/DynamoDB Local/ScyllaDB/SurrealDB (`c3-all-backends-final.txt`).
  Admission/refill and decode-byte-bound tests passed2/2 (`c3-admission-regressions.txt`).
- C6/C10 maintenance outer-loop tests passed2/2; zero-delete/has-more responses and
  partial errors cannot exceed the configured number of storage calls or row budget.
- C7/C2 Surreal review found legacy optional payload caches could lag the authoritative
  latest pointer. Cache reuse now verifies all record identities before reads or
  mutation; explicit stale-cache injection regression added, final runtime run pending.

Still open: C2 durable per-fix measurements/fault matrix; C3 durable reuse measurements;
C7 batch shape/operation measurements and active-stream metadata; final integrated
workspace/Clippy/features/conformance/docs checks and independent final review.

## Metadata and shared-client follow-up (verification pending)

C7 now has `VersionStore::active_stream_count` and leased forwarding; the AI
handler calls it instead of fetching every latest body. Memory inspects shared
latest metadata. DynamoDB reads bounded strongly-consistent metadata pages,
falling back to exact point reads for old records without the boolean. SQL and
SurrealDB record which latest version their boolean describes; legacy or stale
metadata is resolved through authoritative point reads and repaired with a latest
version condition. ScyllaDB merges two paged, ordered identity streams (atomic and
legacy) with constant extra state, preferring atomic rows and point-reading only
legacy statuses. This also fixes mixed legacy/current logical counts. No operator
format option or additional state store was introduced. New205-message tests cover
multiple pages, large bodies, out-of-order imports, terminal updates and explicit
legacy metadata invalidation; they have not run yet. Surreal stale-payload-cache
injection test also still awaits its final build.

Shared Redis TCP construction now setsTCP_NODELAY for standalone, Sentinel and
cluster connections. The A1 actual Redis probe observed206ms pipeline stalls and
250ms freshness rejection with the library default; an equivalent direct-constructor
sparse probe removes those stalls without changing the freshness deadline, but dense
1,280-record pages still fail the same deadline. TCP_NODELAY is provisional while
the pipelined-read behavior is investigated; no resolved-A1 claim is supported. P/A retains
unchanged baseline, A1-only (including failure), TCP-only and integrated variants;
measurements and actual shared-factory Ably regressions remain separately labeled.
The full195 Ably Redis regressions passed through the shared factory after this
change. Existing TLS metadata test now checks the flag survives the TLS upgrade.

First required workspace run failed13 local-socket tests because the sandbox denied
listeners. The authorized socket-enabled rerun `workspace-tests-final-2.txt` exited0
and passed; later metadata edits require a final rerun. Documentation type check
passed (`docs-types-final.txt`); the production build and final docs rerun remain.
Database-only feature checks exposed missing outer Scylla/Surreal version-store
module gates; these are fixed. Fanout fixed its optional-rand dependency issue by
using already-required UUID randomness for bounded retry jitter.

## Final cross-review checkpoint

Services independently reviewed all five active-stream metadata paths. PostgreSQL,
MySQL, DynamoDB and Scylla205-message/legacy tests passed in
`c7-metadata-live-1.txt`. Surreal fixture metadata invalidation returned a large
unused payload and reset its WebSocket; fixture and lazy-repair updates now request
`RETURN NONE`, awaiting rerun. Review found Scylla legacy pointers can survive their
TTL payload; metadata counts now probe retained version identity without loading
payloads and have an explicit stale-pointer test.

Fanout independently reviewed C2 snapshot/CAS/GC and receipt pinning across all five
backends, with no additional concrete correctness defect. Retained append-run text
is linear; cumulative growing snapshot/latest write bytes are still quadratic.
The codec component records those costs separately and does not claim lower wire
write volume or end-to-end append throughput. A deterministic DynamoDB SDK HTTP
fault regression now checks partial BatchGet retries and all-or-error exhaustion;
it still awaits compilation/runtime and is distinct from real-backend coverage.

## Completed additional measurement and regression pass

All thirteen current version-store fixture tests passed (`version-final-live-1.txt`),
covering all five databases, stale cached payload/metadata, out-of-order imports,
retention/receipts, restart/fencing, and partial DynamoDB batch retry/exhaustion.
Minimal and full server builds passed; AI benchmark binary built; offline AI
fixtures passed. Documentation webpack production build and refreshed types passed;
Turbopack stalled and was terminated. Final workspace checks use a dedicated fresh
build directory, separate from all source-isolated variants.

C3 three process pairs per database (45 exact duplicate samples per shape/side)
passed on all five databases. At1,000 retained annotations median microseconds:
Postgres1909→306; MySQL2862→2721; Dynamo376→325; Scylla221→251; Surreal435→422.
Scylla duplicate latency regressed30us with overlapping sample ranges; retain that
cost rather than asserting universal speedups. Projection fencing/admission remains
required for correct bounded caching. Raw records, process resources and variability
are in `c3-durable-*-manifest.json` and `durable-core-summary.json`.

C2 five shared codec component pairs preserve all2,001 historical representations:
entry bytes128,976,158→1,198,424 plus128,000bytes of shared snapshot at2,000 appends.
This is representation storage, not physical database disk or network volume; the
manifest and CSV also expose cumulative latest/snapshot writes and encode/replay
costs. Three integrated current state-store runs passed the unchanged diagnostic
oracles (`state_stores-integrated-*.csv`), supplementing individual C1/C2/C3/C5 probes.

The provisional TCP_NODELAY change was removed. Independent host-network Redis
measurements identify the rootless forwarding fixture as the dense A1 stall source;
P/A owns final same-network before/after confirmation. Public wire protocols remain
unchanged and production has no operator-selectable internal format modes.
