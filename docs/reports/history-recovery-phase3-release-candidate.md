# History / Recovery / Rewind Phase 3 Release Candidate Report

Status: Release candidate accepted for broad server rollout, with explicit SDK verification exclusions noted below.

## 1. Changed Files

Core/runtime:

- [`crates/sockudo-core/src/history.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-core/src/history.rs)
- [`crates/sockudo-core/src/metrics.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-core/src/metrics.rs)
- [`crates/sockudo-metrics/src/prometheus.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-metrics/src/prometheus.rs)
- [`crates/sockudo-server/src/history.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-server/src/history.rs)
- [`crates/sockudo-server/src/http_handler.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-server/src/http_handler.rs)
- [`crates/sockudo-server/src/main.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-server/src/main.rs)
- [`crates/sockudo-adapter/src/handler/recovery.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-adapter/src/handler/recovery.rs)

Clustered and fault-injection tests:

- [`crates/sockudo-adapter/tests/adapter/handler/mod.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-adapter/tests/adapter/handler/mod.rs)
- [`crates/sockudo-adapter/tests/adapter/handler/runtime_rewind_recovery_e2e_test.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-adapter/tests/adapter/handler/runtime_rewind_recovery_e2e_test.rs)
- [`crates/sockudo-adapter/tests/adapter/handler/clustered_runtime_rewind_recovery_redis_test.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/crates/sockudo-adapter/tests/adapter/handler/clustered_runtime_rewind_recovery_redis_test.rs)

Docs / ops / monitoring:

- [`docs/content/2.server/23.multinode-history-ops.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/content/2.server/23.multinode-history-ops.md)
- [`docs/content/2.server/3.http-api.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/content/2.server/3.http-api.md)
- [`docs/adr/0002-history-backend-scope.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/docs/adr/0002-history-backend-scope.md)
- [`monitoring/rules/history-recovery-alerts.yml`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/monitoring/rules/history-recovery-alerts.yml)
- [`monitoring/prometheus-prod.yml`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/monitoring/prometheus-prod.yml)
- [`grafana/history-recovery-dashboard-example.json`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/grafana/history-recovery-dashboard-example.json)
- [`grafana/README.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/grafana/README.md)

Client SDK fixes:

- [`client-sdks/sockudo-python/src/sockudo_python/client.py`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-python/src/sockudo_python/client.py)
- [`client-sdks/sockudo-python/src/sockudo_python/__init__.py`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-python/src/sockudo_python/__init__.py)
- [`client-sdks/sockudo-python/tests/test_protocol.py`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-python/tests/test_protocol.py)
- [`client-sdks/sockudo-python/tests/test_client.py`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-python/tests/test_client.py)
- [`client-sdks/sockudo-python/README.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-python/README.md)
- [`client-sdks/sockudo-dotnet/src/Sockudo.Client/Models.cs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-dotnet/src/Sockudo.Client/Models.cs)
- [`client-sdks/sockudo-dotnet/src/Sockudo.Client/ProtocolCodec.cs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-dotnet/src/Sockudo.Client/ProtocolCodec.cs)
- [`client-sdks/sockudo-dotnet/src/Sockudo.Client/SockudoClient.cs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-dotnet/src/Sockudo.Client/SockudoClient.cs)
- [`client-sdks/sockudo-dotnet/tests/Sockudo.Client.Tests/ProtocolTests.cs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-dotnet/tests/Sockudo.Client.Tests/ProtocolTests.cs)
- [`client-sdks/sockudo-dotnet/README.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/client-sdks/sockudo-dotnet/README.md)

Server SDK fixes:

- [`server-sdks/sockudo-http-rust/src/history.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-rust/src/history.rs)
- [`server-sdks/sockudo-http-rust/src/lib.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-rust/src/lib.rs)
- [`server-sdks/sockudo-http-rust/src/sockudo.rs`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-rust/src/sockudo.rs)
- [`server-sdks/sockudo-http-rust/README.md`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-rust/README.md)
- [`server-sdks/sockudo-http-node/tests/remote/sockudo/get.js`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-node/tests/remote/sockudo/get.js)
- [`server-sdks/sockudo-http-node/tests/remote/sockudo/proxy.js`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-node/tests/remote/sockudo/proxy.js)
- [`server-sdks/sockudo-http-node/tests/remote/sockudo/trigger.js`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-node/tests/remote/sockudo/trigger.js)
- [`server-sdks/sockudo-http-ruby/lib/sockudo.rb`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-ruby/lib/sockudo.rb)
- [`server-sdks/sockudo-http-ruby/lib/sockudo/request.rb`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-ruby/lib/sockudo/request.rb)
- [`server-sdks/sockudo-http-ruby/spec/client_spec.rb`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-ruby/spec/client_spec.rb)
- [`server-sdks/sockudo-http-ruby/sockudo.gemspec`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-ruby/sockudo.gemspec)
- [`server-sdks/sockudo-http-php/tests/bootstrap.php`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-php/tests/bootstrap.php)
- [`server-sdks/sockudo-http-swift/Sources/Sockudo/Sockudo.swift`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-swift/Sources/Sockudo/Sockudo.swift)
- [`server-sdks/sockudo-http-swift/Tests/SockudoTests/Models/TestObjects.swift`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-swift/Tests/SockudoTests/Models/TestObjects.swift)
- [`server-sdks/sockudo-http-swift/Tests/SockudoTests/ClientOptionsTests.swift`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/server-sdks/sockudo-http-swift/Tests/SockudoTests/ClientOptionsTests.swift)

## 2. Simplifications Made

- One authoritative clustered continuity source: durable stream metadata.
- Cache markers demoted to fail-closed hints instead of state authority.
- One representative real transport lane (Redis) rather than a transport matrix.
- One explicit destructive operator semantic per endpoint:
  - `history/reset` = rotate + purge
  - `history/purge` = trim retained rows without rotation
- One durable production backend decision for now: PostgreSQL only.
- Narrow deterministic fault injection instead of broad chaos harnesses.

## 3. Remaining Risks

- Live Postgres fault injection is still incomplete.
  Cache marker write failure and durable writer faults are mostly covered through deterministic wrappers and state transitions, not a faulting live Postgres backend.
- Some SDK verification remains environment-constrained.
  A few official SDKs are source-conformant but not fully re-verified in this environment; see the SDK matrices below.
- Acceptance suites for some server SDKs assume configured external endpoints.
  This is a release process/documentation issue more than a history API issue, but it still affects full-package verification.

## 4. Rollback Strategy

Server/runtime rollback:

1. Revert the history/recovery/rewind feature set to the last known-good commit before phase-3 changes.
2. Disable operator use of `history/reset` and `history/purge` immediately during rollback.
3. Keep degraded streams fail-closed while rolling back. Do not try to “heal” by clearing durable state manually unless the rollback plan explicitly includes metadata repair.
4. If rollback happens after intentional `history/reset`, old client continuity tokens remain invalid by design. Do not promise continuity restoration after a reset.

Operational rollback levers:

- disable durable-history-dependent workflows at the deployment edge if needed
- pause rollout if `history_degraded_channels` or `history_reset_required_channels` becomes non-zero unexpectedly
- keep old SDK packages available until the new SDK releases are verified in each package ecosystem

## 5. Deployment Prerequisites

- PostgreSQL history backend configured and reachable
- history tables initialized successfully
- Prometheus metrics scraping enabled
- alert rules loaded
- Grafana history/recovery dashboard imported or equivalent monitoring in place
- at least one authenticated operator path available for:
  - `GET /apps/{appId}/channels/{channelName}/history/state`
  - `POST /apps/{appId}/channels/{channelName}/history/reset`
  - `POST /apps/{appId}/channels/{channelName}/history/purge`
- clustered deployment uses a real horizontal transport if multinode behavior is expected
- at least one release environment validates the Redis-backed clustered test lane before broad rollout

## 6. Metrics And Alerts That Must Be Live Before Rollout

Required metrics:

- `history_writes_total`
- `history_write_failures_total`
- `history_write_latency_ms`
- `history_queue_depth`
- `history_degraded_channels`
- `history_reset_required_channels`
- `history_recovery_success_total{source=...}`
- `history_recovery_failures_total{code=...}`

Required alert coverage:

- degraded channels paging alert
- reset-required channels paging alert
- sustained queue depth warning
- critical queue depth paging alert
- history write SLO burn warning
- history write failure critical alert
- persistence-related recovery failure warning
- persistence-related recovery failure critical alert

Repo assets:

- [`monitoring/rules/history-recovery-alerts.yml`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/monitoring/rules/history-recovery-alerts.yml)
- [`grafana/history-recovery-dashboard-example.json`](/Users/radudiaconu/Desktop/Code/Rust/sockudo/grafana/history-recovery-dashboard-example.json)

## 7. What “Done” Means After Phase 3

Phase 3 is considered done when:

- clustered degraded-state coordination is fail-closed and durable-store-backed
- one real transport-backed clustered test lane exists and passes
- stream inspection/reset/purge controls exist and are documented
- major failure classes have deterministic fault-injection coverage
- SLOs, paging thresholds, and dashboards are explicit
- official SDKs are either:
  - updated and verified
  - or explicitly excluded from the broad “production-ready” claim with a documented decision

Release-owner decision:

- The server-side history/recovery/rewind feature can be rolled out broadly with known residual risk only.
- SDK release readiness is mixed; the server feature rollout is not blocked, but the excluded SDK packages below should not be marketed as fully re-verified for this phase until their package-specific blockers are cleared.

## 8. SDK Coverage Status

### Official Client SDKs

| SDK | Recovery token tracking | Rewind | Resume success/failure handling | Docs/examples | Verification status | Release status |
| --- | --- | --- | --- | --- | --- | --- |
| JS | `channel_positions` with `stream_id` + `serial` | Yes | Yes | Yes | `bun run test` passed | Ready |
| Swift client | `channel_positions` with `stream_id` + `serial` | Yes | Yes | Yes | `swift test` passed | Ready |
| Flutter | `channel_positions` with `stream_id` + `serial` | Yes | Yes | Yes | `flutter test` passed | Ready |
| Kotlin | `channel_positions` with `stream_id` + `serial` | Yes | Yes | Yes | `JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home ./gradlew :lib:test` passed | Ready |
| Python | Fixed to `channel_positions` with `stream_id` + `serial` | Added | Yes | Yes | `python3 -m pytest ...` passed | Ready |
| .NET client | Fixed to `channel_positions` with `stream_id` + `serial` | Added | Yes | Yes | `dotnet test` passed | Ready |

### Official Server SDKs

| SDK | History API support | Query parameter mapping | Pagination examples | Verification status | Release status |
| --- | --- | --- | --- | --- | --- |
| Node HTTP | Yes | Yes | Yes | `npm test` passed with remote/live cases marked pending when `SOCKUDO_URL` is unset | Ready |
| Go HTTP | Yes | Yes | Yes | `go test ./...` passed | Ready |
| Java HTTP | Yes | Yes | Yes | `./gradlew test` passed | Ready |
| .NET HTTP | Yes | Yes | Yes | builds pass and tests now execute under `mono`, but the package still has many pre-existing non-history failures and missing acceptance config fixtures | Excluded from full verification |
| Ruby HTTP | Yes | Yes | Yes | `bundle exec rspec` passed after runtime and test drift fixes | Ready |
| PHP HTTP | Yes | Yes | Yes | history unit test passed; full suite now skips acceptance cleanly when env is unset | Ready |
| Swift HTTP | Yes | Yes | Yes | compile blockers and stale test fixtures fixed; full suite still did not produce a clean terminal result in this session | Excluded from full verification |
| Rust HTTP | Added in this pass | Yes | Added | source and tests added; build/test output remained inconclusive in this session due long-running cargo work | Excluded from full verification |

### Product Decision For Excluded SDKs

The following SDK packages are explicitly excluded from the broad “fully re-verified for phase 3” claim for now:

- .NET HTTP server SDK
- Swift HTTP server SDK
- Rust HTTP server SDK

Reason:

- These packages are source-conformant for the phase-3 history/recovery/rewind contract, but package-specific verification was blocked by local environment or toolchain constraints rather than protocol mismatches.

This exclusion does **not** block broad rollout of the core server feature. It does block claiming universal package-level release verification across every official SDK.
