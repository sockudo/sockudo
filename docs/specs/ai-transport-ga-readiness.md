# AI Transport GA Readiness

Status: release-candidate gate wired; GA sign-off blocked on external RC evidence.
Date: 2026-06-04
Scope: S16 release engineering for Sockudo AI Transport server platform primitives.

## Gate Commands

Run the deterministic repository gate:

```bash
scripts/ai-transport-ga-gate.sh ci-evidence
```

Run the local server feature matrix:

```bash
scripts/ai-transport-ga-gate.sh matrix
```

Run the final GA evidence gate on a release-candidate branch after external results are committed:

```bash
scripts/ai-transport-ga-gate.sh release-evidence
```

`release-evidence` fails while committed result manifests remain `*.not-run.json` or while the
rolling-upgrade and SDK full-matrix manifests are absent, planned, or failed.

Generate the four S14 fleet manifests from a release-candidate fleet:

```bash
AIT_RELEASE_EVIDENCE_CONFIRM=external-rc \
SOCKUDO_NODE_URLS="http://node-1:6001,http://node-2:6001,..." \
SOCKUDO_METRICS_URLS="http://node-1:9601/metrics,http://node-2:9601/metrics,..." \
scripts/ai-transport-s14-release-evidence.sh
```

Generate shared-Redis rolling-upgrade evidence after wiring real phase hooks:

```bash
AIT_RELEASE_EVIDENCE_CONFIRM=external-rc \
node scripts/ai-transport-rolling-upgrade-redis.mjs --execute \
  --preHook "./ops/rc/bring-up-old-nodes.sh" \
  --mixedHook "./ops/rc/roll-one-node-to-rc.sh" \
  --postHook "./ops/rc/roll-all-nodes-to-rc-and-enable-ai.sh"
```

Generate the full SDK matrix:

```bash
node scripts/sdk-compat-full-matrix.mjs --execute
```

## S16 Checklist

| Gate | Status | Evidence |
| --- | --- | --- |
| Full server feature matrix | Wired | `scripts/ai-transport-ga-gate.sh matrix`; CI job `AI Transport Release Feature Matrix`. |
| Default profile has no AI Transport feature trace | Wired | Matrix script runs `cargo tree -p sockudo -e features` and fails on `ai-transport`. |
| `--no-default-features` pure Pusher build | Wired | Matrix combo `no-default-pusher`. |
| Rolling upgrade on shared Redis | Blocked for GA | Required manifest: `docs/specs/ai-transport-results/rolling-upgrade-redis.json`. |
| Wire-protocol v1 freeze | Wired | `docs/specs/ai-transport-wire-protocol.md` is labeled `Wire protocol version: 1` and includes the compatibility promise. |
| S13 golden transcripts | Wired | `test/ai-conformance/fixtures/golden/*` plus `AIT_CONFORMANCE_OFFLINE=1 scripts/ai-conformance-node.sh`. |
| S12 benchmark sign-off | Wired | `scripts/ai-transport-bench-guard.sh`, `benches/ai/baselines/ai_hot_paths.budgets.json`, and `docs/specs/ai-transport-capacity.md`. |
| S14 scaled smoke/headline evidence | Blocked for GA | Current manifests under `docs/specs/ai-transport-results/*.not-run.json` are explicit placeholders. |
| S11 security sign-off | Wired | `docs/specs/ai-transport-security-review.md`; CI runs `cargo audit` and `cargo deny check`. |
| Fuzz smoke | Wired | `.github/workflows/fuzz-smoke.yml`; S11 lists committed parser smoke scope. |
| Full cross-SDK harness | Wired; blocked until executed | `scripts/sdk-compat-full-matrix.mjs --execute` writes `docs/specs/ai-transport-results/sdk-compat-full-matrix.json`. |
| Release order | Policy recorded | Server ships with AI defaults off before SDK releases depending on AI Transport. See `plans/ai-transport/03-existing-sdks-prompts.md` E6. |
| Release config example | Wired | `config/ai-transport.example.toml`. |
| Docker with/without AI Transport | Wired | CI job `AI Transport Docker Feature Builds`. |

## Rolling Upgrade Notes

AI Transport must stay runtime-disabled during mixed-version Redis clusters unless the release
candidate has a recorded shared-Redis rolling-upgrade run. The minimum accepted run is:

1. Start the previous release and the release candidate against the same Redis, history store, and
   version store.
2. Keep Protocol V1 and non-AI Protocol V2 traffic flowing while replacing nodes one by one.
3. Verify V1 wire output remains unchanged, V2 non-AI recovery/history remains continuous, and AI
   publishes are rejected or degraded with documented feature-disabled errors until every node is on
   the release candidate.
4. Enable `[ai_transport]` only after all nodes are upgraded, then run an AI stream/recovery smoke.
5. Commit the runner output as `docs/specs/ai-transport-results/rolling-upgrade-redis.json`.

Use `scripts/ai-transport-rolling-upgrade-redis.mjs` to record the old, mixed, and all-new phases.
The script requires `AIT_RELEASE_EVIDENCE_CONFIRM=external-rc` when executing so a local smoke run
cannot accidentally replace release evidence.

Operators already using versioned messages, durable history, or push do not need a data migration
for AI Transport itself. They must verify retention windows, shared store backends, idempotency TTL,
and push credential encryption before enabling AI channel prefixes.

## Performance Sign-Off

The GA release notes must include:

- Criterion hardware note from `target/criterion/ai-transport-hardware.txt`.
- Budget summary from `scripts/ai-transport-bench-guard.sh`.
- Headline S14 manifests for 1M connections, reconnect storm, cancel storm, and 24-hour soak.
- Any accepted chaos limitation from `docs/specs/ai-transport-capacity.md`.

Synthetic numbers are not acceptable release evidence.

## Security Sign-Off

The GA release notes must include the S11 summary and current outputs for:

- `cargo audit`
- `cargo deny check`
- `.github/workflows/fuzz-smoke.yml`
- AI release graph checks that exclude known out-of-graph advisory paths

No residual AI Transport release blocker is accepted in `docs/specs/ai-transport-security-review.md`.

## SDK plans status

Full product parity still depends on SDK work outside this server release gate:

| Plan | Status for server GA |
| --- | --- |
| `plans/ai-transport/02-sdk-prompts.md` | P0-P16 must be complete before shipping SDKs that depend on AI Transport. Server can release first with defaults off. |
| `plans/ai-transport/03-existing-sdks-prompts.md` E1 | Full matrix evidence must be committed as `sdk-compat-full-matrix.json` before GA. |
| `plans/ai-transport/03-existing-sdks-prompts.md` E4/E5 | Enablement remains a dependency for full product parity, not for a server release with AI defaults off. |
| `plans/ai-transport/03-existing-sdks-prompts.md` E6 | Release-order policy is recorded; dry-run rehearsal evidence is required before SDK publication. |
