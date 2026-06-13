# AI Transport Capacity Result Manifests

Each committed result manifest uses schema `sockudo.ai-transport.scale-result.v1`.
Production validation runs should write raw runner output here or attach it as a PR artifact and
commit a reviewed summary manifest.

The manifests currently checked in with status `not_executed_local` are execution contracts, not
claimed production results. Replace them with measured `passed` or `failed` manifests after the
headline fleet is run.

Release-candidate commands:

```bash
AIT_RELEASE_EVIDENCE_CONFIRM=external-rc \
SOCKUDO_NODE_URLS="http://node-1:6001,http://node-2:6001,..." \
SOCKUDO_METRICS_URLS="http://node-1:9601/metrics,http://node-2:9601/metrics,..." \
scripts/ai-transport-s14-release-evidence.sh

AIT_RELEASE_EVIDENCE_CONFIRM=external-rc \
node scripts/ai-transport-rolling-upgrade-redis.mjs --execute \
  --preHook "./ops/rc/bring-up-old-nodes.sh" \
  --mixedHook "./ops/rc/roll-one-node-to-rc.sh" \
  --postHook "./ops/rc/roll-all-nodes-to-rc-and-enable-ai.sh"

node scripts/sdk-compat-full-matrix.mjs --execute
scripts/ai-transport-ga-gate.sh release-evidence
```
