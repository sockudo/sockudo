#!/usr/bin/env bash
# Tier 1A verification (aggregate_counts ON). Proves two things at once:
#   correctness: info=subscription_count returns the correct CROSS-NODE total
#   performance: those count reads do ~ZERO ChannelSocketsCount cross-node fan-out
# Requires the sockudo:churn-tier1a image and config.json with aggregate_counts=true.
set -uo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -f docker-compose.yml"
PORTS="9611 9612 9613"
VUS="${VUS:-600}"
PUB_INTERVAL_MS="${PUB_INTERVAL_MS:-20}"
PROBE_SECS="${PROBE_SECS:-20}"
CHANNEL="${CHANNEL:-hot-room}"

sent_light () {
  for p in $PORTS; do curl -s -m3 "http://localhost:$p/metrics"; done \
    | grep 'horizontal_adapter_sent_requests' | grep 'app_id="light"' \
    | awk '{s+=$NF} END{printf "%.0f\n", s+0}'
}

echo ">>> up Tier 1A image (aggregate_counts on)"
SOCKUDO_IMAGE=sockudo:churn-tier1a $COMPOSE up -d >/dev/null 2>&1
echo ">>> warming"; sleep 10

echo ">>> subscribing $VUS connections to $CHANNEL across 3 nodes"
k6 run --quiet -e APP_KEY=lightkey -e CHANNEL="$CHANNEL" -e VUS="$VUS" \
      -e DURATION="$((PROBE_SECS + 28))s" bulk_subs.js > /tmp/t1a_bulk.txt 2>&1 &
BULK=$!
echo ">>> waiting for subscribe + gossip convergence (full snapshot ~5s)"; sleep 20

before=$(sent_light)
echo ">>> ${PROBE_SECS}s of info=subscription_count reads (each would fan out without Tier 1A)"
k6 run --quiet -e PUB_HOST=localhost:6011 -e PROBE_WS=ws://localhost:6012 \
      -e CHANNEL="$CHANNEL" -e INFO=subscription_count -e PUB_INTERVAL_MS="$PUB_INTERVAL_MS" \
      -e DURATION="${PROBE_SECS}s" probe.js > /tmp/t1a_probe.txt 2>&1
after=$(sent_light)
wait $BULK 2>/dev/null

echo "==================== TIER 1A RESULT ===================="
echo "bulk subscribed (ws_sessions): $(grep ws_sessions /tmp/t1a_bulk.txt | grep -oE '[0-9]+' | head -1)"
echo "CORRECTNESS — reported subscription_count (should ~= $VUS across nodes):"
grep -E "reported_count" /tmp/t1a_probe.txt | sed 's/^/  /'
echo "PERF — ChannelSocketsCount fan-out during count reads: +$((after - before)) (Tier 1A target: ~0)"
echo "       info reads performed (pub_ok): $(grep pub_ok /tmp/t1a_probe.txt | grep -oE '[0-9]+' | head -1)"
SOCKUDO_IMAGE=sockudo:churn-tier1a $COMPOSE down -v >/dev/null 2>&1
echo "Interpretation: reported_count ~= $VUS AND fan-out ~0 => aggregate counts are correct cross-node and read locally."
