#!/usr/bin/env bash
# A/B verification for the Tier 1B count-fanout gate.
#   baseline (app "heavy", count webhooks ON)  -> fanout fires per sub/unsub
#   fixed    (app "light", no count consumer)  -> gate skips the fanout
# Same churn workload for both. We measure the cluster-wide request/reply rate
# (sockudo_horizontal_adapter_sent_requests) and pod health during load.
set -uo pipefail
cd "$(dirname "$0")"

export PORTS="9611 9612 9613"
WS_PORTS="6011 6012 6013"
DURATION="${DURATION:-60s}"
SECS="${DURATION%s}"
VUS="${VUS:-200}"
CHURN_MS="${CHURN_MS:-1000}"
ROOMS="${ROOMS:-400}"

timeouts_total () { docker compose logs --no-color 2>/dev/null | grep -c "timed out after"; }

poll_health () { # app_id seconds tag
  local app_id="$1" secs="$2" tag="$3"
  local up_ok=0 up_fail=0 live_ok=0 live_fail=0 end=$((SECONDS + secs))
  while [ $SECONDS -lt $end ]; do
    for p in $WS_PORTS; do
      lc=$(curl -s -m 2 -o /dev/null -w "%{http_code}" "http://localhost:$p/live")
      uc=$(curl -s -m 2 -o /dev/null -w "%{http_code}" "http://localhost:$p/up/$app_id")
      [ "$lc" = "200" ] && live_ok=$((live_ok+1)) || live_fail=$((live_fail+1))
      [ "$uc" = "200" ] && up_ok=$((up_ok+1)) || up_fail=$((up_fail+1))
    done
    sleep 2
  done
  echo "  [$tag] health during load: /live ok=$live_ok fail=$live_fail | /up ok=$up_ok fail=$up_fail"
}

run_case () { # label app_key app_id
  local label="$1" key="$2" app_id="$3"
  echo "================ CASE: $label  (app=$app_id, key=$key) ================"
  local to_before=$(timeouts_total)
  local sent_before=$(./measure.sh horizontal_adapter_sent_requests)

  k6 run --quiet -e APP_KEY="$key" -e VUS="$VUS" -e CHURN_MS="$CHURN_MS" \
        -e ROOMS="$ROOMS" -e DURATION="$DURATION" churn.js > "/tmp/k6_${label}.txt" 2>&1 &
  local k6pid=$!
  poll_health "$app_id" "$SECS" "$label"
  wait $k6pid

  local sent_after=$(./measure.sh horizontal_adapter_sent_requests)
  local to_after=$(timeouts_total)
  local sent_delta=$((sent_after - sent_before))
  local to_delta=$((to_after - to_before))
  local rate=$(awk "BEGIN{printf \"%.1f\", $sent_delta/$SECS}")
  local offered=$(grep -E "subs_sent|unsubs_sent" "/tmp/k6_${label}.txt" | grep -oE '[0-9]+([.][0-9]+)?(/s)?' | head -4 | tr '\n' ' ')
  echo "  RESULT[$label]: horizontal_adapter_sent_requests +$sent_delta over ${SECS}s => ${rate} req/s"
  echo "  RESULT[$label]: request timeouts during case: +$to_delta"
  echo "  offered churn (k6 subs/unsubs counters): see /tmp/k6_${label}.txt"
  echo
}

echo ">>> warming cluster (cluster_health heartbeats need a few seconds)..."
sleep 8
echo ">>> node liveness/readiness pre-load:"
for p in $WS_PORTS; do
  echo "  node :$p  /live=$(curl -s -m2 -o /dev/null -w '%{http_code}' http://localhost:$p/live)  /up/heavy=$(curl -s -m2 -o /dev/null -w '%{http_code}' http://localhost:$p/up/heavy)"
done
echo

run_case baseline_heavy heavykey heavy
echo ">>> cooldown..."; sleep 6
run_case fixed_light lightkey light

echo "================ SUMMARY ================"
echo "baseline = count webhooks ON  (fanout fires)  -> expect high req/s"
echo "fixed    = no count consumer  (gate skips)    -> expect ~0 req/s"
