#!/usr/bin/env bash
# Definitive A/B: same churn workload + same app config (app "light", no count
# consumer) against the pre-gate binary vs the gated binary. Isolates exactly the
# Tier 1B change.
set -uo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -f docker-compose.yml"
PORTS="9611 9612 9613"
WS_PORTS="6011 6012 6013"
DURATION="${DURATION:-40s}"; SECS="${DURATION%s}"
VUS="${VUS:-120}"; CHURN_MS="${CHURN_MS:-400}"; ROOMS="${ROOMS:-300}"

metric_sum () { # substring app_id
  for p in $PORTS; do curl -s -m3 "http://localhost:$p/metrics"; done \
    | grep "$1" | grep "app_id=\"$2\"" | awk '{s+=$NF} END{printf "%.3f\n", s+0}'
}
timeouts () { $COMPOSE logs --no-color 2>/dev/null | grep -c "timed out after"; }

run_gen () { # label image
  local label="$1" image="$2"
  echo "############### GEN: $label  (image=$image) ###############"
  SOCKUDO_IMAGE="$image" $COMPOSE down -v >/dev/null 2>&1
  SOCKUDO_IMAGE="$image" $COMPOSE up -d >/dev/null 2>&1
  echo "  warming cluster..."; sleep 10

  local sent0 rsum0 rcnt0 to0
  sent0=$(metric_sum horizontal_adapter_sent_requests light)
  rsum0=$(metric_sum horizontal_adapter_resolve_time_sum light)
  rcnt0=$(metric_sum horizontal_adapter_resolve_time_count light)
  to0=$(timeouts)

  ( upf=0; livef=0; n=0; end=$((SECONDS+SECS))
    while [ $SECONDS -lt $end ]; do
      for p in $WS_PORTS; do
        n=$((n+1))
        [ "$(curl -s -m2 -o /dev/null -w '%{http_code}' http://localhost:$p/up/light)" = "200" ] || upf=$((upf+1))
        [ "$(curl -s -m2 -o /dev/null -w '%{http_code}' http://localhost:$p/live)" = "200" ] || livef=$((livef+1))
      done; sleep 2
    done
    echo "  health during load: $n probes | /up failures=$upf | /live failures=$livef" ) &
  local hp=$!

  k6 run --quiet -e APP_KEY=lightkey -e VUS="$VUS" -e CHURN_MS="$CHURN_MS" \
        -e ROOMS="$ROOMS" -e DURATION="$DURATION" churn.js > "/tmp/ab_$label.txt" 2>&1
  wait $hp

  local sent1 rsum1 rcnt1 to1
  sent1=$(metric_sum horizontal_adapter_sent_requests light)
  rsum1=$(metric_sum horizontal_adapter_resolve_time_sum light)
  rcnt1=$(metric_sum horizontal_adapter_resolve_time_count light)
  to1=$(timeouts)

  local dsent rate dcnt davg subacks
  dsent=$(awk "BEGIN{printf \"%.0f\", $sent1-$sent0}")
  rate=$(awk "BEGIN{printf \"%.1f\", ($sent1-$sent0)/$SECS}")
  dcnt=$(awk "BEGIN{printf \"%.0f\", $rcnt1-$rcnt0}")
  davg=$(awk "BEGIN{d=$rcnt1-$rcnt0; if(d>0) printf \"%.0f\", 1000*($rsum1-$rsum0)/d; else print 0}")
  subacks=$(grep sub_acks "/tmp/ab_$label.txt" | grep -oE '[0-9]+' | head -1)

  echo "  RESULT[$label]: cluster count fanout (sent_requests, app=light) +$dsent  =>  ${rate} req/s"
  echo "  RESULT[$label]: fanout resolve latency ~${davg} ms avg over $dcnt resolved"
  echo "  RESULT[$label]: request timeouts during load: +$((to1-to0))"
  echo "  RESULT[$label]: offered churn (k6 sub_acks): ${subacks}"
  echo
}

run_gen OLD_pregate sockudo:churn-old
run_gen NEW_gated   sockudo:churn-test
echo "================================================================"
echo "Same workload, app 'light' (no count consumer)."
echo "OLD_pregate: count fanout fires per sub/unsub.  NEW_gated: gate skips it."
SOCKUDO_IMAGE=sockudo:churn-test $COMPOSE down -v >/dev/null 2>&1
