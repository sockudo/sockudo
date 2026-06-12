#!/usr/bin/env bash
# Probe-vs-bulk experiment: is the high broadcast latency a load-gen-client
# artifact, or a real server bottleneck? And does /up degrade under publish load?
#
#   bulk_subs.js : VUS subscribers on a hot channel (the "one machine" load)
#   probe.js     : separate process, publishes timestamped events + 1 clean probe
#   poller       : samples /up status + response time on every node during load
set -uo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -f docker-compose.yml"

VUS="${VUS:-2500}"
PUB_INTERVAL_MS="${PUB_INTERVAL_MS:-100}"
PROBE_SECS="${PROBE_SECS:-40}"
CHANNEL="${CHANNEL:-hot-room}"
HTTP_PORTS="6011 6012 6013"

echo ">>> ensuring gated cluster is up"
SOCKUDO_IMAGE=sockudo:churn-test $COMPOSE up -d >/dev/null 2>&1
echo ">>> warming"; sleep 10

# /up responsiveness poller
( end=$((SECONDS + PROBE_SECS + 18)); : > /tmp/uphealth.csv
  while [ $SECONDS -lt $end ]; do
    for p in $HTTP_PORTS; do
      echo "$p,$(curl -s -m 5 -o /dev/null -w '%{http_code},%{time_total}' http://localhost:$p/up/light)" >> /tmp/uphealth.csv
    done
    sleep 1
  done ) &
POLLER=$!

# pod CPU snapshot mid-load
( sleep $((12 + PROBE_SECS/2)); docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemPerc}}' $(docker ps --format '{{.Names}}' | grep churn-sockudo) > /tmp/podcpu.txt 2>&1 ) &

# bulk subscribers (cover the probe window)
BULK_DUR=$((PROBE_SECS + 25))
k6 run --quiet -e APP_KEY=lightkey -e CHANNEL="$CHANNEL" -e VUS="$VUS" -e DURATION="${BULK_DUR}s" bulk_subs.js > /tmp/bulk.txt 2>&1 &
BULK=$!

echo ">>> letting $VUS bulk subscribers connect..."; sleep 12

echo ">>> publishing + probing for ${PROBE_SECS}s under load"
k6 run --quiet -e PUB_HOST=localhost:6011 -e PROBE_WS=ws://localhost:6012 \
   -e CHANNEL="$CHANNEL" -e PUB_INTERVAL_MS="$PUB_INTERVAL_MS" -e INFO="${INFO:-}" \
   -e DURATION="${PROBE_SECS}s" probe.js > /tmp/probe.txt 2>&1

wait $BULK 2>/dev/null
kill $POLLER 2>/dev/null; wait $POLLER 2>/dev/null

echo
echo "================== RESULTS =================="
echo "--- PROBE (clean: 1 conn, separate process = true server latency) ---"
grep -E "probe_latency|probe_recv|pub_ok|pub_fail" /tmp/probe.txt | sed 's/^/  /'
echo "--- BULK ($VUS conns on this one host = saturated client view) ---"
grep -E "bulk_latency|bulk_recv|ws_sessions|^\s*vus" /tmp/bulk.txt | sed 's/^/  /'
echo "--- /up readiness under load ---"
awk -F, '{n++; if($3>maxt)maxt=$3; if($2!=200)bad++} END{printf "  probes=%d  non-200=%d  max_/up_response=%.0fms\n", n, bad+0, maxt*1000}' /tmp/uphealth.csv
echo "--- pod CPU mid-load ---"; sed 's/^/  /' /tmp/podcpu.txt
echo "============================================="
echo "READ: probe low + /up fast => client artifact (latency was the load box)."
echo "      probe high OR /up slow/non-200 => real server bottleneck."
