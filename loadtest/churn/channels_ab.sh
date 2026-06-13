#!/usr/bin/env bash
# A/B the GET /channels cross-node fan-out: BEFORE (no cache) vs AFTER (short-TTL
# cache). Polls /channels at a fixed rate and measures (a) the cross-node
# ChannelsWithSocketsCount request rate and (b) /channels response latency.
set -uo pipefail
cd "$(dirname "$0")"
COMPOSE="docker compose -f docker-compose.yml"
PORTS="9611 9612 9613"
POLL_MS="${POLL_MS:-100}"
DURATION="${DURATION:-20s}"
SECS="${DURATION%s}"
INFO="${INFO:-subscription_count}"

# sum sent_requests for app "light" only (excludes cluster heartbeats)
sent_light () {
  for p in $PORTS; do curl -s -m3 "http://localhost:$p/metrics"; done \
    | grep 'horizontal_adapter_sent_requests' | grep 'app_id="light"' \
    | awk '{s+=$NF} END{printf "%.0f\n", s+0}'
}

run_case () { # label image
  local label="$1" image="$2"
  echo "################ $label ($image) ################"
  SOCKUDO_IMAGE="$image" $COMPOSE down -v >/dev/null 2>&1
  SOCKUDO_IMAGE="$image" $COMPOSE up -d >/dev/null 2>&1
  echo "  warming..."; sleep 10
  local before after polls
  before=$(sent_light)
  k6 run --quiet -e HOST=localhost:6011 -e INFO="$INFO" -e POLL_MS="$POLL_MS" \
        -e DURATION="$DURATION" channels_poll.js > "/tmp/chpoll_$label.txt" 2>&1
  after=$(sent_light)
  polls=$(grep -E "channels_ok" "/tmp/chpoll_$label.txt" | grep -oE '[0-9]+' | head -1)
  echo "  /channels polls=${polls}  cross-node fanouts(sent_requests, light)=+$((after-before)) over ${SECS}s"
  grep -E "channels_latency|channels_fail" "/tmp/chpoll_$label.txt" | sed 's/^/    /'
  echo
}

run_case BEFORE_no_cache sockudo:churn-test
sleep 4
run_case AFTER_cache     sockudo:churn-cache
SOCKUDO_IMAGE=sockudo:churn-test $COMPOSE down -v >/dev/null 2>&1
echo "Expect: BEFORE fanouts ~= poll count (one cross-node aggregation per request);"
echo "        AFTER fanouts ~= duration/TTL (cache collapses repeated polls), lower latency."
