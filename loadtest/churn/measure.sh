#!/usr/bin/env bash
# Sum a Prometheus counter across all sockudo nodes' /metrics endpoints.
# Usage: measure.sh <metric_substring>
metric="${1:-horizontal_adapter_sent_requests}"
PORTS="${PORTS:-9611 9612 9613}"
for p in $PORTS; do
  curl -s -m 3 "http://localhost:$p/metrics"
done | grep "$metric" | grep -v '^#' | awk '{s+=$NF} END {printf "%.0f\n", s+0}'
