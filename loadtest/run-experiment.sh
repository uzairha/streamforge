#!/usr/bin/env bash
#
# Runs one tuning experiment against a already-running infra stack (make up).
#
# Starts the three pipeline services with a given configuration, samples
# consumer-group lag for the duration, then shuts them down and records the
# throughput each stage achieved.
#
#   ./loadtest/run-experiment.sh baseline 2000 5ms none
#   ./loadtest/run-experiment.sh no-linger 2000 0s none
#
# Arguments: LABEL RATE LINGER COMPRESSION
#
# Every service writes to its own log so a failed run can be diagnosed after
# the fact rather than by re-running it.

set -euo pipefail

LABEL="${1:?usage: run-experiment.sh LABEL RATE LINGER COMPRESSION}"
RATE="${2:-2000}"
LINGER="${3:-5ms}"
COMPRESSION="${4:-none}"
DURATION="${DURATION:-60s}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS="$ROOT/loadtest/results"
LOGS="$RESULTS/logs"
mkdir -p "$RESULTS" "$LOGS"

export KAFKA_BROKERS=localhost:19092
export SYNTHETIC_RATE="$RATE"
export PRODUCER_LINGER="$LINGER"
export PRODUCER_COMPRESSION="$COMPRESSION"
# Short windows so the aggregator actually closes some during a 60s run;
# the default 1m would produce almost nothing to measure.
export WINDOW_SIZE=10s
export ALLOWED_LATENESS=5s
# Tracing off: a full trace per event would measure the tracer rather than
# the pipeline, and M6 is about pipeline throughput.
export TRACING_ENABLED=false

echo "=== $LABEL: rate=$RATE linger=$LINGER compression=$COMPRESSION duration=$DURATION ==="

# Each stage gets its own metrics port because they share the host's network.
METRICS_ADDR=:2112 "$ROOT/bin/ingester"   > "$LOGS/$LABEL-ingester.log"   2>&1 &
ING=$!
METRICS_ADDR=:2113 "$ROOT/bin/normalizer" > "$LOGS/$LABEL-normalizer.log" 2>&1 &
NORM=$!
METRICS_ADDR=:2114 "$ROOT/bin/aggregator" > "$LOGS/$LABEL-aggregator.log" 2>&1 &
AGG=$!

cleanup() {
  kill -TERM $ING $NORM $AGG 2>/dev/null || true
  wait $ING $NORM $AGG 2>/dev/null || true
}
trap cleanup EXIT

# Let the consumer groups join and rebalance before sampling; lag measured
# during a rebalance reflects the rebalance, not the pipeline.
sleep 8

"$ROOT/bin/lagprobe" \
  --brokers localhost:19092 \
  --duration "$DURATION" \
  --interval 2s \
  --label "$LABEL" \
  --out "$RESULTS/lag-$LABEL.csv"

# Scrape the final counters before the services exit.
{
  echo "label,metric,value"
  for port_name in "2112:ingester" "2113:normalizer" "2114:aggregator"; do
    port="${port_name%%:*}"
    name="${port_name##*:}"
    curl -s "http://localhost:$port/metrics" 2>/dev/null \
      | awk -v l="$LABEL" -v n="$name" '
          /^streamforge_events_produced_total/            { s+=$2; m="produced" }
          /^streamforge_normalizer_events_produced_total/ { s+=$2; m="produced" }
          /^streamforge_aggregator_windows_closed_total/  { w+=$2 }
          END {
            if (s > 0) printf "%s,%s_produced,%d\n", l, n, s
            if (w > 0) printf "%s,%s_windows,%d\n", l, n, w
          }'
  done
} > "$RESULTS/counters-$LABEL.csv"

cleanup
trap - EXIT

echo "--- $LABEL results ---"
tail -n +2 "$RESULTS/counters-$LABEL.csv"
echo "peak lag: $(tail -n +2 "$RESULTS/lag-$LABEL.csv" | cut -d, -f4 | sort -n | tail -1)"
