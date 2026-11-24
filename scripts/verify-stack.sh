#!/usr/bin/env bash
# Stack checks for pdm-pipeline (Mosquitto + Kafka + bridge + Kafka UI):
#   1. compose config valid; every long-running service healthy; kafka-init exited 0;
#      scripts/verify-broker.sh passes
#   2. both Kafka topics exist with the right partitions / retention / compression
#   3. Kafka UI reachable at http://localhost:8080 and lists both topics
#   4. (only with --crash) bridge crash test, N rounds (default 3): replay units 81-100 at
#      --speed 20, SIGKILL the bridge after 10 s (no graceful flush), leave it down 15 s,
#      start it again, then scripts/kafka_check.py on a fresh snapshot must show all 4493
#      unique (engine_id, cycle) rows, 0 missing, 0 DLQ. Duplicates are expected and only
#      reported (with raw_regressions); any loss fails the round.
# Usage: scripts/verify-stack.sh [--crash[=N]]   (from anywhere; cds to the repo root)
# Other data-path checks (replay -> Kafka) are separate: see README "Kafka and the bridge".
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

CRASH_ROUNDS=0
for arg in "$@"; do
  case "$arg" in
    --crash) CRASH_ROUNDS=3 ;;
    --crash=*) CRASH_ROUNDS=${arg#--crash=}
               [[ "$CRASH_ROUNDS" =~ ^[1-9][0-9]*$ ]] || { echo "ABORT: --crash=N needs N >= 1"; exit 2; } ;;
    -h|--help) sed -n '2,/^set -uo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
    *) echo "ABORT: unknown argument '$arg' (usage: $0 [--crash[=N]])"; exit 2 ;;
  esac
done

pass=0; fail=0
ok()   { echo "  PASS: $*"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $*"; fail=$((fail+1)); }
hdr()  { echo; echo "================ $* ================"; }

command -v docker >/dev/null 2>&1 || { echo "ABORT: docker is not installed."; exit 127; }

SERVICES=(pdm-mosquitto pdm-kafka pdm-kafka-ui pdm-mqtt-kafka-bridge)

wait_healthy() {  # name timeout_s
  local name=$1 deadline=$((SECONDS+$2)) s=unknown
  while [ $SECONDS -lt $deadline ]; do
    s=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "$name" 2>/dev/null || echo missing)
    [ "$s" = healthy ] && break
    sleep 3
  done
  echo "$s"
}

hdr "1a. docker compose config"
if docker compose config -q; then ok "compose config validated"; else bad "compose config rejected"; fi

hdr "1b. up -d, every service healthy, kafka-init exited 0"
docker compose up -d 2>&1 | tail -n 6
for c in "${SERVICES[@]}"; do
  s=$(wait_healthy "$c" 180)
  [ "$s" = healthy ] && ok "$c healthy" || bad "$c not healthy (last=$s)"
done
init=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' pdm-kafka-init 2>/dev/null || echo "missing -")
[ "$init" = "exited 0" ] && ok "kafka-init exited 0" || bad "kafka-init state: $init"
docker compose ps -a

hdr "1c. scripts/verify-broker.sh"
vb_out=$(mktemp)
if scripts/verify-broker.sh >"$vb_out" 2>&1; then
  ok "verify-broker.sh: $(grep -E '^passed=' "$vb_out")"
else
  tail -n 30 "$vb_out"
  bad "verify-broker.sh failed (output above)"
fi
rm -f "$vb_out"
# verify-broker.sh restarts mosquitto; make sure the bridge is back too.
s=$(wait_healthy pdm-mqtt-kafka-bridge 60)
[ "$s" = healthy ] && ok "bridge healthy after broker restart" || bad "bridge unhealthy after broker restart ($s)"

hdr "2. Kafka topics"
# The CLI is a JVM; give it a small heap so it fits beside the broker in the 1g limit.
describe=$(docker exec -e KAFKA_HEAP_OPTS='-Xms16m -Xmx96m' pdm-kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic 'sensor\.telemetry\..*' 2>&1)
echo "$describe"
check_topic() {  # name partitions retention_ms [compression]
  local line; line=$(grep -E "^Topic: $1\s" <<<"$describe" | head -1)
  [ -n "$line" ] || { bad "$1 missing"; return; }
  grep -q "PartitionCount: $2\b" <<<"$line" && ok "$1 has $2 partitions" || bad "$1 partitions: $line"
  grep -q "retention.ms=$3\b" <<<"$line" && ok "$1 retention.ms=$3" || bad "$1 retention: $line"
  if [ -n "${4:-}" ]; then
    grep -q "compression.type=$4\b" <<<"$line" && ok "$1 compression.type=$4" || bad "$1 compression: $line"
  fi
}
check_topic sensor.telemetry.raw 6 604800000 lz4
check_topic sensor.telemetry.dlq 1 2592000000
auto=$(docker exec pdm-kafka grep -E '^auto.create.topics.enable=' /opt/kafka/config/server.properties)
[ "$auto" = "auto.create.topics.enable=false" ] && ok "$auto" || bad "auto.create.topics.enable: '$auto'"

hdr "3. Kafka UI (http://localhost:8080)"
clusters=$(curl -sf --max-time 10 http://localhost:8080/api/clusters)
if [ -n "$clusters" ] && grep -q '"status":"ONLINE"' <<<"$clusters"; then ok "UI up, cluster ONLINE"; else bad "UI cluster not ONLINE: ${clusters:-no response}"; fi
topics=$(curl -sf --max-time 10 'http://localhost:8080/api/clusters/pdm/topics?perPage=100' \
  | python3 -c 'import sys,json; print("\n".join(t["name"] for t in json.load(sys.stdin)["topics"]))' 2>/dev/null)
echo "$topics" | sed 's/^/  ui topic: /'
for t in sensor.telemetry.raw sensor.telemetry.dlq; do
  grep -qx "$t" <<<"$topics" && ok "UI lists $t" || bad "UI does not list $t"
done

if [ "$CRASH_ROUNDS" -gt 0 ]; then
  hdr "4. bridge crash test ($CRASH_ROUNDS rounds: SIGKILL mid-replay)"
  PY=.venv/bin/python
  BRIDGE=pdm-mqtt-kafka-bridge
  UNITS=81-100; ROWS=4493
  work=$(mktemp -d)
  summary=()
  for round in $(seq 1 "$CRASH_ROUNDS"); do
    echo "--- round $round/$CRASH_ROUNDS"
    s=$(wait_healthy $BRIDGE 60)
    [ "$s" = healthy ] || { bad "round $round: bridge not healthy before start ($s)"; continue; }
    snap=$work/snap$round.json; out=$work/check$round.json
    if ! $PY scripts/kafka_check.py --save-snapshot "$snap"; then bad "round $round: snapshot failed"; continue; fi
    $PY replayer/replay.py --units $UNITS --speed 20 >"$work/replay$round.log" 2>&1 &
    rpid=$!
    sleep 10
    docker kill --signal SIGKILL $BRIDGE >/dev/null && echo "  killed $BRIDGE (SIGKILL) at t=10s"
    sleep 2
    st=$(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}}' $BRIDGE)
    echo "  bridge state after kill: $st"
    [[ "$st" == exited* ]] && ok "round $round: bridge down after SIGKILL ($st)" \
                           || bad "round $round: bridge not down after SIGKILL ($st); outage did not happen"
    sleep 13
    docker compose start mqtt-kafka-bridge >/dev/null 2>&1 && echo "  started bridge at t=25s"
    if wait "$rpid"; then echo "  replayer done: $(tail -n 1 "$work/replay$round.log")"
    else bad "round $round: replayer failed"; tail -n 5 "$work/replay$round.log"; fi
    s=$(wait_healthy $BRIDGE 60)
    [ "$s" = healthy ] || bad "round $round: bridge not healthy after restart ($s)"
    $PY scripts/kafka_check.py --snapshot "$snap" --compare-file data/raw/train_FD001.txt \
        --units $UNITS --expect-unique $ROWS --expect-dlq 0 --out "$out" | sed 's/^/  /'
    rc=${PIPESTATUS[0]}
    line=$($PY -c '
import json, sys
r = json.load(open(sys.argv[1])); c = r["comparison"] or {}
print("unique=%s missing=%s unexpected=%s duplicates=%s raw_regressions=%s dlq=%s" % (
    r["unique"], c.get("missing"), c.get("unexpected"), r["duplicates"], r["raw_regressions"], r["dlq"]))
' "$out" 2>/dev/null || echo "no summary (kafka_check rc=$rc)")
    summary+=("round $round: $line")
    [ "$rc" -eq 0 ] && ok "round $round: no loss ($line)" || bad "round $round: kafka_check rc=$rc ($line)"
  done
  echo; printf '  %s\n' "${summary[@]}"
  rm -rf "$work"
fi

hdr "RESULT"
echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ] || exit 1
