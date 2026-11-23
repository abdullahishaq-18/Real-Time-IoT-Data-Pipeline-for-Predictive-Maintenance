#!/usr/bin/env bash
# Runs checks a-f for the pdm-pipeline mosquitto broker.
# Usage: scripts/verify-broker.sh   (from anywhere; cds to the repo root)
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

pass=0; fail=0
ok()   { echo "  PASS: $*"; pass=$((pass+1)); }
bad()  { echo "  FAIL: $*"; fail=$((fail+1)); }
hdr()  { echo; echo "================ $* ================"; }

command -v docker >/dev/null 2>&1 || { echo "ABORT: docker is not installed on this machine."; exit 127; }

hdr "a. docker compose config"
if docker compose config; then ok "compose config validated"; else bad "compose config rejected the file"; fi

hdr "b. up -d + wait for healthy (60s timeout)"
docker compose up -d
deadline=$((SECONDS+60)); status=unknown
while [ $SECONDS -lt $deadline ]; do
  status=$(docker inspect -f '{{.State.Health.Status}}' pdm-mosquitto 2>/dev/null || echo missing)
  echo "  t=$((SECONDS))s health=$status"
  [ "$status" = healthy ] && break
  sleep 3
done
[ "$status" = healthy ] && ok "container healthy" || bad "never reached healthy (last=$status)"
echo "--- last health probe output ---"
docker inspect -f '{{json .State.Health}}' pdm-mosquitto 2>/dev/null | python3 -m json.tool 2>/dev/null | tail -20

hdr "c. in-container round trip on test/roundtrip"
out=$(mktemp)
docker compose exec -T mosquitto mosquitto_sub -h 127.0.0.1 -p 1883 -t test/roundtrip -C 1 -W 10 >"$out" 2>&1 &
subpid=$!
sleep 2
docker compose exec -T mosquitto mosquitto_pub -h 127.0.0.1 -p 1883 -t test/roundtrip -m hello
wait $subpid; rc=$?
echo "  subscriber rc=$rc output=[$(cat "$out")]"
[ "$rc" -eq 0 ] && [ "$(cat "$out")" = "hello" ] && ok "received 'hello'" || bad "round trip did not deliver 'hello'"
rm -f "$out"

hdr "d. host-side connectivity to localhost:1883"
if nc -z -w 5 localhost 1883; then ok "nc -z localhost 1883"; else bad "nc could not connect"; fi
if .venv/bin/python - <<'PY'
import sys, paho.mqtt.client as mqtt
c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="host-check")
try:
    c.connect("localhost", 1883, 10)
    c.loop_start()
    r = c.publish("healthcheck/host", "from-host", qos=1)
    r.wait_for_publish(10)
    c.loop_stop(); c.disconnect()
    print("  paho: connected and QoS1 publish acknowledged")
except Exception as e:
    print(f"  paho: FAILED {type(e).__name__}: {e}"); sys.exit(1)
PY
then ok "paho-mqtt connect + publish from host"; else bad "paho-mqtt could not reach the broker"; fi

hdr "e. logs clean of errors / permission warnings"
docker compose logs mosquitto
if docker compose logs mosquitto 2>&1 | grep -Ei 'error|denied|permission|unable to|warn|read-only|chown|fail'; then
  bad "suspicious lines in logs (above)"
else
  ok "no error/permission lines in logs"
fi

hdr "f. restart and re-confirm healthy"
docker compose restart mosquitto
deadline=$((SECONDS+60)); status=unknown
while [ $SECONDS -lt $deadline ]; do
  status=$(docker inspect -f '{{.State.Health.Status}}' pdm-mosquitto 2>/dev/null || echo missing)
  echo "  t=$((SECONDS))s health=$status"
  [ "$status" = healthy ] && break
  sleep 3
done
[ "$status" = healthy ] && ok "healthy after restart" || bad "unhealthy after restart (last=$status)"

hdr "RESULT"
echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ] || exit 1
