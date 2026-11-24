#!/usr/bin/env bash
# One-shot topic bootstrap, run by the "kafka-init" compose service (apache/kafka image).
# Idempotent: creates missing topics, (re)applies their configs, grows partitions if a
# topic has fewer than declared. Fails (non-zero) if a topic has MORE partitions than
# declared, since Kafka cannot shrink them.
set -euo pipefail

BS="${BOOTSTRAP:-kafka:9092}"
BIN=/opt/kafka/bin

# name | partitions | configs (comma-separated)
TOPICS=(
  "sensor.telemetry.raw|6|retention.ms=604800000,compression.type=lz4"    # 7 days
  "sensor.telemetry.dlq|1|retention.ms=2592000000"                        # 30 days
)

for spec in "${TOPICS[@]}"; do
  IFS='|' read -r name parts configs <<<"$spec"
  cfg_args=()
  IFS=',' read -ra kvs <<<"$configs"
  for kv in "${kvs[@]}"; do cfg_args+=(--config "$kv"); done

  echo "==> $name: ensure exists ($parts partitions, $configs)"
  "$BIN/kafka-topics.sh" --bootstrap-server "$BS" --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor 1 "${cfg_args[@]}"

  echo "==> $name: apply configs"
  "$BIN/kafka-configs.sh" --bootstrap-server "$BS" --alter --entity-type topics \
    --entity-name "$name" --add-config "$configs"

  actual=$("$BIN/kafka-topics.sh" --bootstrap-server "$BS" --describe --topic "$name" \
             | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -1)
  if [ "$actual" -lt "$parts" ]; then
    echo "==> $name: growing partitions $actual -> $parts"
    "$BIN/kafka-topics.sh" --bootstrap-server "$BS" --alter --topic "$name" --partitions "$parts"
  elif [ "$actual" -gt "$parts" ]; then
    echo "ERROR: $name has $actual partitions, expected $parts (cannot shrink)" >&2
    exit 1
  fi
done

echo "==> topics:"
"$BIN/kafka-topics.sh" --bootstrap-server "$BS" --describe --topic 'sensor\.telemetry\..*'
echo "==> kafka-init done"
