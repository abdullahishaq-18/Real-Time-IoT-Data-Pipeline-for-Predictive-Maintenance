# pdm-pipeline

Real-time IoT predictive-maintenance pipeline: MQTT -> Kafka -> Flink -> TimescaleDB/Iceberg -> ML.

Currently implemented: the MQTT ingest broker, a C-MAPSS telemetry replayer, Kafka, and an
MQTT -> Kafka bridge.

## Running the broker

Local dev broker is Eclipse Mosquitto 2.0.23 in Docker, listening on `1883` with
anonymous access enabled (**local dev only** -- see the note at the top of
`mosquitto/config/mosquitto.conf`).

```bash
# start (detached) and watch it become healthy
docker compose up -d
docker compose ps

# follow broker logs
docker compose logs -f mosquitto

# stop (keeps the persistence DB and logs)
docker compose stop

# tear down the container and the "pdm" network
docker compose down
```

Subscribe and publish from the host (needs `mosquitto-clients`, or use the
`paho-mqtt` client in `.venv`):

```bash
# subscribe to everything
mosquitto_sub -h localhost -p 1883 -t '#' -v

# subscribe to one topic, exit after the first message
mosquitto_sub -h localhost -p 1883 -t test/roundtrip -C 1 -W 10

# publish
mosquitto_pub -h localhost -p 1883 -t test/roundtrip -m hello
```

The same clients ship inside the image, so no host install is required:

```bash
docker compose exec mosquitto mosquitto_sub -h 127.0.0.1 -t 'sensors/#' -v
docker compose exec mosquitto mosquitto_pub -h 127.0.0.1 -t sensors/test -m hello
```

### State and permissions

`mosquitto/data/` (persistence DB) and `mosquitto/log/` are bind-mounted and
gitignored. The container runs as uid/gid 1883 (`user: "1883:1883"` in
`docker-compose.yml`), so on a fresh clone create them owned by that uid before
the first `docker compose up`. A throwaway container does the chown without sudo:

```bash
mkdir -p mosquitto/data mosquitto/log
docker run --rm -v "$PWD/mosquitto/data:/d" -v "$PWD/mosquitto/log:/l" alpine:3.20 \
  sh -c 'chown -R 1883:1883 /d /l && chmod 0755 /d /l'
```

Files the broker writes are mode 0600 and owned by 1883, so read the log with
`docker compose logs mosquitto` rather than from the host.

`scripts/verify-broker.sh` runs the broker's end-to-end checks (config, health,
round trip, host connectivity, clean logs, restart).

Symptom of getting this wrong: `Error: Unable to open log file` or
`Error saving in-memory database` in `docker compose logs mosquitto`.

## Replaying sensor data

`replayer/replay.py` streams the NASA C-MAPSS turbofan dataset into the broker as
if each engine were a live machine: one message per engine per operating cycle,
at QoS 1, on `factory/engines/{engine_id}/telemetry`.

### Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# data/raw/ -> the extracted C-MAPSS dataset (train_FD001.txt, ...); gitignored
ln -s /path/to/CMAPSSData data/raw
```

### Usage

```bash
# defaults: data/raw/train_FD001.txt, engines 81-100, 1 cycle = 1 simulated second, real time
.venv/bin/python replayer/replay.py

# 50x faster, stop after 500 messages
.venv/bin/python replayer/replay.py --speed 50 --limit 500

# one simulated cycle = 1 hour of event time, replayed at 3600x (one cycle per wall second)
.venv/bin/python replayer/replay.py --cycle-seconds 3600 --speed 3600

# inspect payloads without a broker (unpaced; JSON on stdout, logs on stderr)
.venv/bin/python replayer/replay.py --dry-run --limit 5

# watch the stream
docker compose exec mosquitto mosquitto_sub -h 127.0.0.1 -t 'factory/engines/+/telemetry' -v
```

| flag | default | meaning |
|---|---|---|
| `--file` | `data/raw/train_FD001.txt` | C-MAPSS file to replay |
| `--units` | `81-100` | engines to replay (`81-100`, `85`, `81-85,90`). **Engines 1-80 are reserved for model training**; the default replays only the hold-out set |
| `--cycle-seconds` | `1.0` | simulated (event-time) seconds per cycle |
| `--speed` | `1.0` | wall-clock multiplier; changes how fast messages are sent, never `event_time` |
| `--max-offset` | `50` | each engine starts after a random 0..N cycle delay, so engines are staggered rather than in lockstep |
| `--seed` | `42` | seeds both the start offsets and the vibration noise, so runs are reproducible |
| `--host` / `--port` | `localhost` / `1883` | broker address |
| `--dry-run` | off | print payloads instead of publishing |
| `--limit` | none | stop after N messages |

Timing: all engines share one tick timeline (`tick = cycle + offset`).
`event_time` advances by `--cycle-seconds` per tick, and engines on the same tick
share a timestamp. Messages are sent at `(tick - first_tick) * cycle_seconds / speed`
wall seconds after a single monotonic start, so pacing doesn't drift. Ctrl+C
flushes the last publish, prints a summary and exits 130. If the broker can't be
reached, the replayer exits 2 with a one-line error.

### Payload schema (`schema_version: 1`)

This is the contract for the downstream Kafka/Flink stages. Changing it requires
bumping `schema_version`.

```json
{
  "engine_id": "engine-081",
  "cycle": 1,
  "event_time": "2026-11-23T19:32:57.167Z",
  "op_settings": [-0.005, 0.0003, 100.0],
  "sensors": {"s1": 518.67, "s2": 642.04, "...": "...", "s21": 23.3365},
  "vibration": 2.1319,
  "schema_version": 1
}
```

| field | type | notes |
|---|---|---|
| `engine_id` | string | `engine-` + zero-padded 3-digit C-MAPSS unit number |
| `cycle` | int | C-MAPSS operating cycle, starts at 1, strictly increasing per engine |
| `event_time` | string | ISO-8601 UTC, millisecond precision, `Z` suffix |
| `op_settings` | [float, float, float] | C-MAPSS operational settings 1-3 |
| `sensors` | object | exactly `s1`..`s21`, all floats (integer-valued sensors such as `s17` are sent as `392.0`) |
| `vibration` | float | **SYNTHETIC**, see below |
| `schema_version` | int | always `1` |

**`vibration` is synthetic.** C-MAPSS has no vibration sensor. The replayer
generates `vibration = 2.0 + 3.0 * (cycle / engine_max_cycle)^3 + N(0, 0.15)`, where
`engine_max_cycle` is that engine's last cycle in the file. That gives roughly 2.0
for most of an engine's life, rising towards about 5.0 at failure. It's not
measured data. Downstream models should treat it as a toy feature. Because it's
scaled to the engine's last cycle in the file, it only reaches its peak on the
`train_*` files, which run to failure. `test_*` files are truncated before failure.

### Tests

```bash
.venv/bin/python -m pytest -v
```

The tests exercise payload building, scheduling, bridge validation, record building, ack
ordering and the Kafka verifier's analysis, all without a broker. Two tests
also run against `data/raw/train_FD001.txt` and are skipped if it's missing.

For a live end-to-end check, `scripts/e2e_sub.py` subscribes to the telemetry
topic and validates every message against the schema above. It also checks that
each engine's cycles arrive in strictly increasing order. It exits 0 only if
every check passes (1 if any fails, 2 if it can't reach the broker). Start it
before the replayer, since it only sees messages sent after it subscribes. It
stops after `--idle` seconds (default 5) without a message.

```bash
# terminal 1, then terminal 2
.venv/bin/python scripts/e2e_sub.py --expect-count 500 --min-engines 10
.venv/bin/python replayer/replay.py --speed 50 --limit 500

# full hold-out run: message count == file rows, last cycle == file max per engine
.venv/bin/python scripts/e2e_sub.py --compare-file data/raw/train_FD001.txt --units 81-100
.venv/bin/python replayer/replay.py --speed 100
```

Run `.venv/bin/python scripts/e2e_sub.py --help` for all options, including
`--out result.json` and `--ready-file`, which lets a script wait until the
subscriber is ready before starting the replayer.

## Kafka and the bridge

### Architecture

```
replayer / devices                                               kafka-ui :8080
      |  MQTT QoS 1                                                      |
      v                                                                  v
 mosquitto :1883 --(persistent session)--> mqtt-kafka-bridge --> kafka (KRaft) :9092 / :9094
  queues up to 200k msgs                    ack only after       sensor.telemetry.raw (6 part.)
  per offline client                        Kafka confirms       sensor.telemetry.dlq (1 part.)
                                                                         ^
                                                     kafka-init: creates topics, exits 0
```

| service | image | memory | notes |
|---|---|---|---|
| `mosquitto` | `eclipse-mosquitto:2.0.23` | 384m | `max_queued_messages 200000`, `persistent_client_expiration 1d` |
| `kafka` | `apache/kafka:4.4.0` | 1g (heap 512m) | single node, combined broker + controller, no ZooKeeper, `auto.create.topics.enable=false` |
| `kafka-init` | `apache/kafka:4.4.0` | 512m | one-shot `kafka/init-topics.sh`, idempotent; other services wait for it to exit 0 |
| `kafka-ui` | `kafbat/kafka-ui:v1.5.1` | 512m | http://localhost:8080, read-only, no auth (local dev only) |
| `mqtt-kafka-bridge` | built from `bridge/` (`python:3.12.16-slim`, non-root) | 256m | `bridge/bridge.py` |

Kafka listeners: `kafka:9092` for containers on the `pdm` network, `localhost:9094` from
the host. Kafka data lives in the named volume `pdm-kafka-data`. To start from an empty
cluster, run `docker compose down -v`.

```bash
docker compose up -d --build     # whole stack; kafka-init runs once and exits 0
scripts/verify-stack.sh          # config, health, verify-broker.sh, topics, Kafka UI
scripts/verify-stack.sh --crash  # ... plus 3 bridge SIGKILL rounds (--crash=N for N), ~1 min each
docker compose logs -f mqtt-kafka-bridge   # counters every 10 s
```

### Topics

| topic | partitions | retention | key | value |
|---|---|---|---|---|
| `sensor.telemetry.raw` | 6 | 7 days, lz4 | `engine_id` | original MQTT payload bytes (schema above), unchanged |
| `sensor.telemetry.dlq` | 1 | 30 days | `engine_id` from the topic (if parseable) | original MQTT payload bytes |

Headers on raw records: `mqtt_topic`, `bridge_ingest_time` (ISO-8601 UTC, ms), `schema_version`.
Headers on DLQ records: `error_reason`, `mqtt_topic`, `bridge_ingest_time`.

The key is the `engine_id` and the bridge uses the Java-compatible `murmur2` partitioner,
so all of an engine's records land in one partition, in order. Java/Flink producers map
the same keys to the same partitions.

The bridge sends a message to the DLQ when the payload is not JSON (`invalid_json`), is
not an object (`not_an_object`), lacks contract keys (`missing_keys: ...`), has extra keys
(`unexpected_keys: ...`), has `schema_version != 1` (`schema_version_mismatch`), or has an
`engine_id` that doesn't match the topic (`engine_id_mismatch`). Deeper type checks are left
to consumers. A bad message is logged, sent to the DLQ and acked. It never stops the bridge.

### Delivery guarantees: at-least-once

* The bridge subscribes at QoS 1 with a fixed client id and a persistent MQTT 5 session
  (`clean_start=False`, session expiry 1 day). While the bridge is down, Mosquitto queues
  its messages, up to `max_queued_messages`.
* The producer is idempotent (`acks=all`, `enable.idempotence=true`), so its own retries
  don't create duplicates or reorder records. Records never time out locally
  (`message.timeout.ms=0`). During a Kafka outage they wait in the producer. The bridge
  allows at most 1000 unacked MQTT messages in flight (MQTT receive maximum), and Mosquitto
  queues the rest. That is backpressure, not loss.
* paho runs in manual-ack mode. An MQTT message is acked **only from the Kafka delivery
  report**, never earlier, and acks go out in the order the messages were received, as MQTT
  requires.
* On SIGTERM/SIGINT (`docker compose stop`, Ctrl+C) the bridge stops consuming, flushes the
  producer, acks what was delivered and exits 0. Anything it did not finish is redelivered
  in the next session.
* **Duplicates are possible** after a crash or network drop, when Kafka has the record but
  the PUBACK never reached Mosquitto. Consumers must deduplicate on `(engine_id, cycle)`,
  which is unique per the payload contract. Loss is not possible, short of Mosquitto's queue
  overflowing or the session expiring after a day offline.

Measured with `scripts/kafka_check.py` on the hold-out replay (units 81-100, 4493 rows):
all 4493 `(engine_id, cycle)` rows arrived, with 0 DLQ records, for a plain run at
`--speed 50`, for `docker compose stop` + `start` of the bridge mid-replay, and for
`docker compose restart kafka` mid-replay. Both outages ended with 0 duplicates, because
the bridge shut down gracefully and the idempotent producer retried in order.

A crash is different from a graceful stop. If the bridge is SIGKILLed (no flush, no final
acks), it can produce duplicates, but nothing is lost. `scripts/verify-stack.sh --crash`
replays units 81-100 at `--speed 20`, runs `docker kill` on the bridge after 10 s, leaves it
down for 15 s and then starts it again. In testing every run delivered all 4493 rows, with
0 missing and 0 DLQ, and 10-15 duplicates per run (one run had 0). The Flink stage (not
built yet) is responsible for deduplicating on `(engine_id, cycle)`.

`docker kill` counts as a manual stop, so `restart: unless-stopped` does **not** bring the
container back, and the test starts it again with `docker compose start`. A real crash of
the process, or an OOM kill at the 256m `mem_limit`, is not a manual stop, so Docker
restarts the container automatically.

### Running the bridge from the host

Defaults target the compose network. Override them to run with the `.venv`, and stop the
container first, because both use the same client id and session:

```bash
docker compose stop mqtt-kafka-bridge
MQTT_HOST=localhost KAFKA_BOOTSTRAP=localhost:9094 HEARTBEAT_FILE=/tmp/bridge-hb \
  .venv/bin/python -m bridge.bridge
```

Env vars: `MQTT_HOST` (`mosquitto`), `MQTT_PORT` (`1883`), `MQTT_TOPIC`
(`factory/engines/+/telemetry`), `MQTT_CLIENT_ID` (`pdm-mqtt-kafka-bridge`),
`MQTT_SESSION_EXPIRY` (`86400`), `MQTT_RECEIVE_MAXIMUM` (`1000`), `KAFKA_BOOTSTRAP`
(`kafka:9092`), `KAFKA_TOPIC_RAW`, `KAFKA_TOPIC_DLQ`, `HEARTBEAT_FILE`
(`/tmp/bridge-heartbeat`), `STATS_INTERVAL` (`10`), `SHUTDOWN_TIMEOUT` (`30`), `LOG_LEVEL`.

Health: the main loop touches `HEARTBEAT_FILE` every second, and the container healthcheck
fails if it is older than 30 s. The stats line reports `received`, `produced`, `dlq`,
`pending` (received but not yet acked), `errors`, `acked`, `stale` (delivered, but received
on a connection that has since dropped, so not acked; Mosquitto redelivers them), and
`kafka_queue`.

### Verifying a run against Kafka

`scripts/kafka_check.py` checks only the records written after a snapshot, so earlier
data doesn't affect the counts. It exits 0 on pass, 1 on fail and 2 if it can't reach Kafka.

```bash
.venv/bin/python scripts/kafka_check.py --save-snapshot /tmp/snap.json
.venv/bin/python replayer/replay.py --speed 50
.venv/bin/python scripts/kafka_check.py --snapshot /tmp/snap.json \
    --compare-file data/raw/train_FD001.txt --units 81-100 --expect-unique 4493 --expect-dlq 0
```

It waits until the topic end offsets stop moving, then checks:

* every value against the payload contract (the same `check()` as `e2e_sub.py`)
* that the key equals `engine_id`
* that each engine is in one partition
* per-engine cycle order
* unique and duplicate counts
* the exact `(engine_id, cycle)` set against the file
* the DLQ count

It also prints how engines are spread across the 6 partitions. A redelivered duplicate
can step back to a cycle that was already seen. That is reported as `raw_regressions` and
is not a failure. A *new* cycle arriving out of order is a failure.

### Inspecting the DLQ

```bash
# Kafka UI: Topics -> sensor.telemetry.dlq -> Messages (headers show error_reason)
# or the console consumer, with headers:
docker exec -e KAFKA_HEAP_OPTS=-Xmx96m pdm-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic sensor.telemetry.dlq --from-beginning \
  --formatter-property print.headers=true --formatter-property print.key=true \
  --timeout-ms 5000 2>/dev/null   # it exits via a (harmless) TimeoutException after 5 s idle
# bridge log lines for each DLQ'd message:
docker compose logs mqtt-kafka-bridge | grep DLQ
```

Fix the producer and republish. The DLQ keeps the original bytes for 30 days.
