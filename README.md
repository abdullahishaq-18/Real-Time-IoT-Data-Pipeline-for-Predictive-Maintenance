# pdm-pipeline

Real-time IoT predictive-maintenance pipeline: MQTT -> Kafka -> Flink -> TimescaleDB/Iceberg -> ML.

Currently implemented: the MQTT ingest broker and a C-MAPSS telemetry replayer.

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

The tests exercise payload building and scheduling without a broker. Two tests
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
