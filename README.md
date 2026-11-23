# pdm-pipeline

Real-time IoT predictive-maintenance pipeline: MQTT -> Kafka -> Flink -> TimescaleDB/Iceberg -> ML.

Currently implemented: the MQTT ingest broker.

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
gitignored. The container runs as uid 1883, so on a fresh clone create them
writable by that uid before the first `docker compose up`:

```bash
mkdir -p mosquitto/data mosquitto/log
chmod 0777 mosquitto/data mosquitto/log   # or: sudo chown -R 1883:1883 mosquitto/data mosquitto/log && chmod 0755 ...
```

Symptom of getting this wrong: `Error: Unable to open log file` or
`Error saving in-memory database` in `docker compose logs mosquitto`.
