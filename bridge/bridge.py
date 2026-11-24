"""MQTT -> Kafka bridge with at-least-once delivery.

Subscribes to factory/engines/+/telemetry (QoS 1, persistent MQTT v5 session) and
produces each message to Kafka (idempotent, acks=all). Valid messages go to the raw
topic, contract violations to the DLQ topic. An MQTT message is acknowledged ONLY after
Kafka confirmed its record, so anything not yet in Kafka is redelivered by Mosquitto
after a crash or restart. Duplicates are possible; loss is not.

Threading:
  * paho's network thread runs on_connect / on_message. on_message does nothing but
    put (topic, payload, mid, qos, generation) on a thread-safe queue.
  * The main thread does everything else: validate, produce, producer.poll() (which runs
    the Kafka delivery callbacks), ack in receive order, heartbeat and stats. So the
    ack tracker and counters are only ever touched by one thread.

Run in compose (env defaults) or from the host:
  MQTT_HOST=localhost KAFKA_BOOTSTRAP=localhost:9094 .venv/bin/python -m bridge.bridge
"""

from __future__ import annotations

import logging
import os
import queue
import signal
import sys
import threading
import time
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from confluent_kafka import KafkaError, KafkaException, Producer
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from bridge.acks import AckTracker, Entry
from bridge.records import Record, build_record, topic_engine_id

log = logging.getLogger("bridge")


@dataclass(frozen=True)
class Config:
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_topic: str = "factory/engines/+/telemetry"
    mqtt_client_id: str = "pdm-mqtt-kafka-bridge"
    mqtt_session_expiry: int = 86400  # seconds Mosquitto keeps our session (and queue) while we're away
    mqtt_receive_maximum: int = 1000  # max unacked QoS 1 messages the broker sends us at once
    kafka_bootstrap: str = "kafka:9092"
    kafka_topic_raw: str = "sensor.telemetry.raw"
    kafka_topic_dlq: str = "sensor.telemetry.dlq"
    heartbeat_file: str = "/tmp/bridge-heartbeat"
    stats_interval: float = 10.0
    shutdown_timeout: float = 30.0
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        env = os.environ if env is None else env
        kw = {}
        for f in fields(cls):
            raw = env.get(f.name.upper())
            if raw is not None and raw != "":
                kw[f.name] = type(f.default)(raw)
        cfg = cls(**kw)
        topic_engine_id(cfg.mqtt_topic.replace("+", "x"), cfg.mqtt_topic)  # validates the filter shape
        return cfg


class Bridge:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.inbox: queue.SimpleQueue = queue.SimpleQueue()
        self.accepting = threading.Event()
        self.accepting.set()
        self.stopping = threading.Event()
        self.generation = 0  # bumped by paho's thread on every (re)connect
        self.mqtt_connected = False
        self.tracker = AckTracker()
        self.retry: list[tuple[Record, Entry]] = []
        self.fatal: KafkaError | None = None
        self.c = dict(received=0, produced=0, dlq=0, errors=0, acked=0, stale=0)
        self._last_hb = 0.0
        self.producer = Producer({
            "bootstrap.servers": cfg.kafka_bootstrap,
            "client.id": cfg.mqtt_client_id,
            "enable.idempotence": True,  # no dups / reordering from producer retries
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
            # Never time a record out locally: during a Kafka outage records wait in the
            # producer, the unacked MQTT window fills up (receive maximum) and Mosquitto
            # queues the rest. Backpressure instead of loss.
            "message.timeout.ms": 0,
            # Same key -> partition mapping as the Java client, so Flink/Java producers agree.
            "partitioner": "murmur2_random",
            "reconnect.backoff.ms": 200,
            "reconnect.backoff.max.ms": 10000,
            "error_cb": self._on_kafka_error,
        })
        self.client = self._make_mqtt_client()

    # ------------------------------------------------------------------ MQTT (paho thread)

    def _make_mqtt_client(self) -> mqtt.Client:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.cfg.mqtt_client_id,
                        protocol=mqtt.MQTTv5)
        c.manual_ack_set(True)
        c.reconnect_delay_set(min_delay=1, max_delay=30)  # exponential backoff
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        c.on_subscribe = self._on_subscribe
        return c

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.error("MQTT broker refused connection: %s", reason_code)
            return
        self.generation += 1
        self.mqtt_connected = True
        log.info("MQTT connected to %s:%d (session_present=%s, generation=%d)",
                 self.cfg.mqtt_host, self.cfg.mqtt_port, flags.session_present, self.generation)
        client.subscribe(self.cfg.mqtt_topic, qos=1)

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        if any(rc.is_failure for rc in reason_codes):
            log.error("subscribe to %s rejected: %s", self.cfg.mqtt_topic, reason_codes)
        else:
            log.info("subscribed to %s (QoS 1)", self.cfg.mqtt_topic)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.mqtt_connected = False
        if reason_code.is_failure:
            log.warning("MQTT disconnected: %s; reconnecting with backoff", reason_code)

    def _on_message(self, client, userdata, msg):
        if not self.accepting.is_set():
            return  # shutting down: leave unacked, Mosquitto redelivers next session
        self.inbox.put((msg.topic, bytes(msg.payload), msg.mid, msg.qos, self.generation))

    # ------------------------------------------------------------------ Kafka (main thread)

    def _on_kafka_error(self, err: KafkaError):
        self.c["errors"] += 1
        if err.fatal():
            self.fatal = err
            log.error("FATAL Kafka error: %s", err)
        else:
            log.warning("Kafka: %s", err)

    def _delivery_cb(self, rec: Record, entry: Entry):
        def cb(err, msg):
            if err is not None:
                self.c["errors"] += 1
                if err.fatal():
                    self.fatal = err
                    log.error("FATAL delivery error for %s: %s", rec.topic, err)
                else:
                    log.warning("delivery to %s failed (%s); retrying", rec.topic, err)
                    self.retry.append((rec, entry))
                return
            entry.delivered = True
            self.c["dlq" if rec.is_dlq else "produced"] += 1
        return cb

    def _produce(self, rec: Record, entry: Entry) -> None:
        while True:
            try:
                self.producer.produce(rec.topic, value=rec.value, key=rec.key, headers=rec.headers,
                                      on_delivery=self._delivery_cb(rec, entry))
                return
            except BufferError:  # local producer queue full: serve callbacks, try again
                self.producer.poll(0.1)
            except KafkaException as e:
                self.c["errors"] += 1
                log.warning("produce to %s failed: %s; retrying in 1s", rec.topic, e)
                self.producer.poll(1.0)
            self._heartbeat()
            if self.fatal is not None:
                return

    def _handle(self, item) -> None:
        topic, payload, mid, qos, gen = item
        self.c["received"] += 1
        try:
            rec = build_record(topic, payload, topic_filter=self.cfg.mqtt_topic,
                               raw_topic=self.cfg.kafka_topic_raw, dlq_topic=self.cfg.kafka_topic_dlq,
                               now=datetime.now(timezone.utc))
        except Exception as e:  # noqa: BLE001 -- defensive: a bad message must never crash us
            self.c["errors"] += 1
            log.exception("unexpected error building record for %s", topic)
            rec = Record(self.cfg.kafka_topic_dlq, None, payload,
                         [("error_reason", f"internal_error: {e!r}"[:300].encode()),
                          ("mqtt_topic", topic.encode())], error_reason="internal_error")
        if rec.is_dlq:
            log.warning("DLQ %s: %s", topic, rec.error_reason)
        self._produce(rec, self.tracker.add(mid, qos, gen))

    def _ack_ready(self) -> None:
        to_ack, stale = self.tracker.pop_ready(self.generation)
        for mid, qos in to_ack:
            self.client.ack(mid, qos)
        self.c["acked"] += len(to_ack)
        self.c["stale"] += stale

    # ------------------------------------------------------------------ housekeeping

    def _heartbeat(self) -> None:
        now = time.time()
        if now - self._last_hb >= 1.0:
            Path(self.cfg.heartbeat_file).write_text(f"{now:.0f}\n")
            self._last_hb = now

    @property
    def pending(self) -> int:
        return self.tracker.pending + self.inbox.qsize()

    def _log_stats(self) -> None:
        c = self.c
        log.info("stats received=%d produced=%d dlq=%d pending=%d errors=%d acked=%d stale=%d "
                 "kafka_queue=%d mqtt_connected=%s",
                 c["received"], c["produced"], c["dlq"], self.pending, c["errors"], c["acked"],
                 c["stale"], len(self.producer), self.mqtt_connected)

    def wait_for_kafka(self) -> bool:
        """Block until Kafka answers and both topics exist (exponential backoff)."""
        delay = 1.0
        while not self.stopping.is_set():
            try:
                md = self.producer.list_topics(timeout=5)
                missing = {self.cfg.kafka_topic_raw, self.cfg.kafka_topic_dlq} - set(md.topics)
                if not missing:
                    log.info("Kafka reachable at %s, topics present", self.cfg.kafka_bootstrap)
                    return True
                log.warning("Kafka up but topics missing: %s", sorted(missing))
            except KafkaException as e:
                log.warning("Kafka not reachable at %s: %s", self.cfg.kafka_bootstrap, e)
            self.stopping.wait(delay)
            delay = min(delay * 2, 30.0)
        return False

    # ------------------------------------------------------------------ main loop

    def run(self) -> int:
        if not self.wait_for_kafka():
            return 0
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = self.cfg.mqtt_session_expiry
        props.ReceiveMaximum = self.cfg.mqtt_receive_maximum
        # clean_start=False on EVERY connect (paho's default only resumes after the first),
        # so the queued backlog from while we were down is delivered.
        self.client.connect_async(self.cfg.mqtt_host, self.cfg.mqtt_port, keepalive=30,
                                  clean_start=False, properties=props)
        self.client.loop_start()  # retries the first connection too, with backoff

        next_stats = time.monotonic() + self.cfg.stats_interval
        while not self.stopping.is_set() and self.fatal is None:
            try:
                item = self.inbox.get(timeout=0.05)
            except queue.Empty:
                item = None
            budget = 1000
            while item is not None:
                self._handle(item)
                budget -= 1
                if budget == 0:
                    break
                try:
                    item = self.inbox.get_nowait()
                except queue.Empty:
                    item = None
            if self.retry:
                retry, self.retry = self.retry, []
                for rec, entry in retry:
                    self._produce(rec, entry)
            self.producer.poll(0)  # runs delivery callbacks on this thread
            self._ack_ready()
            self._heartbeat()
            if time.monotonic() >= next_stats:
                self._log_stats()
                next_stats += self.cfg.stats_interval
        return self.shutdown()

    def shutdown(self) -> int:
        log.info("shutting down: stop consuming, flush producer, ack delivered")
        self.accepting.clear()
        dropped = 0
        while True:  # received but never produced: leave unacked, they come back next session
            try:
                self.inbox.get_nowait()
                dropped += 1
            except queue.Empty:
                break
        remaining = self.producer.flush(self.cfg.shutdown_timeout) if self.fatal is None else len(self.producer)
        self._ack_ready()
        self._log_stats()
        log.info("shutdown: dropped_unproduced=%d unflushed=%d unacked=%d (will be redelivered)",
                 dropped, remaining, self.tracker.pending)
        # PUBACKs are queued ahead of DISCONNECT, so the network thread sends them first.
        self.client.disconnect()
        self.client.loop_stop()
        return 1 if self.fatal is not None else 0


def main() -> int:
    cfg = Config.from_env()
    logging.basicConfig(level=cfg.log_level.upper(), stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info("config: %s", cfg)
    bridge = Bridge(cfg)

    def on_signal(signum, frame):
        log.info("received %s", signal.Signals(signum).name)
        bridge.stopping.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    return bridge.run()


if __name__ == "__main__":
    sys.exit(main())
