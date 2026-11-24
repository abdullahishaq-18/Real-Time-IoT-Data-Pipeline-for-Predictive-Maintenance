"""Unit tests for the bridge's pure logic: validation, record building, in-order acks.
No brokers needed (and bridge.records / bridge.acks import no client libraries)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from bridge.acks import AckTracker
from bridge.records import (
    ValidationError,
    build_record,
    format_ingest_time,
    topic_engine_id,
    validate,
)

FILTER = "factory/engines/+/telemetry"
RAW, DLQ = "sensor.telemetry.raw", "sensor.telemetry.dlq"
NOW = datetime(2026, 11, 24, 10, 20, 30, 456789, tzinfo=timezone.utc)
TOPIC = "factory/engines/engine-081/telemetry"


def payload(**over) -> dict:
    p = {
        "engine_id": "engine-081",
        "cycle": 1,
        "event_time": "2026-11-23T19:32:57.167Z",
        "op_settings": [-0.005, 0.0003, 100.0],
        "sensors": {f"s{i}": float(i) for i in range(1, 22)},
        "vibration": 2.1319,
        "schema_version": 1,
    }
    p.update(over)
    return p


def raw_bytes(**over) -> bytes:
    # Deliberately unusual formatting: the bridge must forward bytes untouched.
    return json.dumps(payload(**over), indent=1, separators=(" ,", ":  ")).encode()


def build(topic: str, value: bytes):
    return build_record(topic, value, topic_filter=FILTER, raw_topic=RAW, dlq_topic=DLQ, now=NOW)


def hdrs(rec) -> dict[str, bytes]:
    return dict(rec.headers)


# --------------------------------------------------------------------------- topic parsing


def test_topic_engine_id():
    assert topic_engine_id(TOPIC, FILTER) == "engine-081"


@pytest.mark.parametrize("topic", [
    "factory/engines/engine-081",
    "factory/engines/engine-081/telemetry/extra",
    "factory/machines/engine-081/telemetry",
    "factory/engines//telemetry",
])
def test_topic_engine_id_rejects_non_matching(topic):
    with pytest.raises(ValidationError, match="bad_topic"):
        topic_engine_id(topic, FILTER)


@pytest.mark.parametrize("bad_filter", ["factory/engines/#", "factory/+/+/telemetry", "factory/engines/x"])
def test_topic_filter_must_have_one_plus(bad_filter):
    with pytest.raises(ValueError):
        topic_engine_id("factory/engines/x", bad_filter)


def test_ingest_time_format():
    assert format_ingest_time(NOW) == "2026-11-24T10:20:30.456Z"


# --------------------------------------------------------------------------- validation


def test_validate_ok():
    assert validate(TOPIC, raw_bytes(), FILTER) == "engine-081"


@pytest.mark.parametrize("value, reason", [
    (b"not json{", "invalid_json"),
    (b"\xff\xfe\x00garbage", "invalid_json"),
    (b"", "invalid_json"),
    (b"[1, 2, 3]", "not_an_object"),
    (b"42", "not_an_object"),
    (b"null", "not_an_object"),
])
def test_validate_rejects_non_objects(value, reason):
    with pytest.raises(ValidationError, match=f"^{reason}"):
        validate(TOPIC, value, FILTER)


def test_validate_missing_keys_listed():
    p = payload()
    del p["cycle"], p["sensors"]
    with pytest.raises(ValidationError, match="^missing_keys: cycle,sensors$"):
        validate(TOPIC, json.dumps(p).encode(), FILTER)


def test_validate_unexpected_keys():
    with pytest.raises(ValidationError, match="^unexpected_keys: extra$"):
        validate(TOPIC, raw_bytes(extra=1), FILTER)


@pytest.mark.parametrize("sv", [2, 0, "1", 1.0, None, True])
def test_validate_schema_version(sv):
    with pytest.raises(ValidationError, match="^schema_version_mismatch"):
        validate(TOPIC, raw_bytes(schema_version=sv), FILTER)


def test_validate_engine_id_mismatch():
    with pytest.raises(ValidationError, match="^engine_id_mismatch: topic='engine-081' payload='engine-082'"):
        validate(TOPIC, raw_bytes(engine_id="engine-082"), FILTER)


def test_validate_bad_topic():
    with pytest.raises(ValidationError, match="^bad_topic"):
        validate("factory/engines/engine-081", raw_bytes(), FILTER)


# --------------------------------------------------------------------------- record building


def test_build_valid_record_keeps_original_bytes():
    value = raw_bytes()
    rec = build(TOPIC, value)
    assert not rec.is_dlq
    assert rec.topic == RAW
    assert rec.key == b"engine-081"
    assert rec.value == value  # byte-for-byte, not re-serialized
    assert hdrs(rec) == {
        "mqtt_topic": TOPIC.encode(),
        "bridge_ingest_time": b"2026-11-24T10:20:30.456Z",
        "schema_version": b"1",
    }


def test_build_accepts_memoryview_payload():
    value = raw_bytes()
    assert build(TOPIC, memoryview(value)).value == value


@pytest.mark.parametrize("topic, value, reason, key", [
    (TOPIC, b"not json", "invalid_json", b"engine-081"),
    (TOPIC, json.dumps({"engine_id": "engine-081"}).encode(), "missing_keys", b"engine-081"),
    (TOPIC, raw_bytes(engine_id="engine-099"), "engine_id_mismatch", b"engine-081"),
    (TOPIC, raw_bytes(schema_version=2), "schema_version_mismatch", b"engine-081"),
    ("factory/engines/engine-081", raw_bytes(), "bad_topic", None),
])
def test_build_invalid_goes_to_dlq(topic, value, reason, key):
    rec = build(topic, value)
    assert rec.is_dlq
    assert rec.topic == DLQ
    assert rec.value == value
    assert rec.key == key
    h = hdrs(rec)
    assert h["mqtt_topic"] == topic.encode()
    assert h["error_reason"].decode().startswith(reason)
    assert rec.error_reason.startswith(reason)


def test_build_never_raises_on_garbage():
    for value in [b"", b"\x00" * 10, b"{" * 10000, "ü".encode("latin-1"), b'{"engine_id": 1}']:
        assert build(TOPIC, value).is_dlq


# --------------------------------------------------------------------------- ack ordering


def test_acks_only_after_delivery_and_in_order():
    t = AckTracker()
    a, b, c = t.add(1, 1, 1), t.add(2, 1, 1), t.add(3, 1, 1)
    assert t.pop_ready(1) == ([], 0)  # nothing delivered -> nothing acked
    t.mark_delivered(c)
    t.mark_delivered(b)
    assert t.pop_ready(1) == ([], 0)  # 1 still pending blocks 2 and 3
    assert t.pending == 3 and t.undelivered == 1
    t.mark_delivered(a)
    assert t.pop_ready(1) == ([(1, 1), (2, 1), (3, 1)], 0)
    assert t.pending == 0


def test_acks_partial_prefix():
    t = AckTracker()
    a, b, _ = t.add(1, 1, 1), t.add(2, 1, 1), t.add(3, 1, 1)
    t.mark_delivered(a)
    t.mark_delivered(b)
    assert t.pop_ready(1) == ([(1, 1), (2, 1)], 0)
    assert t.pending == 1


def test_stale_generation_not_acked():
    t = AckTracker()
    old = t.add(7, 1, generation=1)
    new = t.add(7, 1, generation=2)  # broker reused mid 7 after reconnect
    t.mark_delivered(old)
    t.mark_delivered(new)
    assert t.pop_ready(current_generation=2) == ([(7, 1)], 1)


def test_qos0_not_acked_but_released():
    t = AckTracker()
    t.mark_delivered(t.add(0, 0, 1))
    t.mark_delivered(t.add(5, 1, 1))
    assert t.pop_ready(1) == ([(5, 1)], 0)
    assert t.pending == 0


# --------------------------------------------------------------------------- config


def test_config_from_env():
    pytest.importorskip("confluent_kafka")
    pytest.importorskip("paho.mqtt")
    from bridge.bridge import Config

    d = Config.from_env({})
    assert (d.mqtt_host, d.mqtt_port, d.kafka_bootstrap) == ("mosquitto", 1883, "kafka:9092")
    c = Config.from_env({"MQTT_HOST": "localhost", "MQTT_PORT": "11883", "KAFKA_BOOTSTRAP": "localhost:9094",
                         "STATS_INTERVAL": "2.5"})
    assert (c.mqtt_host, c.mqtt_port, c.kafka_bootstrap, c.stats_interval) == \
        ("localhost", 11883, "localhost:9094", 2.5)
    with pytest.raises(ValueError):
        Config.from_env({"MQTT_TOPIC": "factory/#"})
