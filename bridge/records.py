"""Pure validation and Kafka-record building for the MQTT -> Kafka bridge.

No I/O and no third-party imports, so everything here is unit-testable without brokers.

Contract (schema_version 1, see README "Payload schema"): the bridge checks only what it
needs to route safely -- the payload is a JSON object with exactly the contract keys,
schema_version == 1, and its engine_id matches the one in the MQTT topic. Full field-type
checks are the job of downstream consumers / scripts/kafka_check.py.

The Kafka record value is ALWAYS the original MQTT payload bytes, never re-serialized.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

SCHEMA_VERSION = 1
CONTRACT_KEYS = frozenset(
    ("engine_id", "cycle", "event_time", "op_settings", "sensors", "vibration", "schema_version")
)


@dataclass(frozen=True)
class Record:
    """One record ready for producer.produce()."""

    topic: str
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]] = field(default_factory=list)
    error_reason: str | None = None  # set only for DLQ records

    @property
    def is_dlq(self) -> bool:
        return self.error_reason is not None


class ValidationError(ValueError):
    """Payload or topic breaks the contract. str(e) is the DLQ error_reason."""


def format_ingest_time(ts: datetime) -> str:
    """ISO-8601 UTC, millisecond precision, Z suffix (same format as the payload's event_time)."""
    ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def topic_engine_id(topic: str, topic_filter: str) -> str:
    """Extract the engine_id that the single '+' wildcard of topic_filter matched.

    topic_engine_id("factory/engines/engine-081/telemetry", "factory/engines/+/telemetry")
    -> "engine-081"
    """
    f_parts = topic_filter.split("/")
    if f_parts.count("+") != 1 or "#" in f_parts:
        raise ValueError(f"topic filter {topic_filter!r} must contain exactly one '+' and no '#'")
    t_parts = topic.split("/")
    if len(t_parts) != len(f_parts):
        raise ValidationError(f"bad_topic: {topic!r} does not match {topic_filter!r}")
    engine_id = ""
    for f, t in zip(f_parts, t_parts):
        if f == "+":
            engine_id = t
        elif f != t:
            raise ValidationError(f"bad_topic: {topic!r} does not match {topic_filter!r}")
    if not engine_id:
        raise ValidationError(f"bad_topic: empty engine_id in {topic!r}")
    return engine_id


def validate(topic: str, payload: bytes, topic_filter: str) -> str:
    """Return the engine_id if the message honours the contract, else raise ValidationError.

    The error message is a short, stable reason code followed by detail, e.g.
    ``missing_keys: cycle,sensors``.
    """
    topic_eid = topic_engine_id(topic, topic_filter)
    try:
        doc = json.loads(payload)
    except (UnicodeDecodeError, ValueError) as e:  # JSONDecodeError subclasses ValueError
        raise ValidationError(f"invalid_json: {type(e).__name__}: {e}"[:300]) from None
    if not isinstance(doc, dict):
        raise ValidationError(f"not_an_object: top-level JSON is {type(doc).__name__}")
    keys = set(doc)
    missing = CONTRACT_KEYS - keys
    if missing:
        raise ValidationError("missing_keys: " + ",".join(sorted(missing)))
    extra = keys - CONTRACT_KEYS
    if extra:
        raise ValidationError("unexpected_keys: " + ",".join(sorted(extra)))
    sv = doc["schema_version"]
    if type(sv) is not int or sv != SCHEMA_VERSION:
        raise ValidationError(f"schema_version_mismatch: got {sv!r}, expected {SCHEMA_VERSION}")
    eid = doc["engine_id"]
    if eid != topic_eid:
        raise ValidationError(f"engine_id_mismatch: topic={topic_eid!r} payload={eid!r}")
    return eid


def build_record(
    topic: str,
    payload: bytes,
    *,
    topic_filter: str,
    raw_topic: str,
    dlq_topic: str,
    now: datetime,
) -> Record:
    """Turn one MQTT message into a Kafka record: the raw topic if valid, the DLQ otherwise.

    Never raises for bad input -- a bad message becomes a DLQ record.
    """
    payload = bytes(payload)  # paho may hand us bytearray/memoryview
    ingest = format_ingest_time(now).encode()
    try:
        engine_id = validate(topic, payload, topic_filter)
    except ValidationError as e:
        try:
            key = topic_engine_id(topic, topic_filter).encode()
        except ValidationError:
            key = None
        reason = str(e)
        return Record(
            topic=dlq_topic,
            key=key,
            value=bytes(payload),
            headers=[
                ("error_reason", reason.encode()),
                ("mqtt_topic", topic.encode()),
                ("bridge_ingest_time", ingest),
            ],
            error_reason=reason,
        )
    return Record(
        topic=raw_topic,
        key=engine_id.encode(),
        value=bytes(payload),
        headers=[
            ("mqtt_topic", topic.encode()),
            ("bridge_ingest_time", ingest),
            ("schema_version", str(SCHEMA_VERSION).encode()),
        ],
    )
