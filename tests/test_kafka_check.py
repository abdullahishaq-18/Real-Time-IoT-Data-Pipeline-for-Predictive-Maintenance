"""Tests for scripts/kafka_check.py's analysis (no Kafka needed)."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("confluent_kafka")
from scripts.kafka_check import analyze_raw  # noqa: E402


class Msg:
    """Minimal stand-in for confluent_kafka.Message."""

    def __init__(self, engine: str, cycle: int, partition: int = 0, offset: int = 0, key: str | None = None,
                 topic_engine: str | None = None):
        self._p, self._o = partition, offset
        self._key = (key if key is not None else engine).encode()
        p = {
            "engine_id": engine, "cycle": cycle, "event_time": "2026-11-23T19:32:57.167Z",
            "op_settings": [0.0, 0.0, 100.0], "sensors": {f"s{i}": 1.0 for i in range(1, 22)},
            "vibration": 2.0, "schema_version": 1,
        }
        self._value = json.dumps(p).encode()
        self._headers = [("mqtt_topic", f"factory/engines/{topic_engine or engine}/telemetry".encode()),
                         ("bridge_ingest_time", b"2026-11-23T19:32:57.200Z"), ("schema_version", b"1")]

    def key(self): return self._key
    def value(self): return self._value
    def headers(self): return self._headers
    def partition(self): return self._p
    def offset(self): return self._o


E1, E2 = "engine-081", "engine-082"


def test_clean_stream():
    r = analyze_raw([Msg(E1, 1), Msg(E2, 1, 1), Msg(E1, 2), Msg(E2, 2, 1)])
    assert (r["total"], r["unique"], r["duplicates"], r["order_violations"]) == (4, 4, 0, 0)
    assert r["engine_partitions"] == {E1: [0], E2: [1]}
    assert not r["invalid"]


def test_redelivered_duplicates_are_not_order_violations():
    # bridge crash: 1,2,3 written, 2,3 redelivered, then 4
    r = analyze_raw([Msg(E1, c) for c in (1, 2, 3, 2, 3, 4)])
    assert (r["unique"], r["duplicates"], r["order_violations"], r["raw_regressions"]) == (4, 2, 0, 1)


def test_new_cycle_out_of_order_is_a_violation():
    r = analyze_raw([Msg(E1, c) for c in (1, 3, 2)])
    assert r["order_violations"] == 1


def test_split_partitions_detected():
    assert analyze_raw([Msg(E1, 1, 0), Msg(E1, 2, 3)])["engine_partitions"] == {E1: [0, 3]}


def test_key_and_topic_mismatch_invalid():
    r = analyze_raw([Msg(E1, 1, key=E2), Msg(E1, 2, topic_engine=E2)])
    assert len(r["invalid"]) == 2 and r["unique"] == 0
