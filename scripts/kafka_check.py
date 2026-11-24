#!/usr/bin/env python3
"""Verify what the MQTT->Kafka bridge wrote to Kafka during one run.

Exit codes: 0 every check passed, 1 a check failed, 2 cannot connect to Kafka.

Usage (two steps, so data from earlier runs never affects the counts):

  # 1. before the run: record the current end offsets of the raw and DLQ topics
  .venv/bin/python scripts/kafka_check.py --save-snapshot /tmp/snap.json
  # 2. run the replayer ...
  # 3. after: read only [snapshot offset, current end) and check it
  .venv/bin/python scripts/kafka_check.py --snapshot /tmp/snap.json \\
      --compare-file data/raw/train_FD001.txt --units 81-100 --expect-dlq 0

Checks on sensor.telemetry.raw (records in the window):
  * every value honours the payload contract (same check() as scripts/e2e_sub.py, using
    the record's mqtt_topic header as the topic)
  * record key == payload engine_id
  * all records of one engine are in a single partition
  * per-engine order: in offset order, the FIRST occurrence of each (engine_id, cycle)
    never has a lower cycle than an earlier one. Re-sent duplicates after a bridge crash
    may step back to cycles already seen -- that is reported (raw_regressions) but is not
    a failure, because duplicates are allowed under at-least-once.
  * unique (engine_id, cycle) count and duplicate count
  * --expect-unique N, --compare-file/--units (exact set of (engine, cycle) == file rows),
    --expect-dlq N (records in the DLQ window), --max-duplicates N
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent))
from e2e_sub import check  # noqa: E402 -- the one hard-coded copy of the contract

RAW, DLQ = "sensor.telemetry.raw", "sensor.telemetry.dlq"


def parse_units(spec: str) -> set[int]:
    units: set[int] = set()
    for part in spec.split(","):
        lo, _, hi = part.strip().partition("-")
        units.update(range(int(lo), int(hi or lo) + 1))
    return units


def expected_pairs(path: str, units_spec: str) -> set[tuple[str, int]]:
    import pandas as pd

    df = pd.read_csv(path, sep=r"\s+", header=None)
    sub = df[df[0].isin(parse_units(units_spec))]
    return {(f"engine-{int(u):03d}", int(c)) for u, c in zip(sub[0], sub[1])}


def end_offsets(consumer: Consumer, topics: list[str], timeout: float = 10.0) -> dict[str, dict[int, int]]:
    md = consumer.list_topics(timeout=timeout)
    out: dict[str, dict[int, int]] = {}
    for t in topics:
        if t not in md.topics or md.topics[t].error is not None:
            raise KafkaException(f"topic {t} not found")
        out[t] = {}
        for p in sorted(md.topics[t].partitions):
            _, high = consumer.get_watermark_offsets(TopicPartition(t, p), timeout=timeout)
            out[t][p] = high
    return out


def wait_stable(consumer: Consumer, topics: list[str], quiet: float, max_wait: float):
    """Return end offsets once they haven't moved for `quiet` seconds (bridge drained)."""
    deadline = time.monotonic() + max_wait
    last = end_offsets(consumer, topics)
    since = time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(1.0)
        cur = end_offsets(consumer, topics)
        if cur != last:
            last, since = cur, time.monotonic()
        elif time.monotonic() - since >= quiet:
            return cur
    print(f"WARNING: offsets still moving after {max_wait:.0f}s; checking what is there", file=sys.stderr)
    return last


def read_range(consumer: Consumer, topic: str, start: dict[int, int], end: dict[int, int]):
    """Yield every message of `topic` with start[p] <= offset < end[p], per partition."""
    todo = {p: e for p, e in end.items() if e > start.get(p, 0)}
    if not todo:
        return
    consumer.assign([TopicPartition(topic, p, start.get(p, 0)) for p in todo])
    idle_deadline = time.monotonic() + 30
    while todo:
        msgs = consumer.consume(500, timeout=1.0)
        if not msgs and time.monotonic() > idle_deadline:
            raise RuntimeError(f"{topic}: timed out reading, still missing partitions {sorted(todo)}")
        for m in msgs:
            if m.error():
                raise KafkaException(m.error())
            idle_deadline = time.monotonic() + 30
            p = m.partition()
            if p not in todo or m.offset() >= todo[p]:
                continue
            yield m
            if m.offset() + 1 >= todo[p]:
                del todo[p]
    consumer.unassign()


def headers_of(m) -> dict[str, str]:
    return {k: (v.decode(errors="replace") if v is not None else None) for k, v in (m.headers() or [])}


def analyze_raw(messages) -> dict:
    """Pure-ish analysis of raw-topic records (iterable of objects with .key/.value/.partition/
    .offset/.headers like confluent_kafka.Message). Records arrive per partition in offset order."""
    invalid: list[str] = []
    partitions: dict[str, set[int]] = defaultdict(set)
    seen: Counter = Counter()
    max_first_cycle: dict[str, int] = {}
    order_violations = 0
    raw_regressions = 0
    last_cycle: dict[str, int] = {}
    total = 0
    for m in messages:
        total += 1
        h = headers_of(m)
        try:
            p = json.loads(m.value())
            check(h.get("mqtt_topic") or "<missing mqtt_topic header>", p)
            key = m.key().decode() if m.key() is not None else None
            assert key == p["engine_id"], f"key {key!r} != engine_id {p['engine_id']!r}"
            for hk in ("mqtt_topic", "bridge_ingest_time", "schema_version"):
                assert hk in h, f"missing header {hk}"
        except Exception as e:  # noqa: BLE001
            invalid.append(f"p{m.partition()}@{m.offset()}: {type(e).__name__}: {e}")
            continue
        eid, cyc = p["engine_id"], p["cycle"]
        partitions[eid].add(m.partition())
        if eid in last_cycle and cyc < last_cycle[eid]:
            raw_regressions += 1
        last_cycle[eid] = cyc
        if seen[(eid, cyc)] == 0:
            if eid in max_first_cycle and cyc < max_first_cycle[eid]:
                order_violations += 1
            max_first_cycle[eid] = max(cyc, max_first_cycle.get(eid, cyc))
        seen[(eid, cyc)] += 1
    return {
        "total": total,
        "invalid": invalid,
        "unique": len(seen),
        "duplicates": sum(n - 1 for n in seen.values()),
        "pairs": set(seen),
        "engine_partitions": {k: sorted(v) for k, v in sorted(partitions.items())},
        "order_violations": order_violations,
        "raw_regressions": raw_regressions,
    }


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="localhost:9094")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--save-snapshot", metavar="FILE", help="record current end offsets to FILE and exit")
    g.add_argument("--snapshot", metavar="FILE", help="check records written after this snapshot")
    p.add_argument("--quiet", type=float, default=5.0,
                   help="wait until end offsets are unchanged for N seconds before reading (default 5)")
    p.add_argument("--max-wait", type=float, default=300.0, help="give up waiting for quiet after N s")
    p.add_argument("--expect-unique", type=int, help="fail unless exactly N unique (engine_id, cycle)")
    p.add_argument("--compare-file", help="C-MAPSS file: unique (engine_id, cycle) set must equal its rows")
    p.add_argument("--units", default="81-100", help="units for --compare-file (default 81-100)")
    p.add_argument("--expect-dlq", type=int, help="fail unless exactly N DLQ records in the window")
    p.add_argument("--max-duplicates", type=int, help="fail if more than N duplicate records")
    p.add_argument("--show", type=int, default=10, help="print at most N invalid records / DLQ records")
    p.add_argument("--out", help="write a JSON summary here")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"kafka-check-{uuid.uuid4()}",  # never committed; assign() only
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": False,
    })
    try:
        try:
            consumer.list_topics(timeout=10)
        except KafkaException as e:
            print(f"ERROR cannot connect to Kafka at {args.bootstrap}: {e}", file=sys.stderr)
            return 2

        if args.save_snapshot:
            snap = end_offsets(consumer, [RAW, DLQ])
            Path(args.save_snapshot).write_text(json.dumps(snap, indent=2))
            print(f"snapshot saved to {args.save_snapshot}: " +
                  " ".join(f"{t}={sum(v.values())}" for t, v in snap.items()))
            return 0

        snap = {t: {int(p): o for p, o in v.items()}
                for t, v in json.loads(Path(args.snapshot).read_text()).items()}
        end = wait_stable(consumer, [RAW, DLQ], args.quiet, args.max_wait)
        r = analyze_raw(read_range(consumer, RAW, snap.get(RAW, {}), end[RAW]))
        dlq = [(headers_of(m), m.value()) for m in read_range(consumer, DLQ, snap.get(DLQ, {}), end[DLQ])]
    finally:
        consumer.close()

    failures: list[str] = []
    if r["invalid"]:
        failures.append(f"{len(r['invalid'])} raw records break the contract")
    split = {e: ps for e, ps in r["engine_partitions"].items() if len(ps) > 1}
    if split:
        failures.append(f"engines spread over several partitions: {split}")
    if r["order_violations"]:
        failures.append(f"{r['order_violations']} per-engine cycle ordering violations")
    if args.expect_unique is not None and r["unique"] != args.expect_unique:
        failures.append(f"unique={r['unique']}, expected {args.expect_unique}")
    if args.max_duplicates is not None and r["duplicates"] > args.max_duplicates:
        failures.append(f"duplicates={r['duplicates']} > {args.max_duplicates}")
    comparison = None
    if args.compare_file:
        exp = expected_pairs(args.compare_file, args.units)
        missing, extra = exp - r["pairs"], r["pairs"] - exp
        comparison = {"file_rows": len(exp), "missing": len(missing), "unexpected": len(extra)}
        if missing:
            failures.append(f"{len(missing)} (engine, cycle) from the file missing in Kafka, "
                            f"e.g. {sorted(missing)[:5]}")
        if extra:
            failures.append(f"{len(extra)} (engine, cycle) in Kafka not in the file, e.g. {sorted(extra)[:5]}")
    if args.expect_dlq is not None and len(dlq) != args.expect_dlq:
        failures.append(f"dlq={len(dlq)}, expected {args.expect_dlq}")

    by_partition: dict[int, list[str]] = defaultdict(list)
    for e, ps in r["engine_partitions"].items():
        by_partition[ps[0]].append(e)

    print(f"raw: records={r['total']} unique={r['unique']} duplicates={r['duplicates']} "
          f"invalid={len(r['invalid'])} engines={len(r['engine_partitions'])} "
          f"order_violations={r['order_violations']} raw_regressions={r['raw_regressions']}")
    if comparison:
        print(f"compare: file_rows={comparison['file_rows']} missing={comparison['missing']} "
              f"unexpected={comparison['unexpected']}")
    print("partition spread:")
    for p in sorted(end[RAW]):
        n = end[RAW][p] - snap.get(RAW, {}).get(p, 0)
        names = ",".join(e.removeprefix("engine-") for e in sorted(by_partition.get(p, [])))
        print(f"  p{p}: records={n:5d} engines={len(by_partition.get(p, [])):2d} [{names}]")
    print(f"dlq: records={len(dlq)}")
    for h, v in dlq[: args.show]:
        print(f"  DLQ topic={h.get('mqtt_topic')!r} error_reason={h.get('error_reason')!r} value={v[:60]!r}")
    for e in r["invalid"][: args.show]:
        print(f"  INVALID {e}")
    for f in failures:
        print(f"FAIL: {f}")
    print("RESULT: " + ("FAIL" if failures else "PASS"))
    if args.out:
        Path(args.out).write_text(json.dumps({
            **{k: v for k, v in r.items() if k != "pairs"}, "dlq": len(dlq),
            "dlq_reasons": [h.get("error_reason") for h, _ in dlq], "comparison": comparison,
            "failures": failures}, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
