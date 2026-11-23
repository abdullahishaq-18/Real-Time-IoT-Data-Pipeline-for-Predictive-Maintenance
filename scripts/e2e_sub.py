#!/usr/bin/env python3
"""Subscribe to replayer telemetry, validate every message against the schema_version 1
contract, and report. Exits 0 only if every requested expectation holds.

Start it BEFORE the replayer (QoS 1, but no persistent session, so messages sent
before it subscribes are not delivered). It stops after --idle seconds without a message.

  # terminal 1
  .venv/bin/python scripts/e2e_sub.py --expect-count 500 --min-engines 10
  # terminal 2
  .venv/bin/python replayer/replay.py --speed 50 --limit 500

  # full run: compare against the source file (row count + last cycle per engine)
  .venv/bin/python scripts/e2e_sub.py --compare-file data/raw/train_FD001.txt --units 81-100

The contract is deliberately hard-coded here, not imported from the replayer, so a
change to the replayer's payload shows up as a failure instead of moving the test with it.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt

KEYS = ["engine_id", "cycle", "event_time", "op_settings", "sensors", "vibration", "schema_version"]
SENSOR_KEYS = [f"s{i}" for i in range(1, 22)]
ENGINE_ID = re.compile(r"engine-\d{3}")
EVENT_TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z")


def check(topic: str, p: dict) -> None:
    """Raise AssertionError if the payload breaks the contract."""
    assert isinstance(p, dict), "payload is not a JSON object"
    assert list(p) == KEYS, f"keys {list(p)}"
    assert type(p["engine_id"]) is str and ENGINE_ID.fullmatch(p["engine_id"]), f"engine_id {p['engine_id']!r}"
    assert topic == f"factory/engines/{p['engine_id']}/telemetry", f"topic {topic!r} != engine_id"
    assert type(p["cycle"]) is int and p["cycle"] >= 1, f"cycle {p['cycle']!r}"
    assert type(p["event_time"]) is str and EVENT_TIME.fullmatch(p["event_time"]), f"event_time {p['event_time']!r}"
    datetime.fromisoformat(p["event_time"])
    ops = p["op_settings"]
    assert type(ops) is list and len(ops) == 3 and all(type(x) is float for x in ops), f"op_settings {ops!r}"
    s = p["sensors"]
    assert type(s) is dict and list(s) == SENSOR_KEYS, f"sensor keys {list(s) if isinstance(s, dict) else s!r}"
    assert all(type(v) is float for v in s.values()), "non-float sensor value"
    assert type(p["vibration"]) is float, f"vibration {p['vibration']!r}"
    assert type(p["schema_version"]) is int and p["schema_version"] == 1, f"schema_version {p['schema_version']!r}"


def expected_from_file(path: str, units_spec: str) -> tuple[int, dict[str, int]]:
    """(row count, {engine_id: max cycle}) for the given units in a C-MAPSS file."""
    import pandas as pd

    units: set[int] = set()
    for part in units_spec.split(","):
        lo, _, hi = part.strip().partition("-")
        units.update(range(int(lo), int(hi or lo) + 1))
    df = pd.read_csv(path, sep=r"\s+", header=None)
    sub = df[df[0].isin(units)]
    return len(sub), {f"engine-{int(u):03d}": int(c) for u, c in sub.groupby(0)[1].max().items()}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate replayer telemetry arriving over MQTT.")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--topic", default="factory/engines/+/telemetry")
    p.add_argument("--idle", type=float, default=5.0, help="stop after N seconds with no message (default 5)")
    p.add_argument("--first-timeout", type=float, default=120.0,
                   help="give up if no message arrives within N seconds of subscribing (default 120)")
    p.add_argument("--expect-count", type=int, help="fail unless exactly N messages arrive")
    p.add_argument("--min-engines", type=int, help="fail unless at least N distinct engine_ids are seen")
    p.add_argument("--compare-file", help="C-MAPSS file: fail unless the message count equals its row count "
                                          "for --units and each engine's last cycle equals its max cycle")
    p.add_argument("--units", default="81-100", help="units for --compare-file (default 81-100)")
    p.add_argument("--ready-file", help="write this file once subscribed (for scripts that start the replayer)")
    p.add_argument("--out", help="write the full result as JSON to this path")
    p.add_argument("--show-errors", type=int, default=10, help="print at most N invalid messages (default 10)")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    msgs: list[dict] = []
    errors: list[str] = []
    last = [time.monotonic()]
    subscribed, first_seen = threading.Event(), threading.Event()

    def on_connect(c, u, flags, rc, props):
        if rc.is_failure:
            print(f"ERROR broker refused connection: {rc}", file=sys.stderr)
            return
        c.subscribe(args.topic, qos=1)

    def on_subscribe(c, u, mid, rcs, props):
        if any(rc.is_failure for rc in rcs):
            print(f"ERROR subscribe to {args.topic} rejected: {rcs}", file=sys.stderr)
            return
        if args.ready_file:
            with open(args.ready_file, "w") as f:
                f.write("ready\n")
        print(f"subscribed to {args.topic} on {args.host}:{args.port}", file=sys.stderr, flush=True)
        subscribed.set()

    def on_message(c, u, m):
        last[0] = time.monotonic()
        first_seen.set()
        try:
            p = json.loads(m.payload)
            check(m.topic, p)
            msgs.append(p)
        except Exception as e:  # noqa: BLE001 -- any parse/contract failure is a reportable error
            errors.append(f"{m.topic}: {type(e).__name__}: {e}")

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"e2e-validator-{time.time_ns()}")
    c.on_connect, c.on_subscribe, c.on_message = on_connect, on_subscribe, on_message
    try:
        c.connect(args.host, args.port)
    except OSError as e:
        print(f"ERROR cannot connect to MQTT broker at {args.host}:{args.port}: {e}", file=sys.stderr)
        return 2
    c.loop_start()
    try:
        if not subscribed.wait(10):
            print("ERROR did not get a SUBACK within 10s", file=sys.stderr)
            return 2
        if first_seen.wait(args.first_timeout):
            while time.monotonic() - last[0] < args.idle:
                time.sleep(0.2)
        else:
            print(f"no messages within {args.first_timeout:.0f}s", file=sys.stderr)
    except KeyboardInterrupt:
        print("interrupted; reporting what was received", file=sys.stderr)
    finally:
        c.loop_stop()
        c.disconnect()

    # Per-engine ordering on the valid messages, in arrival order.
    last_cycle: dict[str, int] = {}
    order_violations = 0
    for p in msgs:
        prev = last_cycle.get(p["engine_id"])
        if prev is not None and p["cycle"] <= prev:
            order_violations += 1
        last_cycle[p["engine_id"]] = p["cycle"]

    received = len(msgs) + len(errors)
    failures: list[str] = []
    if errors:
        failures.append(f"{len(errors)} invalid messages")
    if order_violations:
        failures.append(f"{order_violations} per-engine cycle ordering violations")
    if received == 0:
        failures.append("no messages received")
    if args.expect_count is not None and received != args.expect_count:
        failures.append(f"received {received}, expected {args.expect_count}")
    if args.min_engines is not None and len(last_cycle) < args.min_engines:
        failures.append(f"{len(last_cycle)} distinct engines, expected >= {args.min_engines}")

    comparison = None
    if args.compare_file:
        rows, expected_last = expected_from_file(args.compare_file, args.units)
        mismatched = {k: {"received": last_cycle.get(k), "file_max": v}
                      for k, v in expected_last.items() if last_cycle.get(k) != v}
        extra = sorted(set(last_cycle) - set(expected_last))
        comparison = {"file_rows": rows, "mismatched_last_cycle": mismatched, "unexpected_engines": extra}
        if received != rows:
            failures.append(f"received {received}, file has {rows} rows for units {args.units}")
        if mismatched:
            failures.append(f"{len(mismatched)} engines whose last cycle != file max")
        if extra:
            failures.append(f"engines not in --units: {extra}")

    print(f"received={received} valid={len(msgs)} invalid={len(errors)} "
          f"distinct_engines={len(last_cycle)} per_engine_order_violations={order_violations}")
    if comparison is not None:
        print(f"file_rows={comparison['file_rows']} "
              f"last_cycle_mismatches={len(comparison['mismatched_last_cycle'])} "
              f"unexpected_engines={len(comparison['unexpected_engines'])}")
    for e in errors[: args.show_errors]:
        print(f"  INVALID {e}")
    for f in failures:
        print(f"FAIL: {f}")
    print("RESULT: " + ("FAIL" if failures else "PASS"))

    if args.out:
        with open(args.out, "w") as f:
            json.dump({"received": received, "valid": len(msgs), "errors": errors,
                       "distinct_engines": len(last_cycle), "order_violations": order_violations,
                       "last_cycle": last_cycle, "comparison": comparison, "failures": failures}, f, indent=2)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
