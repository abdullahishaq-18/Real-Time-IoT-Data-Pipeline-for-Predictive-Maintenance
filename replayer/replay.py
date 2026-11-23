"""Replay NASA C-MAPSS turbofan data into MQTT as if the engines were live machines.

Each engine (C-MAPSS "unit") publishes one telemetry message per operating cycle to
``factory/engines/{engine_id}/telemetry`` at QoS 1. Engines start at seeded random
offsets and are interleaved on a shared tick timeline (``tick = cycle + offset``).

Pacing:
  * event_time advances by ``--cycle-seconds`` per tick, independent of ``--speed``;
  * wall-clock time per tick is ``cycle_seconds / speed``, measured against a single
    monotonic start so sleep error never accumulates.

Everything except ``run()`` / ``main()`` is a pure function and testable without a broker.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator

import numpy as np
import pandas as pd

SCHEMA_VERSION = 1
N_SENSORS = 21
SENSOR_KEYS = [f"s{i}" for i in range(1, N_SENSORS + 1)]
COLUMNS = ["unit", "cycle", "op1", "op2", "op3", *SENSOR_KEYS]
OP_COLUMNS = ["op1", "op2", "op3"]
PAYLOAD_KEYS = (
    "engine_id", "cycle", "event_time", "op_settings", "sensors", "vibration", "schema_version",
)
TOPIC_TEMPLATE = "factory/engines/{engine_id}/telemetry"
PROGRESS_EVERY_TICKS = 25

# SYNTHETIC vibration channel -- C-MAPSS has no vibration sensor. This is a made-up
# signal, NOT measured data: a baseline that rises cubically towards end of life,
#   vibration = VIB_BASE + VIB_GAIN * (cycle / engine_max_cycle) ** 3 + N(0, VIB_NOISE_SD)
# It exists only so downstream stages have a vibration-like feature to work with.
VIB_BASE = 2.0
VIB_GAIN = 3.0
VIB_NOISE_SD = 0.15

DEFAULT_FILE = "data/raw/train_FD001.txt"
# Engines 1-80 are reserved for model training; the replayer defaults to the hold-out set.
DEFAULT_UNITS = "81-100"

log = logging.getLogger("replay")


# --------------------------------------------------------------------------- data


@dataclass(frozen=True)
class Event:
    """One scheduled telemetry reading. All fields are native Python types."""

    tick: int
    unit: int
    cycle: int
    op_settings: tuple[float, float, float]
    sensors: tuple[float, ...]  # s1..s21, in order
    vibration: float


def load_cmapss(path: str | Path) -> pd.DataFrame:
    """Load a C-MAPSS file: whitespace-separated, trailing spaces, no header, 26 columns."""
    df = pd.read_csv(path, sep=r"\s+", header=None)
    if df.shape[1] != len(COLUMNS):
        raise ValueError(f"{path}: expected {len(COLUMNS)} columns, found {df.shape[1]}")
    df.columns = COLUMNS
    df["unit"] = df["unit"].astype(int)
    df["cycle"] = df["cycle"].astype(int)
    return df


def parse_units(spec: str) -> list[int]:
    """Parse "81-100", "85" or "81-85,90,95-100" into a sorted list of unit numbers."""
    units: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            if "-" in part:
                lo_s, hi_s = part.split("-", 1)
                lo, hi = int(lo_s), int(hi_s)
            else:
                lo = hi = int(part)
        except ValueError:
            raise ValueError(f"invalid unit range {part!r} (expected e.g. 81-100)") from None
        if lo < 1 or hi < lo:
            raise ValueError(f"invalid unit range {part!r}")
        units.update(range(lo, hi + 1))
    if not units:
        raise ValueError(f"no units in {spec!r}")
    return sorted(units)


def filter_units(df: pd.DataFrame, units: Iterable[int]) -> pd.DataFrame:
    return df[df["unit"].isin(list(units))].reset_index(drop=True)


# --------------------------------------------------------------------------- formatting


def format_engine_id(unit: int) -> str:
    return f"engine-{unit:03d}"


def topic_for(engine_id: str) -> str:
    return TOPIC_TEMPLATE.format(engine_id=engine_id)


def format_event_time(ts: datetime) -> str:
    """ISO-8601 UTC with millisecond precision, e.g. 2026-11-24T00:20:41.005Z."""
    ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def build_payload(event: Event, event_time: datetime) -> dict:
    """Build the downstream contract payload (schema_version 1). Keep in sync with README."""
    return {
        "engine_id": format_engine_id(event.unit),
        "cycle": int(event.cycle),
        "event_time": format_event_time(event_time),
        "op_settings": [float(x) for x in event.op_settings],
        "sensors": {k: float(v) for k, v in zip(SENSOR_KEYS, event.sensors, strict=True)},
        "vibration": float(event.vibration),
        "schema_version": SCHEMA_VERSION,
    }


# --------------------------------------------------------------------------- scheduling


def compute_offsets(units: Iterable[int], max_offset: int, rng: np.random.Generator) -> dict[int, int]:
    """Seeded start stagger in cycles, uniform in [0, max_offset], drawn in sorted unit order."""
    if max_offset < 0:
        raise ValueError("max_offset must be >= 0")
    return {u: int(rng.integers(0, max_offset + 1)) for u in sorted(units)}


def synthetic_vibration(cycles: np.ndarray, max_cycle: int, rng: np.random.Generator) -> np.ndarray:
    """SYNTHETIC vibration (see VIB_* constants): base + gain * life_fraction^3 + noise."""
    life = np.asarray(cycles, dtype=float) / float(max_cycle)
    return VIB_BASE + VIB_GAIN * life**3 + rng.normal(0.0, VIB_NOISE_SD, size=life.shape)


def build_schedule(df: pd.DataFrame, max_offset: int, seed: int) -> list[Event]:
    """Turn rows into Events on a shared tick timeline, sorted by (tick, unit).

    One RNG drives everything: offsets first, then per-engine vibration noise in sorted
    unit order, so the same (data, max_offset, seed) always yields the same schedule.
    Within an engine tick = cycle + offset, so cycle order is preserved.
    """
    rng = np.random.default_rng(seed)
    units = sorted(int(u) for u in df["unit"].unique())
    offsets = compute_offsets(units, max_offset, rng)

    events: list[Event] = []
    for unit in units:
        g = df[df["unit"] == unit].sort_values("cycle")
        cycles = g["cycle"].to_numpy()
        vib = synthetic_vibration(cycles, int(cycles.max()), rng)
        ops = g[OP_COLUMNS].to_numpy(dtype=float).tolist()      # .tolist() -> native floats
        sens = g[SENSOR_KEYS].to_numpy(dtype=float).tolist()
        for cyc, op, s, v in zip(cycles.tolist(), ops, sens, vib.tolist(), strict=True):
            events.append(Event(
                tick=cyc + offsets[unit], unit=unit, cycle=cyc,
                op_settings=tuple(op), sensors=tuple(s), vibration=round(v, 4),
            ))
    events.sort(key=lambda e: (e.tick, e.unit))
    return events


def event_time_for(tick: int, first_tick: int, base_time: datetime, cycle_seconds: float) -> datetime:
    """Simulated time of a tick. Depends only on cycle_seconds -- never on speed."""
    return base_time + timedelta(seconds=(tick - first_tick) * cycle_seconds)


def due_seconds(tick: int, first_tick: int, cycle_seconds: float, speed: float) -> float:
    """Wall-clock seconds after the monotonic start at which a tick should be sent."""
    return (tick - first_tick) * cycle_seconds / speed


def plan(
    events: list[Event], base_time: datetime, cycle_seconds: float, speed: float,
) -> Iterator[tuple[float, Event, str, dict]]:
    """Yield (due_seconds, event, topic, payload) for each event in schedule order."""
    if cycle_seconds <= 0 or speed <= 0:
        raise ValueError("cycle_seconds and speed must be > 0")
    if not events:
        return
    first = events[0].tick
    for ev in events:
        payload = build_payload(ev, event_time_for(ev.tick, first, base_time, cycle_seconds))
        yield due_seconds(ev.tick, first, cycle_seconds, speed), ev, topic_for(payload["engine_id"]), payload


def engine_spans(events: list[Event]) -> dict[int, tuple[int, int]]:
    """Per-unit (first_tick, last_tick)."""
    spans: dict[int, tuple[int, int]] = {}
    for ev in events:
        lo, hi = spans.get(ev.unit, (ev.tick, ev.tick))
        spans[ev.unit] = (min(lo, ev.tick), max(hi, ev.tick))
    return spans


def active_engines(spans: dict[int, tuple[int, int]], tick: int) -> int:
    return sum(1 for lo, hi in spans.values() if lo <= tick <= hi)


# --------------------------------------------------------------------------- MQTT


class BrokerError(RuntimeError):
    pass


class Publisher:
    """Thin paho-mqtt (>= 2.0, CallbackAPIVersion.VERSION2) wrapper."""

    def __init__(self, host: str, port: int, client_id: str, timeout: float = 10.0):
        import paho.mqtt.client as mqtt

        self._mqtt = mqtt
        self.host, self.port, self.timeout = host, port, timeout
        self._connected = threading.Event()
        self._connack_reason = None
        self._last_info = None
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self._connack_reason = reason_code
        self._connected.set()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            log.warning("disconnected from broker: %s (paho will try to reconnect)", reason_code)

    def connect(self) -> None:
        try:
            self.client.connect(self.host, self.port, keepalive=30)
        except (OSError, ValueError) as e:  # refused, DNS failure, timeout, bad port
            raise BrokerError(f"cannot connect to MQTT broker at {self.host}:{self.port}: {e}") from None
        self.client.loop_start()
        if not self._connected.wait(self.timeout):
            self.client.loop_stop()
            raise BrokerError(f"no CONNACK from {self.host}:{self.port} within {self.timeout:.0f}s")
        if self._connack_reason.is_failure:
            self.client.loop_stop()
            raise BrokerError(f"broker at {self.host}:{self.port} refused connection: {self._connack_reason}")

    def publish(self, topic: str, payload: dict) -> None:
        info = self.client.publish(topic, json.dumps(payload), qos=1)
        if info.rc != self._mqtt.MQTT_ERR_SUCCESS and info.rc != self._mqtt.MQTT_ERR_NO_CONN:
            raise BrokerError(f"publish to {topic} failed: {self._mqtt.error_string(info.rc)}")
        self._last_info = info

    def flush_and_close(self, timeout: float = 30.0) -> bool:
        """Wait for the last QoS 1 PUBACK, then disconnect. Returns False on timeout."""
        acked = True
        if self._last_info is not None:
            try:
                self._last_info.wait_for_publish(timeout)
                acked = self._last_info.is_published()
            except (RuntimeError, ValueError) as e:
                log.warning("could not confirm final publish: %s", e)
                acked = False
        self.client.disconnect()
        self.client.loop_stop()
        return acked


# --------------------------------------------------------------------------- runner


def run(args: argparse.Namespace) -> int:
    units = parse_units(args.units)
    df = filter_units(load_cmapss(args.file), units)
    if df.empty:
        log.error("no rows for units %s in %s", args.units, args.file)
        return 2
    events = build_schedule(df, args.max_offset, args.seed)
    spans = engine_spans(events)
    log.info("loaded %d rows for %d engines from %s (ticks %d..%d)",
             len(events), len(spans), args.file, events[0].tick, events[-1].tick)

    pub = None
    if not args.dry_run:
        pub = Publisher(args.host, args.port, client_id=f"cmapss-replayer-{args.seed}")
        pub.connect()
        log.info("connected to %s:%d", args.host, args.port)

    sent = 0
    tick = events[0].tick
    next_progress = tick + PROGRESS_EVERY_TICKS
    interrupted = False
    start = time.monotonic()
    try:
        for due, ev, topic, payload in plan(events, datetime.now(timezone.utc),
                                            args.cycle_seconds, args.speed):
            if args.limit is not None and sent >= args.limit:
                break
            tick = ev.tick
            if tick >= next_progress:
                log.info("tick=%d sent=%d active_engines=%d", tick, sent, active_engines(spans, tick))
                next_progress = tick + PROGRESS_EVERY_TICKS
            if pub is None:
                print(json.dumps(payload), flush=True)  # dry run: no pacing, stdout only
            else:
                delay = start + due - time.monotonic()  # absolute target -> no drift
                if delay > 0:
                    time.sleep(delay)
                pub.publish(topic, payload)
            sent += 1
    except KeyboardInterrupt:
        interrupted = True
        log.warning("interrupted, stopping")
    finally:
        acked = pub.flush_and_close() if pub is not None else True

    elapsed = time.monotonic() - start
    log.info("summary: sent=%d engines=%d last_tick=%d elapsed=%.1fs%s%s",
             sent, len(spans), tick, elapsed,
             " (interrupted)" if interrupted else "",
             "" if acked else " WARNING: final publish not acknowledged")
    if interrupted:
        return 130
    return 0 if acked else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Replay C-MAPSS engine data to MQTT as live telemetry.")
    p.add_argument("--file", default=DEFAULT_FILE, help=f"C-MAPSS file (default {DEFAULT_FILE})")
    p.add_argument("--units", default=DEFAULT_UNITS,
                   help=f"engine units, e.g. 81-100 or 81-85,90 (default {DEFAULT_UNITS}; 1-80 are training engines)")
    p.add_argument("--cycle-seconds", type=float, default=1.0, help="simulated seconds per cycle (default 1.0)")
    p.add_argument("--speed", type=float, default=1.0, help="replay speed multiplier (default 1.0)")
    p.add_argument("--max-offset", type=int, default=50, help="max random start stagger in cycles (default 50)")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dry-run", action="store_true", help="print payloads to stdout instead of publishing (unpaced)")
    p.add_argument("--limit", type=int, default=None, help="stop after N messages")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(message)s")
    if args.cycle_seconds <= 0 or args.speed <= 0:
        log.error("--cycle-seconds and --speed must be > 0")
        return 2
    if args.limit is not None and args.limit < 0:
        log.error("--limit must be >= 0")
        return 2
    try:
        return run(args)
    except (BrokerError, ValueError, FileNotFoundError) as e:
        log.error("%s", e)
        return 2


if __name__ == "__main__":
    sys.exit(main())
