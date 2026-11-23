"""Broker-free tests for replayer/replay.py."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from replayer import replay as r

REAL_FILE = Path(__file__).resolve().parents[1] / "data" / "raw" / "train_FD001.txt"
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_df(units=range(1, 101), seed=0) -> pd.DataFrame:
    """Synthetic C-MAPSS-shaped frame: each unit runs cycles 1..L, L in [128, 362]."""
    rng = np.random.default_rng(seed)
    rows = []
    for u in units:
        life = int(rng.integers(128, 363))
        for c in range(1, life + 1):
            rows.append([u, c, *rng.normal(size=3), *rng.normal(500, 50, size=21)])
    df = pd.DataFrame(rows, columns=r.COLUMNS)
    df["unit"] = df["unit"].astype(int)
    df["cycle"] = df["cycle"].astype(int)
    return df


@pytest.fixture(scope="module")
def df():
    return make_df()


@pytest.fixture(scope="module")
def holdout(df):
    return r.filter_units(df, r.parse_units("81-100"))


@pytest.fixture(scope="module")
def schedule(holdout):
    return r.build_schedule(holdout, max_offset=50, seed=42)


# ------------------------------------------------------------------ payload contract


def test_payload_contract(schedule):
    _, ev, _, p = next(r.plan(schedule, BASE, 1.0, 1.0))
    assert list(p) == list(r.PAYLOAD_KEYS)
    assert set(p) == {"engine_id", "cycle", "event_time", "op_settings", "sensors",
                      "vibration", "schema_version"}
    assert type(p["engine_id"]) is str
    assert type(p["cycle"]) is int
    assert type(p["event_time"]) is str
    assert type(p["op_settings"]) is list and len(p["op_settings"]) == 3
    assert all(type(x) is float for x in p["op_settings"])
    assert list(p["sensors"]) == [f"s{i}" for i in range(1, 22)]
    assert all(type(v) is float for v in p["sensors"].values())
    assert type(p["vibration"]) is float
    assert p["schema_version"] == 1 and type(p["schema_version"]) is int
    assert json.loads(json.dumps(p)) == p


def test_all_payloads_native_types(schedule):
    # json.dumps raises TypeError on numpy scalars; check every message, not just one.
    for _, _, _, p in r.plan(schedule, BASE, 1.0, 1.0):
        json.dumps(p)


def test_integer_sensor_columns_become_floats():
    df = make_df(units=[81])
    df["s17"] = 392  # C-MAPSS s17/s18 are integer-valued -> int64 column
    ev = r.build_schedule(df, 0, 1)[0]
    p = r.build_payload(ev, BASE)
    assert type(p["sensors"]["s17"]) is float and p["sensors"]["s17"] == 392.0


def test_event_time_format():
    ts = datetime(2026, 11, 24, 0, 20, 41, 5436, tzinfo=timezone.utc)
    assert r.format_event_time(ts) == "2026-11-24T00:20:41.005Z"
    parsed = datetime.fromisoformat(r.format_event_time(ts))
    assert parsed.utcoffset() == timedelta(0)


# ------------------------------------------------------------------ ids / topics


@pytest.mark.parametrize("unit,expected", [(1, "engine-001"), (81, "engine-081"), (100, "engine-100")])
def test_engine_id_format(unit, expected):
    assert r.format_engine_id(unit) == expected


def test_topic_format(schedule):
    assert r.topic_for("engine-081") == "factory/engines/engine-081/telemetry"
    for _, _, topic, p in r.plan(schedule, BASE, 1.0, 1.0):
        assert topic == f"factory/engines/{p['engine_id']}/telemetry"


# ------------------------------------------------------------------ unit filtering


def test_unit_range_filter(df, holdout):
    assert sorted(holdout["unit"].unique().tolist()) == list(range(81, 101))
    assert len(holdout) == int(df["unit"].between(81, 100).sum())


def test_default_units_are_holdout():
    assert r.build_parser().parse_args([]).units == "81-100"
    assert r.parse_units(r.DEFAULT_UNITS) == list(range(81, 101))


@pytest.mark.parametrize("spec,expected", [("85", [85]), ("81-83,90", [81, 82, 83, 90])])
def test_parse_units_forms(spec, expected):
    assert r.parse_units(spec) == expected


@pytest.mark.parametrize("spec", ["", "abc", "100-81", "0-5", "81-"])
def test_parse_units_rejects_bad(spec):
    with pytest.raises(ValueError):
        r.parse_units(spec)


# ------------------------------------------------------------------ determinism


def test_same_seed_identical(holdout):
    a = r.build_schedule(holdout, 50, seed=42)
    b = r.build_schedule(holdout, 50, seed=42)
    assert a == b
    assert r.compute_offsets(range(81, 101), 50, np.random.default_rng(42)) == \
        r.compute_offsets(range(81, 101), 50, np.random.default_rng(42))


def test_different_seed_differs(holdout):
    a = r.build_schedule(holdout, 50, seed=42)
    b = r.build_schedule(holdout, 50, seed=43)
    off_a = r.compute_offsets(range(81, 101), 50, np.random.default_rng(42))
    off_b = r.compute_offsets(range(81, 101), 50, np.random.default_rng(43))
    assert off_a != off_b
    vib_a = {(e.unit, e.cycle): e.vibration for e in a}
    vib_b = {(e.unit, e.cycle): e.vibration for e in b}
    assert vib_a.keys() == vib_b.keys() and vib_a != vib_b


def test_offsets_within_bounds():
    offs = r.compute_offsets(range(81, 101), 50, np.random.default_rng(42))
    assert all(0 <= o <= 50 for o in offs.values())
    assert r.compute_offsets([1, 2], 0, np.random.default_rng(1)) == {1: 0, 2: 0}


# ------------------------------------------------------------------ ordering


def test_ticks_non_decreasing(schedule):
    ticks = [e.tick for e in schedule]
    assert ticks == sorted(ticks)


def test_engines_interleaved(schedule):
    # Not simply one engine after another: the first 100 events span multiple engines.
    assert len({e.unit for e in schedule[:100]}) > 1


def test_per_engine_cycles_strictly_increasing_and_complete(schedule, holdout):
    last: dict[int, int] = {}
    for e in schedule:
        assert e.cycle > last.get(e.unit, 0)
        last[e.unit] = e.cycle
    assert len(schedule) == len(holdout)
    assert last == holdout.groupby("unit")["cycle"].max().to_dict()


# ------------------------------------------------------------------ pacing


@pytest.mark.parametrize("cycle_seconds", [0.5, 1.0, 10.0])
def test_event_time_step_equals_cycle_seconds_independent_of_speed(schedule, cycle_seconds):
    def tick_times(speed):
        out = {}
        for _, ev, _, p in r.plan(schedule, BASE, cycle_seconds, speed):
            out[ev.tick] = datetime.fromisoformat(p["event_time"])
        return out

    slow, fast = tick_times(1.0), tick_times(100.0)
    assert slow == fast
    ticks = sorted(slow)
    for a, b in zip(ticks, ticks[1:]):
        assert (slow[b] - slow[a]).total_seconds() == pytest.approx((b - a) * cycle_seconds)
    # Consecutive ticks -> exactly one cycle_seconds apart.
    assert any(b - a == 1 for a, b in zip(ticks, ticks[1:]))


def test_same_tick_same_event_time(schedule):
    by_tick: dict[int, set[str]] = {}
    for _, ev, _, p in r.plan(schedule, BASE, 1.0, 1.0):
        by_tick.setdefault(ev.tick, set()).add(p["event_time"])
    assert all(len(v) == 1 for v in by_tick.values())


def test_wall_clock_due_scales_with_speed(schedule):
    d1 = [d for d, *_ in r.plan(schedule, BASE, 1.0, 1.0)]
    d50 = [d for d, *_ in r.plan(schedule, BASE, 1.0, 50.0)]
    assert d1[0] == 0.0
    assert d50 == pytest.approx([d / 50 for d in d1])
    assert r.due_seconds(tick=26, first_tick=1, cycle_seconds=2.0, speed=10.0) == pytest.approx(5.0)


def test_plan_rejects_bad_pacing(schedule):
    with pytest.raises(ValueError):
        list(r.plan(schedule, BASE, 1.0, 0.0))


def test_active_engines(schedule):
    spans = r.engine_spans(schedule)
    assert r.active_engines(spans, schedule[0].tick) >= 1
    assert r.active_engines(spans, schedule[-1].tick + 1) == 0


# ------------------------------------------------------------------ synthetic vibration


def _check_vibration_profile(events):
    by_unit: dict[int, list] = {}
    for e in events:
        by_unit.setdefault(e.unit, []).append(e)
    for unit, evs in by_unit.items():
        evs.sort(key=lambda e: e.cycle)
        n = len(evs)
        early = [e.vibration for e in evs[: max(1, int(n * 0.20))]]
        late = [e.vibration for e in evs[n - max(1, int(n * 0.05)):]]
        assert np.mean(early) < 2.3, f"unit {unit} early mean {np.mean(early):.3f}"
        assert np.mean(late) > 4.0, f"unit {unit} late mean {np.mean(late):.3f}"


def test_vibration_profile_synthetic(schedule):
    _check_vibration_profile(schedule)


@pytest.mark.skipif(not REAL_FILE.exists(), reason="C-MAPSS data not present")
def test_vibration_profile_real_fd001_holdout():
    df = r.filter_units(r.load_cmapss(REAL_FILE), r.parse_units("81-100"))
    _check_vibration_profile(r.build_schedule(df, 50, 42))


@pytest.mark.skipif(not REAL_FILE.exists(), reason="C-MAPSS data not present")
def test_real_file_loads():
    df = r.load_cmapss(REAL_FILE)
    assert df.shape == (20631, 26)
    assert df["unit"].nunique() == 100
    assert not df.isna().any().any()
