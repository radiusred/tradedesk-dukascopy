"""Tests for daily tick CSV caching helpers."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import tradedesk_dukascopy.export as ex


def _sample_ticks(day: date, count: int = 60) -> list[ex.Tick]:
    """Build sample ticks spread across the first hour of `day`."""
    base = datetime(day.year, day.month, day.day, 0, 0, tzinfo=UTC)
    return [
        ex.Tick(
            ts=base + timedelta(seconds=i),
            bid=1.10 + i * 0.0001,
            ask=1.10 + i * 0.0001 + 0.0002,
            bid_vol=100.0,
            ask_vol=200.0,
        )
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# _daily_tick_path
# ---------------------------------------------------------------------------


def test_daily_tick_path_structure(tmp_path: Path) -> None:
    day = date(2025, 3, 15)
    p = ex._daily_tick_path(tmp_path, "EURUSD", day)
    # Month is zero-based: March (3) → "02"
    assert p == tmp_path / "EURUSD" / "2025" / "02" / "15_ticks.csv.zst"


def test_daily_tick_path_no_price_side(tmp_path: Path) -> None:
    """Tick path has no price side — a single file holds all sides."""
    day = date(2025, 1, 1)
    p = ex._daily_tick_path(tmp_path, "EURUSD", day)
    assert p.name == "01_ticks.csv.zst"


# ---------------------------------------------------------------------------
# _write_daily_ticks / _load_daily_ticks round-trip
# ---------------------------------------------------------------------------


def test_write_and_load_round_trip(tmp_path: Path) -> None:
    day = date(2025, 6, 1)
    ticks = _sample_ticks(day, count=10)
    path = ex._daily_tick_path(tmp_path, "EURUSD", day)

    ex._write_daily_ticks(ticks, path)
    assert path.exists()

    loaded = ex._load_daily_ticks(path)
    assert loaded is not None
    assert len(loaded) == len(ticks)
    for orig, ld in zip(loaded, ticks, strict=True):
        assert orig.ts == ld.ts
        assert orig.bid == pytest.approx(ld.bid)
        assert orig.ask == pytest.approx(ld.ask)
        assert orig.bid_vol == pytest.approx(ld.bid_vol)
        assert orig.ask_vol == pytest.approx(ld.ask_vol)


def test_write_creates_parent_dirs(tmp_path: Path) -> None:
    day = date(2025, 6, 1)
    nested = tmp_path / "deep" / "nested"
    path = ex._daily_tick_path(nested, "GBPUSD", day)
    ticks = _sample_ticks(day, count=5)

    ex._write_daily_ticks(ticks, path)
    assert path.exists()


def test_write_is_atomic(tmp_path: Path) -> None:
    """No .tmp file should remain after a successful write."""
    day = date(2025, 6, 1)
    path = ex._daily_tick_path(tmp_path, "EURUSD", day)
    ex._write_daily_ticks(_sample_ticks(day), path)

    tmp_files = list(tmp_path.rglob("*.tmp"))
    assert tmp_files == []


def test_load_returns_none_for_missing_file(tmp_path: Path) -> None:
    p = tmp_path / "nonexistent.csv"
    assert ex._load_daily_ticks(p) is None


def test_load_returns_none_for_corrupt_file(tmp_path: Path) -> None:
    p = tmp_path / "bad.csv"
    p.write_text("not,valid,csv\nbad data here")
    assert ex._load_daily_ticks(p) is None


def test_load_handles_mixed_fractional_timestamps(tmp_path: Path) -> None:
    """Some ticks land on exact seconds (no fractional part), others have ms."""
    day = date(2025, 6, 1)
    base = datetime(2025, 6, 1, 0, 0, tzinfo=UTC)
    ticks = [
        ex.Tick(ts=base, bid=1.0, ask=1.1, bid_vol=10.0, ask_vol=20.0),  # no fractional
        ex.Tick(
            ts=base + timedelta(milliseconds=500), bid=1.0, ask=1.1, bid_vol=10.0, ask_vol=20.0
        ),
    ]
    path = ex._daily_tick_path(tmp_path, "TEST", day)
    ex._write_daily_ticks(ticks, path)
    loaded = ex._load_daily_ticks(path)
    assert loaded is not None
    assert len(loaded) == 2
    assert loaded[0].ts == ticks[0].ts
    assert loaded[1].ts == ticks[1].ts


# ---------------------------------------------------------------------------
# tick CSV → candle aggregation round-trip
# ---------------------------------------------------------------------------


def test_ticks_roundtrip_through_csv(tmp_path: Path) -> None:
    """Write ticks to CSV, reload, convert to candles — values must be stable."""
    day = date(2025, 1, 5)
    ticks = _sample_ticks(day, count=120)  # 2 minutes of second-resolution ticks

    path = ex._daily_tick_path(tmp_path, "EURUSD", day)
    ex._write_daily_ticks(ticks, path)
    loaded = ex._load_daily_ticks(path)

    assert loaded is not None
    candles = ex._ticks_to_candles(loaded, resample_rule="1min", price_side="bid")
    assert len(candles) == 2  # 2 one-minute candles
    assert candles.iloc[0]["open"] == pytest.approx(ticks[0].bid)
    assert candles.iloc[0]["volume"] == pytest.approx(100.0 * 60)  # 60 ticks summed
