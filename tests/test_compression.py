"""Tests for Zstandard compression of daily tick cache files."""

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
import zstandard as zstd

import tradedesk_dukascopy.export as ex

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ticks(n: int = 3) -> list[ex.Tick]:
    base = datetime(2025, 1, 15, 0, 0, 0, tzinfo=UTC)
    from datetime import timedelta

    return [
        ex.Tick(
            ts=base + timedelta(seconds=i),
            bid=1.1000 + i * 0.0001,
            ask=1.1001 + i * 0.0001,
            bid_vol=float(i + 1),
            ask_vol=float(i + 2),
        )
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# _daily_tick_path
# ---------------------------------------------------------------------------


def test_daily_tick_path_uses_zst_extension(tmp_path: Path) -> None:
    day = date(2025, 6, 15)
    path = ex._daily_tick_path(tmp_path, "EURUSD", day)
    assert path.suffix == ".zst"
    assert path.stem.endswith(".csv")
    assert path.name == "15_ticks.csv.zst"


# ---------------------------------------------------------------------------
# _write_daily_ticks / _load_daily_ticks round-trip
# ---------------------------------------------------------------------------


def test_write_and_load_round_trip(tmp_path: Path) -> None:
    ticks = _make_ticks(5)
    day = date(2025, 1, 15)
    path = ex._daily_tick_path(tmp_path, "EURUSD", day)

    ex._write_daily_ticks(ticks, path)

    assert path.exists()
    assert not path.with_suffix("").exists(), "uncompressed .csv must not exist"

    # File must be valid zstd
    dctx = zstd.ZstdDecompressor()
    raw = dctx.decompress(path.read_bytes())
    assert b"bid" in raw  # CSV header present

    loaded = ex._load_daily_ticks(path)
    assert loaded is not None
    assert len(loaded) == len(ticks)

    for orig, got in zip(ticks, loaded, strict=True):
        assert got.ts == orig.ts
        assert got.bid == pytest.approx(orig.bid)
        assert got.ask == pytest.approx(orig.ask)
        assert got.bid_vol == pytest.approx(orig.bid_vol)
        assert got.ask_vol == pytest.approx(orig.ask_vol)


def test_load_returns_none_for_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "EURUSD" / "2025" / "00" / "15_ticks.csv.zst"
    assert ex._load_daily_ticks(path) is None


def test_load_returns_none_for_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv.zst"
    path.write_bytes(b"this is not zstd data")
    assert ex._load_daily_ticks(path) is None


# ---------------------------------------------------------------------------
# _migrate_to_compressed
# ---------------------------------------------------------------------------


def test_migrate_compresses_csv_and_removes_original(tmp_path: Path) -> None:
    csv_path = tmp_path / "15_ticks.csv"
    zst_path = tmp_path / "15_ticks.csv.zst"

    # Write a plain CSV
    ticks = _make_ticks(4)
    df = pd.DataFrame(
        {
            "ts": [t.ts.isoformat() for t in ticks],
            "bid": [t.bid for t in ticks],
            "ask": [t.ask for t in ticks],
            "bid_vol": [t.bid_vol for t in ticks],
            "ask_vol": [t.ask_vol for t in ticks],
        }
    )
    df.to_csv(csv_path, index=False)

    result = ex._migrate_to_compressed(csv_path, zst_path)

    assert result is True
    assert zst_path.exists()
    assert not csv_path.exists()

    # Verify contents survive the migration
    loaded = ex._load_daily_ticks(zst_path)
    assert loaded is not None
    assert len(loaded) == 4
    for orig, got in zip(ticks, loaded, strict=True):
        assert got.bid == pytest.approx(orig.bid)


def test_migrate_returns_false_on_unreadable_source(tmp_path: Path) -> None:
    csv_path = tmp_path / "nonexistent.csv"
    zst_path = tmp_path / "nonexistent.csv.zst"

    result = ex._migrate_to_compressed(csv_path, zst_path)

    assert result is False
    assert not zst_path.exists()


def test_migrate_cleans_up_tmp_on_failure(tmp_path: Path) -> None:
    """Verify no .tmp file is left behind after a failed migration."""
    csv_path = tmp_path / "15_ticks.csv"
    zst_path = tmp_path / "15_ticks.csv.zst"
    # source does not exist → migration fails
    ex._migrate_to_compressed(csv_path, zst_path)
    tmp_path_glob = list(tmp_path.glob("*.tmp"))
    assert tmp_path_glob == []


# ---------------------------------------------------------------------------
# _cleanup_empty_day_dirs
# ---------------------------------------------------------------------------


def test_cleanup_removes_empty_day_dirs(tmp_path: Path) -> None:
    symbol = "EURUSD"
    # Create: one empty day dir, one non-empty day dir, one day dir with a file
    empty_dir = tmp_path / symbol / "2025" / "00" / "14"
    empty_dir.mkdir(parents=True)

    non_empty_dir = tmp_path / symbol / "2025" / "00" / "15"
    non_empty_dir.mkdir(parents=True)
    (non_empty_dir / "00h_ticks.bi5").write_bytes(b"data")

    ex._cleanup_empty_day_dirs(tmp_path, symbol)

    assert not empty_dir.exists()
    assert non_empty_dir.exists()


def test_cleanup_is_noop_for_missing_symbol_dir(tmp_path: Path) -> None:
    # Should not raise even if symbol dir doesn't exist
    ex._cleanup_empty_day_dirs(tmp_path, "GBPUSD")


def test_cleanup_handles_multiple_months(tmp_path: Path) -> None:
    symbol = "USDJPY"
    empty1 = tmp_path / symbol / "2024" / "11" / "31"
    empty2 = tmp_path / symbol / "2025" / "00" / "01"
    empty1.mkdir(parents=True)
    empty2.mkdir(parents=True)

    ex._cleanup_empty_day_dirs(tmp_path, symbol)

    assert not empty1.exists()
    assert not empty2.exists()
