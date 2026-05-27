"""Tests for Zstandard compression of daily candle cache files."""

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
import zstandard as zstd

import tradedesk_dukascopy.export as ex

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_candles(n: int = 5) -> pd.DataFrame:
    base = pd.Timestamp(datetime(2025, 1, 15, 0, 0, 0, tzinfo=UTC))
    idx = pd.date_range(base, periods=n, freq="1min")
    return pd.DataFrame(
        {
            "open": [1.1000 + i * 0.0001 for i in range(n)],
            "high": [1.1010 + i * 0.0001 for i in range(n)],
            "low": [1.0990 + i * 0.0001 for i in range(n)],
            "close": [1.1005 + i * 0.0001 for i in range(n)],
            "volume": [float(i + 1) for i in range(n)],
        },
        index=idx,
    )


# ---------------------------------------------------------------------------
# _daily_candle_path
# ---------------------------------------------------------------------------


def test_daily_candle_path_uses_zst_extension_bid(tmp_path: Path) -> None:
    day = date(2025, 6, 15)
    path = ex._daily_candle_path(tmp_path, "EURUSD", day, "bid")
    assert path.suffix == ".zst"
    assert path.stem.endswith(".csv")
    assert path.name == "15_bid.csv.zst"


def test_daily_candle_path_uses_zst_extension_ask(tmp_path: Path) -> None:
    day = date(2025, 6, 15)
    path = ex._daily_candle_path(tmp_path, "EURUSD", day, "ask")
    assert path.name == "15_ask.csv.zst"


# ---------------------------------------------------------------------------
# _write_daily_candles / _load_daily_candles round-trip
# ---------------------------------------------------------------------------


def test_write_and_load_round_trip(tmp_path: Path) -> None:
    candles = _make_candles(5)
    day = date(2025, 1, 15)
    path = ex._daily_candle_path(tmp_path, "EURUSD", day, "bid")

    ex._write_daily_candles(candles, path)

    assert path.exists()
    assert not path.with_suffix("").exists(), "uncompressed .csv must not exist"

    # File must be valid zstd
    dctx = zstd.ZstdDecompressor()
    raw = dctx.decompress(path.read_bytes())
    assert b"open" in raw  # CSV header present

    loaded = ex._load_daily_candles(path)
    assert loaded is not None
    assert len(loaded) == len(candles)

    for col in ("open", "high", "low", "close", "volume"):
        for orig, got in zip(candles[col], loaded[col], strict=True):
            assert orig == pytest.approx(got)


def test_load_returns_none_for_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "EURUSD" / "2025" / "00" / "15_bid.csv.zst"
    assert ex._load_daily_candles(path) is None


def test_load_returns_none_for_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv.zst"
    path.write_bytes(b"this is not zstd data")
    assert ex._load_daily_candles(path) is None


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


def test_cleanup_removes_redundant_bi5_dir_when_candle_csvs_exist(tmp_path: Path) -> None:
    # RAD-3015: a non-empty bi5 day-dir is removed when both daily-candle CSVs
    # for that day exist (the .bi5 are redundant once candles are written).
    symbol = "EURUSD"
    leftover_dir = tmp_path / symbol / "2025" / "00" / "15"
    leftover_dir.mkdir(parents=True)
    (leftover_dir / "00h_ticks.bi5").write_bytes(b"data")
    (leftover_dir / "01h_ticks.bi5").write_bytes(b"data")

    # Candle CSVs are siblings in the month dir: {DD}_bid.csv.zst / {DD}_ask.csv.zst.
    month_dir = tmp_path / symbol / "2025" / "00"
    (month_dir / "15_bid.csv.zst").write_bytes(b"candles")
    (month_dir / "15_ask.csv.zst").write_bytes(b"candles")

    ex._cleanup_empty_day_dirs(tmp_path, symbol)

    assert not leftover_dir.exists(), "redundant bi5 day-dir must be removed once candles exist"
    # The candle CSVs themselves must be left untouched.
    assert (month_dir / "15_bid.csv.zst").read_bytes() == b"candles"
    assert (month_dir / "15_ask.csv.zst").read_bytes() == b"candles"


def test_cleanup_preserves_bi5_dir_when_only_one_candle_csv_exists(tmp_path: Path) -> None:
    # RAD-3015: a partial candle pair (only bid written) is NOT a complete day,
    # so the bi5 must be kept for a retry to finish committing it.
    symbol = "EURUSD"
    leftover_dir = tmp_path / symbol / "2025" / "00" / "15"
    leftover_dir.mkdir(parents=True)
    (leftover_dir / "00h_ticks.bi5").write_bytes(b"data")

    month_dir = tmp_path / symbol / "2025" / "00"
    (month_dir / "15_bid.csv.zst").write_bytes(b"candles")  # ask missing

    ex._cleanup_empty_day_dirs(tmp_path, symbol)

    assert leftover_dir.exists(), "bi5 day-dir must be kept when the candle pair is incomplete"


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
