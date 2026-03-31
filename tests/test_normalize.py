"""Tests for tradedesk_dukascopy.normalize."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from tradedesk_dukascopy.normalize import (
    _expected_price_range,
    _read_zst,
    _write_zst,
    infer_price_divisor,
    normalize_cache,
    normalize_symbol,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_daily_candle_df(close_price: float, n: int = 5) -> pd.DataFrame:
    """Create a minimal 1-min candle DataFrame at a fixed price."""
    base = pd.Timestamp("2026-03-10T00:00:00", tz="UTC")
    idx = pd.date_range(base, periods=n, freq="1min")
    return pd.DataFrame(
        {
            "open": [close_price] * n,
            "high": [close_price + 0.1] * n,
            "low": [close_price - 0.1] * n,
            "close": [close_price] * n,
            "volume": [100.0] * n,
        },
        index=idx,
    )


def _write_candle_zst(path: Path, close_price: float, n: int = 5) -> None:
    """Write a minimal candle CSV.zst at *path*."""
    df = _make_daily_candle_df(close_price, n)
    _write_zst(df, path)


def _read_median_close(path: Path) -> float:
    df = _read_zst(path)
    assert df is not None
    return float(df["close"].median())


def _make_cache_day(
    tmp_path: Path,
    symbol: str,
    year: int,
    month_0: int,
    day: int,
    bid_price: float,
    ask_price: float,
) -> tuple[Path, Path]:
    """Write bid+ask CSV.zst files for one day in the standard cache layout."""
    day_dir = tmp_path / symbol / str(year) / f"{month_0:02d}"
    day_dir.mkdir(parents=True, exist_ok=True)
    bid_path = day_dir / f"{day:02d}_bid.csv.zst"
    ask_path = day_dir / f"{day:02d}_ask.csv.zst"
    _write_candle_zst(bid_path, bid_price)
    _write_candle_zst(ask_path, ask_price)
    return bid_path, ask_path


# ---------------------------------------------------------------------------
# _expected_price_range
# ---------------------------------------------------------------------------


def test_expected_price_range_jpy_cross() -> None:
    lo, hi = _expected_price_range("USDJPY")
    assert lo == 50.0
    assert hi == 500.0


def test_expected_price_range_jpy_cross_lowercase() -> None:
    lo, hi = _expected_price_range("audjpy")
    assert lo == 50.0
    assert hi == 500.0


def test_expected_price_range_gold() -> None:
    lo, hi = _expected_price_range("XAUUSD")
    assert lo > 0 and hi > lo


def test_expected_price_range_index() -> None:
    lo, hi = _expected_price_range("USA500IDXUSD")
    assert hi > 10_000


def test_expected_price_range_fx4() -> None:
    lo, hi = _expected_price_range("EURUSD")
    # Standard FX pair: price well under 15
    assert hi < 20


def test_expected_price_range_crude_oil() -> None:
    lo, hi = _expected_price_range("BRENTCMDUSD")
    assert lo <= 20.0
    assert hi >= 150.0


def test_expected_price_range_high_rate_fx() -> None:
    lo, hi = _expected_price_range("EURSEK")
    # EURSEK trades around 11 — must be above the standard FX upper of 15
    assert lo >= 5.0
    assert hi >= 15.0


def test_infer_divisor_crude_oil_already_correct() -> None:
    lo, hi = _expected_price_range("BRENTCMDUSD")
    # Brent at ~$76 should be left unchanged
    assert infer_price_divisor(76.0, lo, hi) == 1.0


def test_infer_divisor_crude_oil_100x_too_large() -> None:
    lo, hi = _expected_price_range("BRENTCMDUSD")
    # Raw Dukascopy int ~7600 (= $76 × 100) → should divide by 100
    assert infer_price_divisor(7_600.0, lo, hi) == 100.0


def test_infer_divisor_high_rate_fx_already_correct() -> None:
    lo, hi = _expected_price_range("EURSEK")
    # EURSEK at 11.03 should be left unchanged
    assert infer_price_divisor(11.03, lo, hi) == 1.0


def test_infer_divisor_high_rate_fx_10000x_too_large() -> None:
    lo, hi = _expected_price_range("EURSEK")
    # Raw Dukascopy int 110314 (= 11.03 × 10000) → should divide by 10000
    assert infer_price_divisor(110_314.0, lo, hi) == 10_000.0


# ---------------------------------------------------------------------------
# infer_price_divisor
# ---------------------------------------------------------------------------


def test_infer_divisor_already_in_range_returns_1() -> None:
    lo, hi = _expected_price_range("EURUSD")
    assert infer_price_divisor(1.10, lo, hi) == 1.0


def test_infer_divisor_100x_too_large_returns_100() -> None:
    lo, hi = _expected_price_range("EURUSD")
    # EURUSD at 1.10 → 100× → 110
    assert infer_price_divisor(110.0, lo, hi) == 100.0


def test_infer_divisor_10000x_too_large_returns_10000() -> None:
    lo, hi = _expected_price_range("EURUSD")
    # EURUSD at 1.10 → 10 000× → 11000
    assert infer_price_divisor(11_000.0, lo, hi) == 10_000.0


def test_infer_divisor_jpy_100x_too_large() -> None:
    lo, hi = _expected_price_range("USDJPY")
    # USDJPY at 155 → 100× → 15 500
    assert infer_price_divisor(15_500.0, lo, hi) == 100.0


def test_infer_divisor_jpy_already_correct() -> None:
    lo, hi = _expected_price_range("USDJPY")
    assert infer_price_divisor(155.0, lo, hi) == 1.0


def test_infer_divisor_gold_100x_too_large() -> None:
    lo, hi = _expected_price_range("XAUUSD")
    # Gold at ~$5 363 → stored as 536 300 (×100); ÷1000 must NOT be chosen
    # (536.3 is outside the gold range) — ÷100 must win.
    assert infer_price_divisor(536_300.0, lo, hi) == 100.0


def test_infer_divisor_gold_already_correct() -> None:
    lo, hi = _expected_price_range("XAUUSD")
    assert infer_price_divisor(2_641.0, lo, hi) == 1.0


def test_infer_divisor_returns_1_when_no_candidate_works() -> None:
    # Price 0.00001 — nothing plausible
    lo, hi = _expected_price_range("EURUSD")
    assert infer_price_divisor(0.00001, lo, hi) == 1.0


# ---------------------------------------------------------------------------
# _read_zst / _write_zst round-trip
# ---------------------------------------------------------------------------


def test_read_write_zst_round_trip(tmp_path: Path) -> None:
    df = _make_daily_candle_df(1.2345)
    path = tmp_path / "test.csv.zst"
    _write_zst(df, path)
    loaded = _read_zst(path)
    assert loaded is not None
    assert len(loaded) == len(df)
    assert abs(loaded["close"].iloc[0] - 1.2345) < 1e-6


def test_read_zst_returns_none_for_missing_file(tmp_path: Path) -> None:
    assert _read_zst(tmp_path / "nonexistent.csv.zst") is None


# ---------------------------------------------------------------------------
# normalize_symbol
# ---------------------------------------------------------------------------


def test_normalize_symbol_corrects_100x_error(tmp_path: Path) -> None:
    """4-decimal FX daily CSV at 100× real price should be divided by 100."""
    symbol = "EURUSD"
    # EURUSD real price ≈ 1.10 → stored as 110.0 (100×)
    bid_path, ask_path = _make_cache_day(tmp_path, symbol, 2026, 2, 10, 110.0, 110.5)

    result = normalize_symbol(tmp_path / symbol, symbol)

    assert result["fixed"] == 1
    assert result["errors"] == 0
    assert abs(_read_median_close(bid_path) - 110.0 / 100) < 0.01
    assert abs(_read_median_close(ask_path) - 110.5 / 100) < 0.01


def test_normalize_symbol_corrects_10000x_error(tmp_path: Path) -> None:
    """4-decimal FX daily CSV at 10 000× real price should be divided by 10 000."""
    symbol = "AUDNZD"
    # AUDNZD real price ≈ 1.20 → stored as 12 000
    bid_path, ask_path = _make_cache_day(tmp_path, symbol, 2026, 2, 5, 12_000.0, 12_010.0)

    result = normalize_symbol(tmp_path / symbol, symbol)

    assert result["fixed"] == 1
    assert abs(_read_median_close(bid_path) - 12_000 / 10_000) < 0.01


def test_normalize_symbol_leaves_correct_price_unchanged(tmp_path: Path) -> None:
    """A correctly priced daily CSV must not be modified."""
    symbol = "GBPUSD"
    # GBPUSD real price ≈ 1.27 — already correct
    bid_path, ask_path = _make_cache_day(tmp_path, symbol, 2026, 2, 10, 1.27, 1.271)

    result = normalize_symbol(tmp_path / symbol, symbol)

    assert result["fixed"] == 0
    assert abs(_read_median_close(bid_path) - 1.27) < 1e-4


def test_normalize_symbol_jpy_corrects_100x(tmp_path: Path) -> None:
    """JPY cross at 100× real price should be divided by 100."""
    symbol = "USDJPY"
    # USDJPY real price ≈ 155 → stored as 15 500
    bid_path, _ask_path = _make_cache_day(tmp_path, symbol, 2026, 2, 5, 15_500.0, 15_520.0)

    result = normalize_symbol(tmp_path / symbol, symbol)

    assert result["fixed"] == 1
    assert abs(_read_median_close(bid_path) - 155.0) < 0.5


def test_normalize_symbol_jpy_leaves_correct_unchanged(tmp_path: Path) -> None:
    symbol = "AUDJPY"
    bid_path, _ask_path = _make_cache_day(tmp_path, symbol, 2026, 2, 12, 113.5, 113.6)

    result = normalize_symbol(tmp_path / symbol, symbol)

    assert result["fixed"] == 0
    assert abs(_read_median_close(bid_path) - 113.5) < 0.01


def test_normalize_symbol_dry_run_does_not_modify_files(tmp_path: Path) -> None:
    """Dry-run must not write any files."""
    symbol = "EURUSD"
    bid_path, _ask_path = _make_cache_day(tmp_path, symbol, 2026, 2, 10, 110.0, 110.5)

    result = normalize_symbol(tmp_path / symbol, symbol, dry_run=True)

    assert result["fixed"] == 1
    # File must still contain the original wrong price
    assert abs(_read_median_close(bid_path) - 110.0) < 0.01


def test_normalize_symbol_handles_empty_month_dir(tmp_path: Path) -> None:
    symbol = "EURUSD"
    sym_dir = tmp_path / symbol / "2026" / "02"
    sym_dir.mkdir(parents=True)
    # No .csv.zst files

    result = normalize_symbol(tmp_path / symbol, symbol)

    assert result["fixed"] == 0
    assert result["errors"] == 0


def test_normalize_symbol_skips_corrupted_file(tmp_path: Path) -> None:
    symbol = "EURUSD"
    day_dir = tmp_path / symbol / "2026" / "02"
    day_dir.mkdir(parents=True)
    bad_path = day_dir / "10_bid.csv.zst"
    bad_path.write_bytes(b"not a valid zstd file")

    result = normalize_symbol(tmp_path / symbol, symbol)

    assert result["skipped"] == 1
    assert result["errors"] == 0


def test_normalize_symbol_multiple_days(tmp_path: Path) -> None:
    """Multiple days in the same month — all wrong ones get corrected."""
    symbol = "USDCAD"
    # Day 5 wrong: 13600 (10000×) → should become 1.36
    # Day 6 wrong: 13610 (10000×) → should become 1.361
    # Day 12 correct: 1.36 → should stay
    _make_cache_day(tmp_path, symbol, 2026, 2, 5, 13_600.0, 13_620.0)
    _make_cache_day(tmp_path, symbol, 2026, 2, 6, 13_610.0, 13_630.0)
    bid_ok, _ = _make_cache_day(tmp_path, symbol, 2026, 2, 12, 1.36, 1.362)

    result = normalize_symbol(tmp_path / symbol, symbol)

    assert result["fixed"] == 2
    assert abs(_read_median_close(bid_ok) - 1.36) < 1e-4


# ---------------------------------------------------------------------------
# normalize_cache
# ---------------------------------------------------------------------------


def test_normalize_cache_processes_multiple_symbols(tmp_path: Path) -> None:
    _make_cache_day(tmp_path, "EURUSD", 2026, 2, 10, 110.0, 110.5)
    _make_cache_day(tmp_path, "GBPUSD", 2026, 2, 10, 135.0, 135.5)

    results = normalize_cache(tmp_path)

    assert results["EURUSD"]["fixed"] == 1
    assert results["GBPUSD"]["fixed"] == 1


def test_normalize_cache_symbol_filter(tmp_path: Path) -> None:
    _make_cache_day(tmp_path, "EURUSD", 2026, 2, 10, 110.0, 110.5)
    _make_cache_day(tmp_path, "GBPUSD", 2026, 2, 10, 135.0, 135.5)

    results = normalize_cache(tmp_path, ["EURUSD"])

    assert "EURUSD" in results
    assert "GBPUSD" not in results


def test_normalize_cache_warns_missing_symbol(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.WARNING):
        results = normalize_cache(tmp_path, ["DOESNOTEXIST"])

    assert "DOESNOTEXIST" not in results
    assert any("not found" in r.message.lower() for r in caplog.records)


def test_normalize_cache_dry_run(tmp_path: Path) -> None:
    bid_path, _ = _make_cache_day(tmp_path, "EURUSD", 2026, 2, 10, 110.0, 110.5)

    results = normalize_cache(tmp_path, dry_run=True)

    assert results["EURUSD"]["fixed"] == 1
    # Original wrong price must be untouched
    assert abs(_read_median_close(bid_path) - 110.0) < 0.01
