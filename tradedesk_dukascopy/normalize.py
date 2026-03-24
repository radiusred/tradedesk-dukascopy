"""
Normalize Dukascopy cache daily candle files with incorrect price scaling.

Detects days where cached prices are scaled incorrectly (100× or 10000× too
large) based on instrument type, and divides by the correct factor in-place.

Background
----------
Dukascopy changed their bi5 tick encoding format around 2026-03-11.  Before
that date the format was int32 with a per-instrument point-factor divisor:

* 4-decimal FX (EURUSD, AUDNZD, GBPUSD …): point factor 10 000
* 2-decimal FX / JPY crosses (USDJPY, AUDJPY …): point factor 100
* 2-decimal commodities (XAUUSD …): point factor 100

After 2026-03-11 the format changed:

* 4-decimal FX: int32 with point factor 100 (until ~2026-03-20)
* 2-decimal FX / JPY / commodities: float32 native (correct as-is)

After ~2026-03-20 the format changed again to float32 native for all pairs.

When ``tradedesk-dc-export`` is run with the default ``--price-divisor 1.0``
the daily candle CSVs end up storing the raw integer values, not actual
prices.  This module detects and corrects those cached files without
re-downloading data from Dukascopy.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

import pandas as pd
import zstandard as zstd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Instrument classification
# ---------------------------------------------------------------------------

_JPY_CROSSES = frozenset(
    {
        "AUDJPY",
        "CADJPY",
        "CHFJPY",
        "EURJPY",
        "GBPJPY",
        "NZDJPY",
        "SGDJPY",
        "USDJPY",
    }
)
_PRECIOUS_METALS = frozenset({"XAUUSD", "XAGUSD"})
_IDX_SUBSTRINGS = ("IDX",)


def _expected_price_range(symbol: str) -> tuple[float, float]:
    """Return the (min, max) plausible mid-price range for *symbol*.

    These ranges are used to detect and correct price scale errors in cached
    daily candle files.  They are intentionally wide to avoid false positives.
    """
    upper = symbol.upper()
    if upper in _JPY_CROSSES:
        return (50.0, 500.0)
    if upper in _PRECIOUS_METALS:
        return (500.0, 50_000.0)
    if any(s in upper for s in _IDX_SUBSTRINGS):
        return (100.0, 500_000.0)
    # Standard 4-decimal FX pairs
    return (0.3, 15.0)


def infer_price_divisor(
    median_close: float,
    price_min: float,
    price_max: float,
) -> float:
    """Return the divisor that brings *median_close* into [price_min, price_max].

    Tries candidate divisors in order (largest first) and returns the first
    that puts the adjusted price inside the expected range.  Returns ``1.0``
    if the price is already in range, or if no candidate works.
    """
    if price_min <= median_close <= price_max:
        return 1.0
    for divisor in (100_000.0, 10_000.0, 1_000.0, 100.0, 10.0):
        if price_min <= (median_close / divisor) <= price_max:
            return divisor
    return 1.0  # cannot determine — leave unchanged


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _read_zst(path: Path) -> pd.DataFrame | None:
    """Read a Zstandard-compressed CSV into a DataFrame.  Returns None on error."""
    try:
        dctx = zstd.ZstdDecompressor()
        with open(path, "rb") as f_in:
            with dctx.stream_reader(f_in) as reader:
                df = pd.read_csv(
                    io.TextIOWrapper(io.BufferedReader(reader), encoding="utf-8")
                )
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.set_index("timestamp")
    except Exception:
        return None


def _write_zst(df: pd.DataFrame, path: Path) -> None:
    """Atomically write *df* as a Zstandard-compressed CSV to *path*."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    out = df.copy()
    out.index.name = "timestamp"
    cctx = zstd.ZstdCompressor(level=3)
    compressed = cctx.compress(out.reset_index().to_csv(index=False).encode("utf-8"))
    tmp.write_bytes(compressed)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Core normalization logic
# ---------------------------------------------------------------------------


def normalize_symbol(
    sym_dir: Path,
    symbol: str,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Normalize all daily candle files for *symbol* in *sym_dir*.

    Scans every ``*_bid.csv.zst`` file under *sym_dir*, checks whether the
    median close price is within the expected range, infers the required
    divisor if not, and rewrites both the bid and the corresponding ask file
    in-place.

    Args:
        sym_dir: Directory for this symbol (e.g. ``cache_dir / "AUDNZD"``).
        symbol: Instrument symbol string used for range lookup.
        dry_run: If ``True``, report what would change without writing files.

    Returns:
        Dict with keys ``"fixed"``, ``"skipped"``, ``"errors"``.
    """
    price_min, price_max = _expected_price_range(symbol)
    result: dict[str, int] = {"fixed": 0, "skipped": 0, "errors": 0}

    for year_dir in sorted(sym_dir.iterdir()):
        if not year_dir.is_dir():
            continue
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            for bid_path in sorted(month_dir.glob("*_bid.csv.zst")):
                ask_path = bid_path.parent / bid_path.name.replace(
                    "_bid.csv.zst", "_ask.csv.zst"
                )

                df_bid = _read_zst(bid_path)
                if df_bid is None or df_bid.empty:
                    result["skipped"] += 1
                    continue

                median = float(df_bid["close"].median())
                if pd.isna(median):
                    result["skipped"] += 1
                    continue

                divisor = infer_price_divisor(median, price_min, price_max)
                if divisor == 1.0:
                    continue  # already correct

                day_label = f"{year_dir.name}/{month_dir.name}/{bid_path.name[:2]}"
                log.info(
                    "%s %s: median close %.4f is %.0f× too large — %s by %.0f",
                    symbol,
                    day_label,
                    median,
                    divisor,
                    "would divide" if dry_run else "dividing",
                    divisor,
                )

                result["fixed"] += 1
                if dry_run:
                    continue

                # Apply divisor to bid OHLC (not volume)
                price_cols = ["open", "high", "low", "close"]
                for col in price_cols:
                    df_bid[col] = df_bid[col] / divisor
                try:
                    _write_zst(df_bid, bid_path)
                except Exception as exc:
                    log.error("Failed to write %s: %s", bid_path, exc)
                    result["errors"] += 1
                    continue

                # Apply same divisor to ask
                df_ask = _read_zst(ask_path)
                if df_ask is not None and not df_ask.empty:
                    for col in price_cols:
                        df_ask[col] = df_ask[col] / divisor
                    try:
                        _write_zst(df_ask, ask_path)
                    except Exception as exc:
                        log.error("Failed to write %s: %s", ask_path, exc)
                        result["errors"] += 1

    return result


def normalize_cache(
    cache_dir: Path,
    symbols: list[str] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, dict[str, int]]:
    """Normalize all symbols (or a specified subset) in *cache_dir*.

    Args:
        cache_dir: Root of the Dukascopy cache directory.
        symbols: Symbols to process; defaults to every subdirectory.
        dry_run: If ``True``, no files are modified.

    Returns:
        Dict mapping symbol name to per-symbol result dicts.
    """
    if symbols is None:
        symbols = sorted(d.name for d in cache_dir.iterdir() if d.is_dir())

    results: dict[str, dict[str, int]] = {}
    for symbol in sorted(symbols):
        sym_dir = cache_dir / symbol
        if not sym_dir.is_dir():
            log.warning("Symbol directory not found: %s", sym_dir)
            continue
        results[symbol] = normalize_symbol(sym_dir, symbol, dry_run=dry_run)

    return results
