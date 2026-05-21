#!/usr/bin/env python3
"""RAD-2146 — delete corrupt FX cache day-files and re-fetch them from
Dukascopy with the correct ``price_divisor`` for each symbol.

This is a generalisation of ``refetch_nzdusd_corrupt_days.py`` (RAD-2132) across
the full Phase-4 FX universe. The RAD-2142 audit found systemic 10 000× / 100×
/ 100 000× scale corruption on every FX symbol except ``NZDUSD`` (which RAD-2132
already remediated).

Pattern
-------
For each :class:`FxScaleConfig`:

1. Audit the symbol cache (``<cache_dir>/<SYMBOL>``) and collect the set of
   day-files whose median close falls outside ``[envelope_min, envelope_max]``.
2. For each corrupt date:
   - Delete the bid/ask daily candle CSVs and any leftover ``DD/`` bi5
     directory so :func:`export_range` re-downloads from Dukascopy.
   - Call :func:`export_range` for the single day with the configured
     ``price_divisor``.
3. The RAD-1920 write-time scale-discontinuity sentry remains active during
   re-fetch (it cannot catch a *uniform* misscale across the whole history,
   which is exactly the failure mode RAD-2146 documents — but it will catch
   any new drift introduced during remediation).

Scale config
------------
The price_divisor required to convert Dukascopy raw int32 ticks back to true
quote prices depends on the symbol's tick format:

- 4-decimal FX (EUR/USD, GBP/USD, AUD/USD, AUD/NZD, EUR/GBP, USD/CAD, etc.) —
  divisor 100_000, expected median close envelope ``[0.30, 2.00]``.
- JPY pairs (USD/JPY, AUD/JPY, GBP/JPY, CHF/JPY) — divisor 1_000, envelope
  ``[20, 250]``.
- EURSEK (rate ~10) — divisor 100_000, envelope ``[5, 20]``.
- EURSGD (rate ~1.4) — divisor 100_000, envelope ``[1.0, 2.5]``.

NZDUSD is intentionally absent — it was remediated by RAD-2132 (PR #53).

Re-runs are safe: ``_delete_day`` no-ops on already-removed files and
``export_range`` overwrites the same cache paths. The script can be killed
and restarted; only dates still failing the audit will be re-touched.
"""
from __future__ import annotations

import argparse
import io
import logging
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import zstandard as zstd

# Add the package src tree to path so we don't depend on `uv pip install -e .`
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tradedesk_dukascopy.export import export_range  # noqa: E402

CACHE_DIR = Path("/paperclip/tradedesk/marketdata")


@dataclass(frozen=True)
class FxScaleConfig:
    """Per-symbol cache audit + re-fetch parameters."""

    symbol: str
    price_divisor: float
    envelope_min: float
    envelope_max: float


# Source of truth for the RAD-2146 remediation scope. Keep alphabetised within
# each scale class so reviewers can see the universe at a glance.
FX_CONFIG: list[FxScaleConfig] = [
    # 4-decimal FX (envelope [0.30, 2.00], divisor 100_000)
    FxScaleConfig("AUDCAD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("AUDNZD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("AUDUSD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("EURCAD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("EURCHF", 100_000.0, 0.30, 2.00),
    FxScaleConfig("EURGBP", 100_000.0, 0.30, 2.00),
    FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("GBPAUD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("GBPCHF", 100_000.0, 0.30, 2.00),
    FxScaleConfig("GBPUSD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("NZDCAD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("USDCAD", 100_000.0, 0.30, 2.00),
    FxScaleConfig("USDCHF", 100_000.0, 0.30, 2.00),
    # JPY pairs (envelope [20, 250], divisor 1_000)
    FxScaleConfig("AUDJPY", 1_000.0, 20.0, 250.0),
    FxScaleConfig("CHFJPY", 1_000.0, 20.0, 250.0),
    FxScaleConfig("GBPJPY", 1_000.0, 20.0, 250.0),
    FxScaleConfig("USDJPY", 1_000.0, 20.0, 250.0),
    # Exotic pairs (different price levels, same int32 scale)
    FxScaleConfig("EURSEK", 100_000.0, 5.0, 20.0),
    FxScaleConfig("EURSGD", 100_000.0, 1.0, 2.5),
]


def _read_median_close(path: Path) -> float | None:
    """Return median close for a daily candle CSV.zst, or None on read failure."""
    try:
        dctx = zstd.ZstdDecompressor()
        with open(path, "rb") as f_in, dctx.stream_reader(f_in) as reader:
            df = pd.read_csv(io.TextIOWrapper(io.BufferedReader(reader), encoding="utf-8"))
    except (OSError, ValueError, zstd.ZstdError, pd.errors.ParserError, UnicodeDecodeError):
        return None
    if df is None or df.empty or "close" not in df.columns:
        return None
    med = float(df["close"].median())
    if pd.isna(med):
        return None
    return med


def _iter_day_files(sym_dir: Path, side: str) -> Iterable[Path]:
    """Yield daily candle files for *side* under a symbol directory.

    Dukascopy month numbering on disk is 0-indexed (e.g. ``2024/00/`` = January).
    """
    if not sym_dir.is_dir():
        return
    for year_dir in sorted(sym_dir.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue
            yield from sorted(month_dir.glob(f"*_{side}.csv.zst"))


def _parse_day(path: Path) -> date:
    """Parse a YYYY/MM0/DD_{side}.csv.zst cache path back into a date."""
    year = int(path.parts[-3])
    month0 = int(path.parts[-2])
    day = int(path.name.split("_")[0])
    return date(year, month0 + 1, day)


def find_corrupt_dates(cfg: FxScaleConfig, cache_dir: Path) -> list[date]:
    """Return the sorted list of corrupt dates for *cfg* in *cache_dir*.

    A date is *corrupt* when the bid OR ask median close lies outside
    ``[envelope_min, envelope_max]``. Unreadable / empty files are skipped
    (matching the audit script's ``err`` bucket — those are weekends/holidays,
    not scale corruption).
    """
    sym_dir = cache_dir / cfg.symbol
    corrupt: set[date] = set()
    for side in ("bid", "ask"):
        for path in _iter_day_files(sym_dir, side):
            med = _read_median_close(path)
            if med is None:
                continue
            if med < cfg.envelope_min or med > cfg.envelope_max:
                corrupt.add(_parse_day(path))
    return sorted(corrupt)


def _candle_path(cache_dir: Path, symbol: str, d: date, side: str) -> Path:
    return cache_dir / symbol / f"{d.year}" / f"{d.month - 1:02d}" / f"{d.day:02d}_{side}.csv.zst"


def _bi5_day_dir(cache_dir: Path, symbol: str, d: date) -> Path:
    return cache_dir / symbol / f"{d.year}" / f"{d.month - 1:02d}" / f"{d.day:02d}"


def _delete_day(cache_dir: Path, symbol: str, d: date) -> int:
    """Delete the corrupt cache files for *d*. Returns number of paths removed."""
    n = 0
    for side in ("bid", "ask"):
        p = _candle_path(cache_dir, symbol, d, side)
        if p.exists():
            p.unlink()
            n += 1
    bi5_dir = _bi5_day_dir(cache_dir, symbol, d)
    if bi5_dir.is_dir():
        for child in bi5_dir.iterdir():
            try:
                child.unlink()
                n += 1
            except OSError:
                pass
        try:
            bi5_dir.rmdir()
        except OSError:
            pass
    return n


def _refetch_day(cache_dir: Path, cfg: FxScaleConfig, d: date) -> bool:
    """Re-fetch a single day. Returns True if bid+ask cache files exist after."""
    start = datetime(d.year, d.month, d.day, tzinfo=UTC)
    end = start
    try:
        export_range(
            symbol=cfg.symbol,
            start_utc=start,
            end_utc_inclusive=end,
            out=Path("/tmp/fx-refetch-out"),  # unused (resample_rule=None)
            price_divisor=cfg.price_divisor,
            resample_rule=None,
            cache_dir=cache_dir,
        )
    except RuntimeError as exc:
        # export_range raises "No data produced ..." when resample_rule is None
        # AND no 1-min frames accumulate. With cache_dir set the daily candle
        # is still written via _flush_day, so this path can be ignored for
        # days with valid data — re-check existence below.
        logging.debug("export_range non-fatal RuntimeError for %s %s: %s", cfg.symbol, d, exc)
    bid_ok = _candle_path(cache_dir, cfg.symbol, d, "bid").exists()
    ask_ok = _candle_path(cache_dir, cfg.symbol, d, "ask").exists()
    return bid_ok and ask_ok


def remediate_symbol(
    cfg: FxScaleConfig, cache_dir: Path, log: logging.Logger
) -> tuple[int, list[date]]:
    """Audit + re-fetch one symbol. Returns (ok_count, failures)."""
    corrupt = find_corrupt_dates(cfg, cache_dir)
    if not corrupt:
        log.info("%s: no corrupt dates — skipping", cfg.symbol)
        return 0, []

    log.info(
        "%s: %d corrupt dates (envelope=[%.2f, %.2f], divisor=%g)",
        cfg.symbol, len(corrupt), cfg.envelope_min, cfg.envelope_max, cfg.price_divisor,
    )

    ok = 0
    failures: list[date] = []
    for i, d in enumerate(corrupt, 1):
        n_removed = _delete_day(cache_dir, cfg.symbol, d)
        if _refetch_day(cache_dir, cfg, d):
            ok += 1
            if i % 50 == 0 or i == len(corrupt):
                log.info("%s [%d/%d] %s ok (removed %d, %d failures)",
                         cfg.symbol, i, len(corrupt), d.isoformat(), n_removed, len(failures))
        else:
            log.error("%s [%d/%d] %s — REFETCH FAILED", cfg.symbol, i, len(corrupt), d.isoformat())
            failures.append(d)
    log.info("%s: done — %d/%d ok, %d failures", cfg.symbol, ok, len(corrupt), len(failures))
    return ok, failures


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="RAD-2146 FX cache scale remediation.")
    ap.add_argument(
        "--cache-dir",
        type=Path,
        default=CACHE_DIR,
        help=f"Cache root (default: {CACHE_DIR}).",
    )
    ap.add_argument(
        "--symbols",
        nargs="+",
        help="Optional subset of symbols (default: every FX_CONFIG entry).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Audit only; do not delete or re-fetch.",
    )
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("refetch-fx")

    configs = FX_CONFIG
    if args.symbols:
        wanted = {s.upper() for s in args.symbols}
        configs = [c for c in FX_CONFIG if c.symbol in wanted]
        missing = wanted - {c.symbol for c in configs}
        if missing:
            log.error("unknown symbols: %s", sorted(missing))
            return 2

    grand_failures: dict[str, list[date]] = {}
    for cfg in configs:
        if args.dry_run:
            corrupt = find_corrupt_dates(cfg, args.cache_dir)
            log.info("%s: %d corrupt dates (dry-run)", cfg.symbol, len(corrupt))
            continue
        _, failures = remediate_symbol(cfg, args.cache_dir, log)
        if failures:
            grand_failures[cfg.symbol] = failures

    if grand_failures:
        log.error("remediation finished with failures:")
        for sym, dates in grand_failures.items():
            log.error("  %s (%d): %s", sym, len(dates), ", ".join(d.isoformat() for d in dates))
        return 1
    log.info("remediation finished cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
