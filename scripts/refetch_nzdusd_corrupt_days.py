#!/usr/bin/env python3
"""RAD-2132 — delete the 45 known-corrupt NZDUSD day-files and re-fetch fresh
from Dukascopy with the correct ``--price-divisor 100000``.

Why this script
---------------
NZDUSD ticks come from Dukascopy as int32 scaled by 100 000 (4-decimal FX).
On the 45 affected dates the cache was written with the exporter default
``--price-divisor 1.0``, so close values landed at ~57 000-72 000 instead of
~0.55-0.75.

For each date:

1. Delete the bid/ask ``.csv.zst`` candle files (and any leftover bi5
   directory) so ``export_range`` re-downloads from Dukascopy.
2. Call ``export_range`` with ``price_divisor=100000`` for the single day.
3. Write-time ``check_scale_consistency`` (RAD-1920) verifies the fresh
   median against neighbour days; a divergence here would re-raise the bug.

The script unconditionally deletes and re-fetches every date in
``CORRUPT_DATES``; it does not inspect a day's existing candles to decide
whether to skip it. Re-runs are safe because ``_delete_day`` no-ops on
already-removed files and ``export_range`` overwrites the same cache paths.
"""
from __future__ import annotations

import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

# Add the package src tree to path so we don't depend on `uv pip install -e .`
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tradedesk_dukascopy.export import export_range  # noqa: E402

CACHE_DIR = Path("/paperclip/tradedesk/marketdata")
SYMBOL = "NZDUSD"
PRICE_DIVISOR = 100_000.0

# Confirmed corrupt dates (RAD-2132 audit, envelope [0.30, 2.00]).
CORRUPT_DATES: list[date] = [
    date(2019, 6, 30), date(2019, 8, 4), date(2019, 12, 15),
    date(2020, 1, 9), date(2020, 1, 10), date(2020, 10, 28), date(2020, 11, 1),
    date(2021, 5, 2), date(2021, 5, 3), date(2021, 8, 18), date(2021, 8, 20),
    date(2022, 10, 26), date(2022, 10, 27), date(2022, 10, 28), date(2022, 11, 6),
    date(2023, 1, 2), date(2023, 1, 3), date(2023, 5, 29),
    date(2024, 1, 18), date(2024, 3, 7), date(2024, 3, 8),
    date(2024, 4, 18), date(2024, 4, 19), date(2024, 5, 5), date(2024, 5, 6),
    date(2024, 7, 3), date(2024, 8, 14), date(2024, 10, 29), date(2024, 11, 1),
    date(2025, 1, 29), date(2025, 1, 30), date(2025, 6, 30), date(2025, 7, 3),
    date(2026, 3, 12), date(2026, 3, 15),
    date(2026, 5, 10), date(2026, 5, 11), date(2026, 5, 12), date(2026, 5, 13),
    date(2026, 5, 14), date(2026, 5, 15),
    date(2026, 5, 17), date(2026, 5, 18), date(2026, 5, 19), date(2026, 5, 20),
]


def _candle_path(d: date, side: str) -> Path:
    return CACHE_DIR / SYMBOL / f"{d.year}" / f"{d.month - 1:02d}" / f"{d.day:02d}_{side}.csv.zst"


def _bi5_day_dir(d: date) -> Path:
    return CACHE_DIR / SYMBOL / f"{d.year}" / f"{d.month - 1:02d}" / f"{d.day:02d}"


def _delete_day(d: date) -> int:
    """Delete the corrupt cache files for *d*. Returns number of paths removed."""
    n = 0
    for side in ("bid", "ask"):
        p = _candle_path(d, side)
        if p.exists():
            p.unlink()
            n += 1
    bi5_dir = _bi5_day_dir(d)
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


def _refetch_day(d: date) -> bool:
    """Re-fetch a single day. Returns True if both bid+ask cache files exist after the call."""
    start = datetime(d.year, d.month, d.day, tzinfo=UTC)
    end = start
    try:
        export_range(
            symbol=SYMBOL,
            start_utc=start,
            end_utc_inclusive=end,
            out=Path("/tmp/nzdusd-refetch-out"),  # unused (resample_rule=None)
            price_divisor=PRICE_DIVISOR,
            resample_rule=None,
            cache_dir=CACHE_DIR,
        )
    except RuntimeError as exc:
        # export_range raises "No data produced ..." when resample_rule is None
        # AND no 1-min frames accumulate. With cache_dir set, the daily candle
        # is still written via _flush_day, so this path can be safely ignored
        # for days with valid data. Re-check existence below.
        logging.debug("export_range non-fatal RuntimeError for %s: %s", d, exc)
    bid_ok = _candle_path(d, "bid").exists()
    ask_ok = _candle_path(d, "ask").exists()
    return bid_ok and ask_ok


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("refetch")

    n_total = len(CORRUPT_DATES)
    n_ok = 0
    failures: list[date] = []

    for i, d in enumerate(CORRUPT_DATES, 1):
        log.info("[%d/%d] %s — deleting corrupt cache files", i, n_total, d.isoformat())
        n_removed = _delete_day(d)
        log.info("[%d/%d] %s — removed %d files; re-fetching", i, n_total, d.isoformat(), n_removed)
        if _refetch_day(d):
            n_ok += 1
        else:
            log.error("[%d/%d] %s — REFETCH FAILED (bid or ask missing)", i, n_total, d.isoformat())
            failures.append(d)

    log.info("done: %d/%d days re-fetched cleanly; failures: %s",
             n_ok, n_total, ", ".join(d.isoformat() for d in failures) or "(none)")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
