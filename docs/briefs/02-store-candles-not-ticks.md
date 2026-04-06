Always read and follow instructions in `CLAUDE.md` in the project root before processing the brief.

# Store candles not ticks

## Context

Work completed as part of `01-compression.md` is correct and functional but reveals a severe runtime issue when `tradedesk` code uses the cache directly to populate backtests. Conversion of the tick data to candle formats is slow and for long back test runs, unacceptably so. This hit needs to be taken once at download time and not on every backtest.

## Goal

The goal of this session is to replace the tick storage files with OHLCV candle files at one minute resolution. The files should still be compressed on disk as the tick files are now. When the goal is achieved, all future downloads will result in 2 compressed data files per instrument per day using the same directory structure. One file will contain `bid` prices, the other `ask` prices.

## Approach

 - Code for this already exists as the 2 one minute resolution files are created during resampling phase (see `export.py#876`)
   - this work should now occur when the raw `.bi5` files are downloaded and parsed instead
   - future resampling should start from the base one minute files and aggregate
 - Tick files are no longer required and code for this can be discarded
 - Create a separate, one-off script that can be used to convert existing compressed tick files to compressed one minute candle files. This will be a one-shot operation, the tool does not need to handle this requirement as part of its feature set. The script will be discarded when it has finished running on the existing cache
   - Keep any code that operates on incomplete or existing `.bi5` files in the cache. Conversion of these should still occur as needed through normal tool operation
 - 

  
## Output

### export.py changes

**Removed:**
- `_daily_tick_path()` — replaced by `_daily_candle_path()`
- `_migrate_to_compressed()` — no longer needed in the main tool
- `_write_daily_ticks()` — replaced by `_write_daily_candles()`
- `_load_daily_ticks()` — replaced by `_load_daily_candles()`

**Added:**
- `_daily_candle_path(cache_dir, symbol, day, side)` — returns `{day}_{side}.csv.zst` path (e.g. `15_bid.csv.zst`)
- `_write_daily_candles(df, path)` — atomically writes a 1-min candle DataFrame as Zstandard-compressed CSV (level 3)
- `_load_daily_candles(path)` — decompresses and returns a candle DataFrame with UTC DatetimeIndex; returns `None` on error

**Changed in `export_range`:**
- `day_ticks` accumulation replaced with `day_bid_frames` / `day_ask_frames` (per-day lists of 1-min candle DataFrames)
- `days_fully_cached` pre-check now detects `{day}_bid.csv.zst` + `{day}_ask.csv.zst` pairs instead of `_ticks.csv.zst`; removed `_migrate_to_compressed` call
- `_flush_day` — concatenates per-day candle frames and writes bid + ask candle CSVs; empty DataFrame written for market-closed days (preserves cache-hit detection on subsequent runs)
- Cached-day processing branch — loads candle CSVs directly instead of loading ticks and converting
- Tick→candle generation — now runs when `resample_rule is not None OR cache_dir is not None` (previously only when `resample_rule is not None`), so the cache is populated even when no resample output is requested
- Updated docstring to reflect candle-based caching strategy

### Test changes

- **`test_export_caching.py`** — rewritten to assert on `_daily_candle_path` / `_write_daily_candles` / `_load_daily_candles`; removed tick-file references
- **`test_export_1min_cache.py`** — completely rewritten to test `_daily_candle_path`, `_write_daily_candles`, `_load_daily_candles`
- **`test_compression.py`** — removed tick-file and `_migrate_to_compressed` tests; replaced with candle equivalents; `_cleanup_empty_day_dirs` tests retained
- **`test_export_helpers.py`** — replaced `_write_daily_ticks`/`_load_daily_ticks` round-trip tests with `_write_daily_candles`/`_load_daily_candles` equivalents

### New file

- **`scripts/convert_tick_cache_to_candles.py`** — one-off migration script; walks a cache directory, finds all `*_ticks.csv.zst` files, converts each to `*_bid.csv.zst` + `*_ask.csv.zst`, deletes the source. Discard after running.

### Result

78 tests pass, 82% coverage.

---

## License

Licensed under the Apache License, Version 2.0.
See: https://www.apache.org/licenses/LICENSE-2.0

Copyright 2026 [Radius Red Ltd.](https://github.com/radiusred) | [Contact](mailto:opensource@radiusred.uk)
