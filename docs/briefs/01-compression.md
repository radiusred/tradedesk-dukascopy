Always read and follow instructions in `CLAUDE.md` in the project root before processing the brief.

# Compression of data files

## Context

Recent work in `tradedesk-dukascopy` improved the cache by aggregating hourly `.bi5` files into daily `.csv` files of tick data. However, this has led to an explosion in disk usage for a modest number of instrumets and periods collected.

## Goal

The goal of this session is to retain the beneficial cache format improvement but reduce the disk usage. Each time a daily `.csv` file is written to the cache, it must be compressed using the `Zstandard` library.

## Approach

- Add `zstandard` as a dependency (e.g., via `poetry add zstandard` or `pip install zstandard`).
 - Compress files as they are written, store only the compressed version on disk.
 - Optimise the current process by removing the parent directory of the `.bi5` files in addition to the files. Currently empty directories are left on disk
 - The goal should be achieved regardless of the current contents of the cache. For example;
   - when a new download is requested for existing instruments/periods, any current uncompressed daily `.csv` files must be compressed
   - existing empty daily directories must be removed
 - When resampling files to an `--out` directory, the resampled files should not be compressed
 - Ensure that the progress bars used in the `rich` terminal output reflect the operations in progress. Currently downloads can finish quickly from the user perspective (progress bar reaches 100%) but the program can take many minutes more to end with no user feedback while the actual daily files are generated.
   - Progress for daily CSV writing: add a fourth progress task per symbol (e.g. [cyan]EURUSD[/] cache) that tracks per-day CSV writes. Total = number of days being processed (excluding already-cached days). It advances in _flush_day.
 - File extension for compressed daily files will be `.csv.zst` (e.g. 20_ticks.csv.zst). _daily_tick_path will be updated accordingly.
 - Migration of existing uncompressed .csv files: When export_range runs and finds an existing _ticks.csv (uncompressed) for a day, compress it to .csv.zst and delete the .csv. This happens at the start, during the days_fully_cached check.
 - Use Zstandard compression level 3
 - When cached compressed files need to be read to resample, use `io.TextIOWrapper` for speed and efficiency
  
## Output

All 78 tests pass. Here's a summary of everything implemented:

### export.py changes:

- import zstandard as zstd added
- `_daily_tick_path` — extension changed to `.csv.zst`
- `_migrate_to_compressed(csv_path, zst_path)` — compresses existing uncompressed `.csv` → `.csv.zst` atomically via copy_stream, deletes original; returns False and cleans up .tmp on failure
- `_cleanup_empty_day_dirs(cache_dir, symbol)` — removes empty day directories left over from previous bi5 deletion runs
- `_write_daily_ticks` — now compresses with ZstdCompressor(level=3)
- `_load_daily_ticks` — decompresses via stream_reader wrapped in `io.TextIOWrapper(io.BufferedReader(...))` as specified
- `days_fully_cached check` — migrates any existing uncompressed `.csv` files during startup, then calls `_cleanup_empty_day_dirs`
- `cache_task_id` progress task — fourth task per symbol ([cyan]SYMBOL[/] cache), total = days not already cached, advances in `_flush_day` for every day (including gapped days, so it always reaches 100%)
- `_flush_day` — after deleting `bi5` files, attempts `day_dir.rmdir()` to remove the now-empty directory

New test file: `tests/test_compression.py` — covers write/load round-trip, corrupt file handling, migration (success, failure, tmp cleanup), and empty dir cleanup.

---

## License

Licensed under the Apache License, Version 2.0.
See: https://www.apache.org/licenses/LICENSE-2.0

Copyright 2026 [Radius Red Ltd.](https://github.com/radiusred) | [Contact](mailto:opensource@radiusred.uk)
