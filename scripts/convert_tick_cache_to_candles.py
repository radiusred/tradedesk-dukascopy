#!/usr/bin/env python3
"""
One-shot script to convert existing compressed tick cache files to compressed 1-min candle files.

Usage:
    python scripts/convert_tick_cache_to_candles.py <cache_dir>

Finds all *_ticks.csv.zst files, converts each to *_bid.csv.zst + *_ask.csv.zst,
then deletes the source tick file.

Cache directory structure expected:
    <cache_dir>/<SYMBOL>/<YYYY>/<MM_zerobased>/<DD>_ticks.csv.zst

This script is a one-off migration tool. Discard after the existing cache has been converted.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import zstandard as zstd


def _load_tick_file(path: Path) -> pd.DataFrame | None:
    """Load a compressed tick CSV. Returns DataFrame with columns ts,bid,ask,bid_vol,ask_vol."""
    try:
        dctx = zstd.ZstdDecompressor()
        with open(path, "rb") as f_in:
            with dctx.stream_reader(f_in) as reader:
                df = pd.read_csv(io.TextIOWrapper(io.BufferedReader(reader), encoding="utf-8"))
        return df
    except Exception as e:
        print(f"  ERROR loading {path}: {e}")
        return None


def _ticks_df_to_candles(df: pd.DataFrame, price_col: str, vol_col: str) -> pd.DataFrame:
    """Convert a tick DataFrame to 1-min OHLCV candles."""
    if df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    idx = pd.to_datetime(df["ts"], format="ISO8601", utc=True)
    px = pd.Series(df[price_col].values, index=idx)
    vol = pd.Series(df[vol_col].values, index=idx)
    ohlc = px.resample("1min").ohlc()
    v = vol.resample("1min").sum().rename("volume")
    out = pd.concat([ohlc, v], axis=1)
    return out.dropna(subset=["open"])


def _write_candles(df: pd.DataFrame, path: Path) -> None:
    """Atomically write a 1-min candle DataFrame as a Zstandard-compressed CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    out = df.copy()
    out.index.name = "timestamp"
    cctx = zstd.ZstdCompressor(level=3)
    compressed = cctx.compress(out.reset_index().to_csv(index=False).encode("utf-8"))
    tmp.write_bytes(compressed)
    tmp.replace(path)


def _candle_path(tick_path: Path, side: str) -> Path:
    """Derive the candle path from a tick path: DD_ticks.csv.zst → DD_bid.csv.zst."""
    day_str = tick_path.name.split("_")[0]  # e.g. "15" from "15_ticks.csv.zst"
    return tick_path.parent / f"{day_str}_{side}.csv.zst"


def convert_cache(cache_dir: Path) -> None:
    tick_files = sorted(cache_dir.rglob("*_ticks.csv.zst"))
    print(f"Found {len(tick_files)} tick cache file(s) to convert in {cache_dir}")

    success = 0
    failed = 0

    for tick_path in tick_files:
        rel = tick_path.relative_to(cache_dir)
        print(f"  Converting {rel} ...", end=" ", flush=True)

        df = _load_tick_file(tick_path)
        if df is None:
            failed += 1
            continue

        empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        if df.empty or "ts" not in df.columns:
            bid_candles = empty
            ask_candles = empty
        else:
            bid_candles = _ticks_df_to_candles(df, "bid", "bid_vol")
            ask_candles = _ticks_df_to_candles(df, "ask", "ask_vol")

        bid_path = _candle_path(tick_path, "bid")
        ask_path = _candle_path(tick_path, "ask")

        try:
            _write_candles(bid_candles, bid_path)
            _write_candles(ask_candles, ask_path)
            tick_path.unlink()
            print(f"OK ({len(bid_candles)} bid candles, {len(ask_candles)} ask candles)")
            success += 1
        except Exception as e:
            print(f"ERROR writing candles: {e}")
            # Clean up partial output to avoid half-cached state
            for p in (bid_path, ask_path):
                if p.exists():
                    try:
                        p.unlink()
                    except OSError:
                        pass
            failed += 1

    print(f"\nDone. Converted: {success}, Failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <cache_dir>")
        sys.exit(1)

    cache_dir = Path(sys.argv[1])
    if not cache_dir.is_dir():
        print(f"Error: {cache_dir} is not a directory")
        sys.exit(1)

    convert_cache(cache_dir)
