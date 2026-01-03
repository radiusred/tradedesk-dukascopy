"""
Export Dukascopy historical data by downloading .bi5 *tick* files and resampling to candles.

- Downloads hourly tick files:
    https://datafeed.dukascopy.com/datafeed/{SYMBOL}/{YYYY}/{MM}/{DD}/{HH}h_ticks.bi5
  where MM is zero-based (Jan=00..Dec=11).

- Decompresses LZMA .bi5
- Decodes ticks (bid/ask + volumes)
- Resamples to candles (default 5-minute OHLCV; uses BID unless specified)
- Writes a single CSV per instrument for the requested date range.

Output format:
timestamp,open,high,low,close,volume
(UTC, ISO8601)

- Prices are floats, volumes are floats.
- Month in URL is zero-based. See Dukascopy datafeed conventions.

Examples:
  python scripts/export_dukascopy_candles.py --symbol EURUSD --from 2025-08-01 --to 2025-12-31 --out out/EURUSD_5MINUTE.csv
  python scripts/export_dukascopy_candles.py --symbol USA500IDXUSD --from 2025-11-01 --to 2025-12-31 --out out/US500_5MINUTE.csv
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
import pandas as pd
import logging, requests, io, math, lzma, struct

from tradedesk_dukascopy.metadata import write_sidecar

BASE_URL = "https://datafeed.dukascopy.com/datafeed"
UA = "tradedesk/1.0 bi5-export (https://github.com/radiusred/tradedesk-dukascopy)"
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": UA})

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class Tick:
    ts: datetime
    bid: float
    ask: float
    bid_vol: float
    ask_vol: float


def _symbol_normalise(s: str) -> str:
    """
    Accept inputs like:
      - EURUSD
      - USDJPY
      - USA500.IDX/USD
      - GBR.IDX/GBP
      - usa500idxusd
    Convert to Dukascopy datafeed folder naming, typically uppercase alnum only.
    """
    raw = s.strip()
    if not raw:
        raise ValueError("Empty symbol")

    # Remove separators
    cleaned = "".join(ch for ch in raw if ch.isalnum())
    # Datafeed folders are typically uppercase
    return cleaned.upper()


def _iter_hours(start: datetime, end_exclusive: datetime) -> Iterable[datetime]:
    """
    Yield hour starts [start, end_exclusive) at hourly granularity, UTC.
    """
    cur = start.replace(minute=0, second=0, microsecond=0)
    if cur < start:
        cur += timedelta(hours=1)
    while cur < end_exclusive:
        yield cur
        cur += timedelta(hours=1)


def _dukascopy_tick_url(symbol: str, hour_start: datetime) -> str:
    """
    Dukascopy uses zero-based months in the URL: Jan=00 ... Dec=11
    """
    y = hour_start.year
    m0 = hour_start.month - 1
    d = hour_start.day
    h = hour_start.hour
    return f"{BASE_URL}/{symbol}/{y}/{m0:02d}/{d:02d}/{h:02d}h_ticks.bi5"

def _download_bi5(
    url: str,
    cache_path: Path | None,
    timeout: tuple[float, float] = (5.0, 10.0),
    retries: int = 3,
) -> bytes | None:
    """
    Returns compressed bytes, or None if file doesn't exist or contains no data.

    Notes:
    - Dukascopy sometimes responds 200 with an empty body for hours with no data.
      Treat those as "no data" (no retries), otherwise probe becomes very slow.
    - Retries are for transient transport/server failures, not for empty payloads.
    """
    if cache_path is not None and cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path.read_bytes()

    last_exc: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            with _SESSION.get(url, timeout=timeout) as r:
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                data = r.content

            # Empirical: 200 with empty/tiny payload is effectively "no data for this hour"
            # (do NOT retry; it multiplies runtime).
            if len(data) == 0:
                return None
            if len(data) < 64:
                log.debug("tiny bi5 payload (%d bytes) for %s; treating as no data", len(data), url)
                return None

            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
                tmp.write_bytes(data)
                tmp.replace(cache_path)

            return data

        except Exception as e:
            last_exc = e
            log.debug("download attempt %d/%d failed for %s: %s", attempt, retries, url, e)

    log.warning("skipping %s after %d failed attempts (%s)", url, retries, last_exc)
    return None

def _probe_price_format(compressed: bytes) -> str:
    """
    Read only the first 20-byte tick record via streaming LZMA and decide whether
    bid/ask are float32 or int32.

    Heuristic:
      - interpret ask/bid as float32: if non-finite OR absurdly small (subnormal/near-zero)
        then treat as int32.
    """
    with lzma.open(io.BytesIO(compressed), "rb") as f:
        first = f.read(20)
    if len(first) < 20:
        raise ValueError("bi5 too short to probe")

    # float layout: >i f f f f
    ms, ask_f, bid_f, ask_v, bid_v = struct.unpack(">i f f f f", first)

    if (not math.isfinite(ask_f)) or (not math.isfinite(bid_f)):
        return "int"

    # Float mis-decode often yields tiny denormals ~1e-38 for indices.
    if abs(ask_f) < 1e-6 and abs(bid_f) < 1e-6:
        return "int"

    return "float"

def _read_n_tick_records(compressed: bytes, n: int) -> bytes:
    # Stream-decompress just enough to read n tick records (20 bytes each).
    need = 20 * n
    with lzma.open(io.BytesIO(compressed), "rb") as f:
        return f.read(need)

def _decode_ticks(hour_start: datetime, compressed: bytes, *, price_format: str, price_divisor: float) -> list[Tick]:
    """
    Decode a .bi5 tick file.

    Layout per tick row (20 bytes):
      int32  ms_since_hour_start
      float32 ask
      float32 bid
      float32 ask_volume
      float32 bid_volume

    Endianness: big-endian is commonly used in bi5 decoders.
    """
    raw = lzma.decompress(compressed)
    if len(raw) % 20 != 0:
        raise ValueError(f"Unexpected bi5 payload length: {len(raw)} (not multiple of 20)")

    ticks: list[Tick] = []

    if price_format == "float":
        unpack = struct.Struct(">i f f f f").unpack_from
        for i in range(0, len(raw), 20):
            ms, ask, bid, ask_vol, bid_vol = unpack(raw, i)
            ts = hour_start + timedelta(milliseconds=int(ms))
            ticks.append(Tick(ts=ts, bid=float(bid), ask=float(ask), bid_vol=float(bid_vol), ask_vol=float(ask_vol)))
        return ticks

    if price_format == "int":
        div = float(price_divisor or 1.0)
        unpack = struct.Struct(">i i i f f").unpack_from  # ask,bid as int32
        for i in range(0, len(raw), 20):
            ms, ask_i, bid_i, ask_vol, bid_vol = unpack(raw, i)
            ts = hour_start + timedelta(milliseconds=int(ms))
            ticks.append(Tick(ts=ts, bid=float(bid_i) / div, ask=float(ask_i) / div, bid_vol=float(bid_vol), ask_vol=float(ask_vol)))
        return ticks

    raise ValueError("price_format must be 'float' or 'int'")


def _ticks_to_candles(
    ticks: list[Tick],
    *,
    resample_rule: str,
    price_side: str = "bid",
) -> pd.DataFrame:
    """
    Resample ticks to OHLCV using a pandas resample rule (e.g. '1min', '5min', '15min', '1H').
    Volume uses bid_vol (for bid) or ask_vol (for ask); if mid, uses (bid_vol+ask_vol)/2.
    """
    if not ticks:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    idx = pd.DatetimeIndex([t.ts for t in ticks], tz="UTC")

    if price_side == "bid":
        px = pd.Series([t.bid for t in ticks], index=idx)
        vol = pd.Series([t.bid_vol for t in ticks], index=idx)
    elif price_side == "ask":
        px = pd.Series([t.ask for t in ticks], index=idx)
        vol = pd.Series([t.ask_vol for t in ticks], index=idx)
    elif price_side == "mid":
        px = pd.Series([(t.bid + t.ask) / 2.0 for t in ticks], index=idx)
        vol = pd.Series([(t.bid_vol + t.ask_vol) / 2.0 for t in ticks], index=idx)

    else:
        raise ValueError("price_side must be one of: bid, ask, mid")

    ohlc = px.resample(resample_rule).ohlc()
    v = vol.resample(resample_rule).sum().rename("volume")

    out = pd.concat([ohlc, v], axis=1)
    out = out.dropna(subset=["open", "high", "low", "close"])

    return out


def export_range(
    *,
    symbol: str,
    start_utc: datetime,
    end_utc_inclusive: datetime,
    out: Path,
    price_side: str,
    price_divisor: float = 1.0,
    resample_rule: str,
    cache_dir: Path | None,
    probe: bool = False,
    probe_ticks: int = 10,
) -> None:
    """
    Export [start_utc, end_utc_inclusive] into one CSV.
    """

    # counters
    hours_total = 0
    hours_missing_404 = 0
    hours_downloaded = 0
    hours_decode_failed = 0
    hours_resampled_nonempty = 0

    detected_format: str | None = None
    symbol = _symbol_normalise(symbol)

    log.info(f"Exporting {symbol} from {start_utc.isoformat()} to {end_utc_inclusive.isoformat()}")

    # End-exclusive boundary for hour iteration
    end_exclusive = (end_utc_inclusive + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    all_frames: list[pd.DataFrame] = []

    for hour_start in _iter_hours(start_utc, end_exclusive):
        hours_total += 1
        url = _dukascopy_tick_url(symbol, hour_start)

        cache_path = None
        if cache_dir is not None:
            # Cache mirrors URL path (nice for debugging)
            cache_path = cache_dir / symbol / f"{hour_start.year}" / f"{hour_start.month-1:02d}" / f"{hour_start.day:02d}" / f"{hour_start.hour:02d}h_ticks.bi5"
        
        dl_timeout = (3.0, 3.0) if probe else (5.0, 10.0)
        dl_retries = 1 if probe else 3
        try:
            comp = _download_bi5(url, cache_path=cache_path, timeout=dl_timeout, retries=dl_retries)
        except Exception as e:
            if probe:
                log.debug("probe: skipping hour %s due to download error: %s", url, e)
                continue
            raise

        if comp is None:
            hours_missing_404 += 1
            continue

        hours_downloaded += 1
        if detected_format is None:
            detected_format = _probe_price_format(comp)
            log.info(f"{symbol}: detected tick price format = {detected_format}")

        if probe:
            print(f"{symbol}: detected tick price format = {detected_format}")
            raw20 = _read_n_tick_records(comp, max(1, probe_ticks))
            if len(raw20) < 20:
                print(f"{symbol}: probe failed (not enough decompressed bytes)")
                return

            if detected_format == "float":
                unpack = struct.Struct(">i f f f f").unpack_from
                print(f"{symbol} @ {hour_start.isoformat()} (float): first {probe_ticks} ticks")
                for i in range(0, min(len(raw20), 20 * probe_ticks), 20):
                    ms, ask, bid, ask_vol, bid_vol = unpack(raw20, i)
                    ts = hour_start + timedelta(milliseconds=int(ms))
                    print(ts.isoformat(), "bid", bid, "ask", ask, "bid_vol", bid_vol)
            else:
                unpack = struct.Struct(">i i i f f").unpack_from
                print(f"{symbol} @ {hour_start.isoformat()} (int): first {probe_ticks} ticks")
                # show a few divisor interpretations to make it obvious
                divisors = [1, 10, 100, 1000, 10000, 100000]
                rows = []
                for i in range(0, min(len(raw20), 20 * probe_ticks), 20):
                    ms, ask_i, bid_i, ask_vol, bid_vol = unpack(raw20, i)
                    ts = hour_start + timedelta(milliseconds=int(ms))
                    rows.append((ts, bid_i, ask_i, bid_vol))
                # print first tick with suggested scalings
                ts0, bid0, ask0, vol0 = rows[0]
                print("first tick raw:", ts0.isoformat(), "bid_i", bid0, "ask_i", ask0, "vol", vol0)
                for d in divisors:
                    print(f"  divisor {d:>6}: bid {bid0/d:.6f} ask {ask0/d:.6f}")
                # also print using user divisor
                d = price_divisor or 1.0
                print(f"using --price-divisor {d}:")
                for ts, bid_i, ask_i, bid_vol in rows:
                    print(ts.isoformat(), "bid", bid_i/d, "ask", ask_i/d, "bid_vol", bid_vol)

            return  # probe exits after first successful hour

        try:
            assert detected_format is not None
            ticks = _decode_ticks(hour_start, comp, price_format=detected_format, price_divisor=price_divisor)
        except lzma.LZMAError:
            # If we cached a partial download, self-heal: delete cache and retry once.
            if cache_path is not None and cache_path.exists():
                try:
                    log.warning(f"{symbol}: deleting suspect cache file: {cache_path}")
                    cache_path.unlink()
                except OSError:
                    log.error(f"{symbol}: failed deleting suspect cache file: {cache_path}")

            comp2 = _download_bi5(url, cache_path=cache_path)
            if comp2 is None:
                continue

            try:
                ticks = _decode_ticks(hour_start, comp2, price_format=detected_format, price_divisor=price_divisor)
            except Exception as e:
                # Don’t kill the whole export; skip this hour and continue.
                log.warning(f"skipping corrupt hour {url}: {e}")
                hours_decode_failed += 1
                continue
        except Exception as e:
            log.warning(f"skipping hour {url}: {e}")
            hours_decode_failed += 1
            continue

        df = _ticks_to_candles(ticks, resample_rule=resample_rule, price_side=price_side)
        if not df.empty:
            hours_resampled_nonempty += 1
            all_frames.append(df)

        if (hour_start.hour % 6 == 0):
            log.info(f"{symbol}: processed up to {hour_start.isoformat()}")

    if not all_frames:
        raise RuntimeError(f"No data produced for symbol={symbol} in range {start_utc}..{end_utc_inclusive}")

    frames = pd.concat(all_frames).sort_index()
    # clip exactly to requested date range (inclusive)
    frames = frames.loc[start_utc : (end_utc_inclusive + timedelta(days=1) - timedelta(microseconds=1))]
    # de-duplicate any overlapping resample bins (shouldn't happen, but safe)
    frames = frames[~frames.index.duplicated(keep="last")]
    # Export with explicit timestamp column (backtest expects this)
    out_reset = frames.reset_index().rename(columns={"index": "timestamp"})
    # ISO8601 in UTC with offset
    out_reset["timestamp"] = out_reset["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
    
    # log stats/counters
    log.info(
        f"{symbol}: hours total={hours_total}, missing_404={hours_missing_404}, "
        f"downloaded={hours_downloaded}, decode_failed={hours_decode_failed}, "
        f"resampled_nonempty={hours_resampled_nonempty}, candles={len(frames)}"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    rule_label = resample_rule.replace(" ", "").upper()
    out_csv = out / f"{symbol}_{rule_label}.csv"
    out_reset.to_csv(out_csv, index=False)

    log.info(f"Wrote: {out_csv}")
