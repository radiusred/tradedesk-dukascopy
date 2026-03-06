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
  python scripts/export_dukascopy_candles.py \
    --symbol EURUSD --from 2025-08-01 --to 2025-12-31 \
    --out out/EURUSD_5MINUTE.csv

  python scripts/export_dukascopy_candles.py \
    --symbol USA500IDXUSD --from 2025-11-01 --to 2025-12-31 \
    --out out/US500_5MINUTE.csv
"""

import io
import logging
import lzma
import math
import struct
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from rich.progress import Progress

BASE_URL = "https://datafeed.dukascopy.com/datafeed"
UA = "tradedesk/1.0 bi5-export (https://github.com/radiusred/tradedesk-dukascopy)"
# Retry configuration
RETRY_BASE_DELAY = 0.5  # seconds
RETRY_MAX_DELAY = 4.0  # seconds
RETRY_BACKOFF_FACTOR = 2.0
# Download parallelisation
DOWNLOAD_THREADS_PER_INSTRUMENT = 4

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
    timeout: tuple[float, float] = (3.0, 15.0),
    retries: int = 3,
) -> bytes | None:
    """
    Returns compressed bytes.

    - None means "no file" (HTTP 404) or unrecoverable failure.
    - b"" means "valid but empty" (HTTP 200 with zero-length body): no tick data for that hour.

    We cache empty payloads as empty files so repeated exports do not re-download them.

    Uses exponential backoff on retries: 0.5s, 1.0s, 2.0s, 4.0s (capped).
    """
    # If cached, return it even if it's 0 bytes (0 bytes means "no ticks for this hour")
    if cache_path is not None and cache_path.exists():
        return cache_path.read_bytes()

    last_exc: Exception | None = None
    delay = RETRY_BASE_DELAY

    for attempt in range(1, retries + 1):
        try:
            with _SESSION.get(url, timeout=timeout) as r:
                if r.status_code == 404:
                    log.info("no tick data found (HTTP 404): %s", url)
                    return None
                r.raise_for_status()
                data: bytes = bytes(r.content)

            # HTTP 200 with empty body is valid: "no ticks this hour"
            if len(data) == 0:
                if cache_path is not None:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.touch(exist_ok=True)  # cache the "empty hour"
                return b""

            # Tiny non-zero payloads are usually junk/edge; keep existing behavior.
            if len(data) < 64:
                log.debug("tiny bi5 payload (%d bytes) for %s; treating as no data", len(data), url)
                if cache_path is not None:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.touch(exist_ok=True)
                return b""

            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
                tmp.write_bytes(data)
                tmp.replace(cache_path)

            return data

        except Exception as e:
            last_exc = e
            log.debug("download attempt %d/%d failed for %s: %s", attempt, retries, url, e)

            # Backoff before retry (but not after final attempt)
            if attempt < retries:
                import time

                time.sleep(delay)
                delay = min(delay * RETRY_BACKOFF_FACTOR, RETRY_MAX_DELAY)

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
    try:
        with lzma.open(io.BytesIO(compressed), "rb") as f:
            first = f.read(20)

        if len(first) < 20:
            raise ValueError("bi5 too short to probe")

    except EOFError as e:
        raise ValueError("Not enough decompressed bytes to probe tick format") from e

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


def _decode_ticks(
    hour_start: datetime, compressed: bytes, *, price_format: str, price_divisor: float
) -> list[Tick]:
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
            ticks.append(
                Tick(
                    ts=ts,
                    bid=float(bid),
                    ask=float(ask),
                    bid_vol=float(bid_vol),
                    ask_vol=float(ask_vol),
                )
            )
        return ticks

    if price_format == "int":
        div = float(price_divisor or 1.0)
        unpack = struct.Struct(">i i i f f").unpack_from  # ask,bid as int32
        for i in range(0, len(raw), 20):
            ms, ask_i, bid_i, ask_vol, bid_vol = unpack(raw, i)
            ts = hour_start + timedelta(milliseconds=int(ms))
            ticks.append(
                Tick(
                    ts=ts,
                    bid=float(bid_i) / div,
                    ask=float(ask_i) / div,
                    bid_vol=float(bid_vol),
                    ask_vol=float(ask_vol),
                )
            )
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
    resample_rule = resample_rule.strip().lower()

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


def _probe(
    symbol: str,
    hours: list[datetime],
    cache_dir: Path | None,
    probe_ticks: int,
    price_divisor: float | None,
) -> None:
    for hour in hours:
        url = _dukascopy_tick_url(symbol, hour)
        cache_path = None
        if cache_dir is not None:
            cache_path = (
                cache_dir
                / symbol
                / f"{hour.year}"
                / f"{hour.month - 1:02d}"
                / f"{hour.day:02d}"
                / f"{hour.hour:02d}h_ticks.bi5"
            )

        comp = _download_bi5(url, cache_path=cache_path, timeout=(2.0, 10.0), retries=3)

        if comp is None or len(comp) == 0:
            print(f"{symbol}: no data for probe hour {hour.isoformat()}")
            continue

        detected_format = _probe_price_format(comp)
        print(f"{symbol}: detected tick price format = {detected_format}")
        raw20 = _read_n_tick_records(comp, max(1, probe_ticks))

        if len(raw20) < 20:
            print(f"{symbol}: probe failed (not enough decompressed bytes)")
            continue

        if detected_format == "float":
            unpack = struct.Struct(">i f f f f").unpack_from
            print(f"{symbol} @ {hour.isoformat()} (float): first {probe_ticks} ticks")
            for i in range(0, min(len(raw20), 20 * probe_ticks), 20):
                ms, ask, bid, ask_vol, bid_vol = unpack(raw20, i)
                ts = hour + timedelta(milliseconds=int(ms))
                print(ts.isoformat(), "bid", bid, "ask", ask, "bid_vol", bid_vol)
        else:
            unpack = struct.Struct(">i i i f f").unpack_from
            print(f"{symbol} @ {hour.isoformat()} (int): first {probe_ticks} ticks")
            divisors = [1, 10, 100, 1000, 10000, 100000]
            rows = []
            for i in range(0, min(len(raw20), 20 * probe_ticks), 20):
                ms, ask_i, bid_i, ask_vol, bid_vol = unpack(raw20, i)
                ts = hour + timedelta(milliseconds=int(ms))
                rows.append((ts, bid_i, ask_i, bid_vol))
            ts0, bid0, ask0, vol0 = rows[0]
            print("first tick raw:", ts0.isoformat(), "bid_i", bid0, "ask_i", ask0, "vol", vol0)
            for divisor in divisors:
                print(f"  divisor {divisor:>6}: bid {bid0 / divisor:.6f} ask {ask0 / divisor:.6f}")

            price_div: float = price_divisor or 1.0
            print(f"using --price-divisor {price_div}:")
            for ts, bid_i, ask_i, bid_vol in rows:
                print(
                    ts.isoformat(),
                    "bid",
                    bid_i / price_div,
                    "ask",
                    ask_i / price_div,
                    "bid_vol",
                    bid_vol,
                )
        return None


def _daily_tick_path(cache_dir: Path, symbol: str, day: date) -> Path:
    """Return the path for a daily tick CSV cache file."""
    return cache_dir / symbol / f"{day.year}" / f"{day.month - 1:02d}" / f"{day.day:02d}_ticks.csv"


def _candles_to_candles(df: pd.DataFrame, resample_rule: str) -> pd.DataFrame:
    """
    Aggregate OHLCV candle DataFrame to a larger timeframe.

    Uses first/max/min/last/sum aggregation — matching the CandleAggregator
    pattern in the tradedesk project.
    """
    if df.empty:
        return df
    rule = resample_rule.strip().lower()
    out = df.resample(rule).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    )
    return out.dropna(subset=["open"])


def _write_daily_ticks(ticks: list[Tick], path: Path) -> None:
    """Atomically write daily tick data to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df = pd.DataFrame(
        {
            "ts": [t.ts.isoformat() for t in ticks],
            "bid": [t.bid for t in ticks],
            "ask": [t.ask for t in ticks],
            "bid_vol": [t.bid_vol for t in ticks],
            "ask_vol": [t.ask_vol for t in ticks],
        }
    )
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def _load_daily_ticks(path: Path) -> list[Tick] | None:
    """Read a daily tick CSV. Returns None on missing file or parse error."""
    try:
        df = pd.read_csv(path)
        timestamps = pd.to_datetime(df["ts"], format="ISO8601", utc=True)
        return [
            Tick(
                ts=ts.to_pydatetime().replace(tzinfo=UTC),
                bid=float(bid),
                ask=float(ask),
                bid_vol=float(bv),
                ask_vol=float(av),
            )
            for ts, bid, ask, bv, av in zip(
                timestamps, df["bid"], df["ask"], df["bid_vol"], df["ask_vol"], strict=True
            )
        ]
    except Exception:
        return None


def export_range(
    *,
    symbol: str,
    start_utc: datetime,
    end_utc_inclusive: datetime,
    out: Path,
    price_side: str = "bid",
    price_divisor: float = 1.0,
    resample_rule: str,
    cache_dir: Path | None,
    probe: bool = False,
    probe_ticks: int = 10,
    progress: "Progress | None" = None,
) -> tuple[Path | None, Path | None]:
    """
    Export [start_utc, end_utc_inclusive] into two CSVs: one for bid prices, one for ask.
    Returns (bid_csv, ask_csv); either may be None if no data or resample_rule is None.

    If progress is provided, we create three tasks per symbol:
      - dl: download attempts
      - rs: processing/resampling progress (advances once per hour processed)
      - write: advances once per output file written (max 2)

    Caching strategy (when cache_dir is set):
      - .bi5 tick files are downloaded and cached as before.
      - After all hours of a day decode successfully, a daily tick CSV file is
        written (containing raw tick data: ts/bid/ask/bid_vol/ask_vol) and the
        .bi5 files for that day are deleted.
      - On subsequent runs, days with a daily tick CSV present skip .bi5
        download/decode entirely and load ticks from the CSV, converting to
        candles on the fly.
      - A day is only committed to a daily tick CSV when every hour has a
        definitive result (successfully decoded or legitimate empty-200). Hours
        with 404 or decode failures leave the day uncommitted so the next run
        can retry.
    """

    # counters
    hours_total = 0
    hours_missing_404 = 0
    hours_empty_200 = 0
    hours_downloaded = 0
    hours_decode_failed = 0
    hours_resampled_nonempty = 0
    hours_loaded_from_cache = 0

    detected_format: str | None = None
    symbol = _symbol_normalise(symbol)

    # End-exclusive boundary for hour iteration
    end_exclusive = (end_utc_inclusive + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Accumulates 1-minute candle frames (bid and ask) across all hours.
    # At the end these are each aggregated once to the target resample_rule.
    all_1min_bid_frames: list[pd.DataFrame] = []
    all_1min_ask_frames: list[pd.DataFrame] = []

    # Per-day tick lists — used for writing daily tick CSVs.
    day_ticks: dict[date, list[Tick]] = {}

    # Days where at least one hour had a non-definitive result (404 / decode failure).
    # These days are NOT written to daily CSVs so the next run can retry.
    day_has_gaps: set[date] = set()

    # Collect all hours to download
    hours_to_fetch = list(_iter_hours(start_utc, end_exclusive))
    hours_total = len(hours_to_fetch)

    # Probe mode: check only first 24 hours, probe the first that works, and exit immediately
    if probe:
        log.info(f"Running probe for {symbol} starting at {start_utc.isoformat()}")
        _probe(symbol, hours_to_fetch[0:24], cache_dir, probe_ticks, price_divisor)
        return (None, None)

    # Pre-check: identify days where a daily tick CSV already exists.
    # Those days skip .bi5 download and decode entirely.
    days_fully_cached: set[date] = set()
    unique_days: set[date] = set()
    if cache_dir is not None:
        unique_days = {h.date() for h in hours_to_fetch}
        days_fully_cached = {
            day for day in unique_days if _daily_tick_path(cache_dir, symbol, day).exists()
        }

    # Early exit: if every day is cached there may be nothing to (re)generate.
    if cache_dir is not None and unique_days and days_fully_cached == unique_days:
        if resample_rule is None:
            # No output is ever written without a resample rule; cache is complete.
            log.info(
                f"{symbol}: all {len(unique_days)} days cached and no resample requested; nothing to do"
            )
            return (None, None)
        rule_label = resample_rule.replace(" ", "").upper()
        bid_csv = out / f"{symbol}_{rule_label}_bid.csv"
        ask_csv = out / f"{symbol}_{rule_label}_ask.csv"
        if bid_csv.exists() and ask_csv.exists():
            log.info(
                f"{symbol}: all {len(unique_days)} days cached and output CSVs exist; skipping export"
            )
            return (bid_csv, ask_csv)

    # Create progress tasks if Progress object provided.
    dl_task_id = None
    rs_task_id = None
    write_task_id = None
    if progress is not None:
        dl_task_id = progress.add_task(
            f"[cyan]{symbol}[/] dl",
            total=hours_total,
            symbol=symbol,
            phase="dl",
        )
        if resample_rule is not None:
            rs_task_id = progress.add_task(
                f"[cyan]{symbol}[/] rs",
                total=hours_total,
                symbol=symbol,
                phase="rs",
            )
            write_task_id = progress.add_task(
                f"[cyan]{symbol}[/] write",
                total=2,
                symbol=symbol,
                phase="write",
            )

    hours_to_download = [h for h in hours_to_fetch if h.date() not in days_fully_cached]

    # last_hour_of_day[d] is the latest hour in hours_to_fetch for date d.
    # Used to detect when a day's processing is complete.
    last_hour_of_day: dict[date, datetime] = {h.date(): h for h in hours_to_fetch}

    # Sentinel value stored in hour_data for hours belonging to fully-cached days.
    _DAY_CACHED = object()

    # Normal mode: parallel download
    log.info(f"Exporting {symbol} from {start_utc.isoformat()} to {end_utc_inclusive.isoformat()}")
    log.info(
        f"{symbol}: fetching {len(hours_to_download)} hours "
        f"({len(days_fully_cached)} days loaded from cache) "
        f"with {DOWNLOAD_THREADS_PER_INSTRUMENT} threads"
    )

    # Pre-populate hour_data for cached days and advance dl progress immediately.
    hour_data: dict[datetime, object] = {}
    for h in hours_to_fetch:
        if h.date() in days_fully_cached:
            hour_data[h] = _DAY_CACHED
    if days_fully_cached and progress is not None and dl_task_id is not None:
        n_cached_hours = sum(1 for h in hours_to_fetch if h.date() in days_fully_cached)
        progress.update(dl_task_id, advance=n_cached_hours)

    # Download hours in parallel
    def download_hour(hour_start: datetime) -> tuple[datetime, bytes | None]:
        """Download a single hour's tick data."""
        try:
            url = _dukascopy_tick_url(symbol, hour_start)
            cache_path = None
            if cache_dir is not None:
                cache_path = (
                    cache_dir
                    / symbol
                    / f"{hour_start.year}"
                    / f"{hour_start.month - 1:02d}"
                    / f"{hour_start.day:02d}"
                    / f"{hour_start.hour:02d}h_ticks.bi5"
                )

            comp = _download_bi5(url, cache_path=cache_path, timeout=(2.0, 10.0), retries=3)

            if progress is not None and dl_task_id is not None:
                progress.update(dl_task_id, advance=1)

            return (hour_start, comp)

        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.debug(f"Download failed for {hour_start}: {e}")
            return (hour_start, None)

    next_to_process = 0  # Index in hours_to_fetch

    def _advance_resample_progress() -> None:
        if progress is not None and rs_task_id is not None and resample_rule is not None:
            progress.update(rs_task_id, advance=1)

    def _flush_day(day: date) -> None:
        """
        Write daily tick CSV and delete .bi5 files,
        but only if the day completed without gaps (no 404s or decode failures).
        """
        ticks_for_day = day_ticks.pop(day, [])
        if cache_dir is None or day in day_has_gaps:
            return

        try:
            _write_daily_ticks(ticks_for_day, _daily_tick_path(cache_dir, symbol, day))
        except Exception as e:
            log.warning(f"{symbol}: failed to write daily tick CSV for {day}: {e}")
            return

        # Delete .bi5 files for this day now that daily CSVs are in place
        for h in hours_to_fetch:
            if h.date() != day:
                continue
            bi5_path = (
                cache_dir
                / symbol
                / f"{h.year}"
                / f"{h.month - 1:02d}"
                / f"{h.day:02d}"
                / f"{h.hour:02d}h_ticks.bi5"
            )
            if bi5_path.exists():
                try:
                    bi5_path.unlink()
                except OSError:
                    log.warning(f"{symbol}: could not delete bi5 cache: {bi5_path}")

    def _process_ready_hours() -> None:
        nonlocal next_to_process, hours_missing_404, hours_empty_200, hours_downloaded
        nonlocal hours_decode_failed, hours_resampled_nonempty, detected_format
        nonlocal hours_loaded_from_cache

        while next_to_process < len(hours_to_fetch):
            current_hour = hours_to_fetch[next_to_process]

            if current_hour not in hour_data:
                break  # Wait for this hour to download

            comp = hour_data.pop(current_hour)
            next_to_process += 1

            cache_path = None
            if cache_dir is not None:
                cache_path = (
                    cache_dir
                    / symbol
                    / f"{current_hour.year}"
                    / f"{current_hour.month - 1:02d}"
                    / f"{current_hour.day:02d}"
                    / f"{current_hour.hour:02d}h_ticks.bi5"
                )

            current_day = current_hour.date()
            is_last_hour_of_day = current_hour == last_hour_of_day[current_day]

            # --- Cached day: load daily tick CSV on the last hour of the day ---
            if comp is _DAY_CACHED:
                hours_loaded_from_cache += 1
                if is_last_hour_of_day and resample_rule is not None:
                    cached_ticks = _load_daily_ticks(
                        _daily_tick_path(cache_dir, symbol, current_day)  # type: ignore[arg-type]
                    )
                    if cached_ticks:
                        one_min_bid = _ticks_to_candles(
                            cached_ticks, resample_rule="1min", price_side="bid"
                        )
                        one_min_ask = _ticks_to_candles(
                            cached_ticks, resample_rule="1min", price_side="ask"
                        )
                        if not one_min_bid.empty:
                            all_1min_bid_frames.append(one_min_bid)
                        if not one_min_ask.empty:
                            all_1min_ask_frames.append(one_min_ask)
                _advance_resample_progress()
                continue

            # --- 404: no data for this hour ---
            if comp is None:
                hours_missing_404 += 1
                day_has_gaps.add(current_day)
                _advance_resample_progress()
                if is_last_hour_of_day:
                    _flush_day(current_day)
                continue

            # --- Empty 200: legitimate market-closed hour ---
            if len(comp) == 0:  # type: ignore[arg-type]
                hours_empty_200 += 1
                _advance_resample_progress()
                if is_last_hour_of_day:
                    _flush_day(current_day)
                continue

            hours_downloaded += 1

            if detected_format is None:
                detected_format = _probe_price_format(comp)  # type: ignore[arg-type]
                log.info(f"{symbol}: detected tick price format = {detected_format}")

            # --- Decode ticks ---
            try:
                assert detected_format is not None
                ticks = _decode_ticks(
                    current_hour,
                    comp,  # type: ignore[arg-type]
                    price_format=detected_format,
                    price_divisor=price_divisor,
                )
            except lzma.LZMAError:
                if cache_path is not None and cache_path.exists():
                    try:
                        log.warning(f"{symbol}: deleting suspect cache file: {cache_path}")
                        cache_path.unlink()
                    except OSError:
                        log.error(f"{symbol}: rm failed: suspect cache file: {cache_path}")

                comp2 = _download_bi5(
                    _dukascopy_tick_url(symbol, current_hour), cache_path=cache_path
                )
                if comp2 is None:
                    day_has_gaps.add(current_day)
                    _advance_resample_progress()
                    if is_last_hour_of_day:
                        _flush_day(current_day)
                    continue

                try:
                    ticks = _decode_ticks(
                        current_hour,
                        comp2,
                        price_format=detected_format,
                        price_divisor=price_divisor,
                    )
                except Exception as e:
                    log.warning(f"corrupt hour {_dukascopy_tick_url(symbol, current_hour)}: {e}")
                    hours_decode_failed += 1
                    day_has_gaps.add(current_day)
                    _advance_resample_progress()
                    if is_last_hour_of_day:
                        _flush_day(current_day)
                    continue
            except Exception as e:
                log.warning(f"skipping hour {_dukascopy_tick_url(symbol, current_hour)}: {e}")
                hours_decode_failed += 1
                day_has_gaps.add(current_day)
                _advance_resample_progress()
                if is_last_hour_of_day:
                    _flush_day(current_day)
                continue

            # --- Generate 1-minute candles for bid and ask ---
            if resample_rule is not None:
                one_min_bid = _ticks_to_candles(ticks, resample_rule="1min", price_side="bid")
                one_min_ask = _ticks_to_candles(ticks, resample_rule="1min", price_side="ask")
                if not one_min_bid.empty or not one_min_ask.empty:
                    hours_resampled_nonempty += 1
                if not one_min_bid.empty:
                    all_1min_bid_frames.append(one_min_bid)
                if not one_min_ask.empty:
                    all_1min_ask_frames.append(one_min_ask)

            # Store raw ticks for daily CSV writing (when caching)
            if cache_dir is not None:
                day_ticks.setdefault(current_day, []).extend(ticks)

            _advance_resample_progress()

            if (current_hour.hour % 24 == 0) and progress is None:
                log.info(f"{symbol}: processed up to {current_hour.isoformat()}")

            if is_last_hour_of_day:
                _flush_day(current_day)

    try:
        with ThreadPoolExecutor(max_workers=DOWNLOAD_THREADS_PER_INSTRUMENT) as executor:
            futures = {executor.submit(download_hour, h): h for h in hours_to_download}

            for future in as_completed(futures):
                from tradedesk_dukascopy.parallel import _cancellation_event

                if _cancellation_event.is_set():
                    raise KeyboardInterrupt()

                hour_start, comp = future.result()
                hour_data[hour_start] = comp

                _process_ready_hours()

    except KeyboardInterrupt:
        log.warning(f"{symbol}: download interrupted")
        raise

    # Flush any remaining hours (e.g. all days were cached, no futures ran)
    _process_ready_hours()

    log.info(
        f"{symbol}: hours total={hours_total}, missing_404={hours_missing_404}, "
        f"missing_200={hours_empty_200}, downloaded={hours_downloaded}, "
        f"decode_failed={hours_decode_failed}, "
        f"resampled_nonempty={hours_resampled_nonempty}, "
        f"loaded_from_cache={hours_loaded_from_cache}"
    )

    if resample_rule is None:
        return (None, None)

    if not all_1min_bid_frames and not all_1min_ask_frames:
        raise RuntimeError(
            f"No data produced for symbol={symbol} in range {start_utc}..{end_utc_inclusive}"
        )

    out.mkdir(parents=True, exist_ok=True)
    rule_label = resample_rule.replace(" ", "").upper()
    start_ts = pd.Timestamp(start_utc)
    end_ts = pd.Timestamp(end_utc_inclusive + timedelta(days=1) - timedelta(microseconds=1))

    out_csv_bid: Path | None = None
    out_csv_ask: Path | None = None

    for side, frames_list in (
        ("bid", all_1min_bid_frames),
        ("ask", all_1min_ask_frames),
    ):
        if not frames_list:
            continue
        all_1min = pd.concat(frames_list).sort_index()
        all_1min = all_1min.loc[start_ts:end_ts]
        # Aggregate 1-min candles to target resample rule (single pass over full range
        # avoids boundary artefacts that occur when aggregating hour-by-hour)
        frames = _candles_to_candles(all_1min, resample_rule)
        # Deduplication is a safety net; should be a no-op after single-pass aggregation
        frames = frames.loc[~frames.index.duplicated(keep="last")]
        if frames.empty:
            continue
        out_csv = out / f"{symbol}_{rule_label}_{side}.csv"
        out_reset = frames.reset_index().rename(columns={"index": "timestamp"})
        out_reset["timestamp"] = out_reset["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
        out_reset.to_csv(out_csv, index=False)
        log.info(f"Wrote: {out_csv} ({len(frames)} candles)")
        if side == "bid":
            out_csv_bid = out_csv
        else:
            out_csv_ask = out_csv
        if progress is not None and write_task_id is not None:
            progress.update(write_task_id, advance=1)

    return (out_csv_bid, out_csv_ask)
