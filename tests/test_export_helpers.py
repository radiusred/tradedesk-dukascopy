import lzma
import struct
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

import tradedesk_dukascopy.export as ex


def _compress_records(raw: bytes) -> bytes:
    return lzma.compress(raw)


def test_symbol_normalise_empty_raises() -> None:
    with pytest.raises(ValueError, match="Empty symbol"):
        ex._symbol_normalise("   ")


def test_symbol_normalise_removes_separators_and_uppercases() -> None:
    assert ex._symbol_normalise("usa500.idx/usd") == "USA500IDXUSD"
    assert ex._symbol_normalise(" EURUSD ") == "EURUSD"


def test_iter_hours_includes_hour_when_start_on_boundary() -> None:
    start = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    end_excl = datetime(2025, 1, 1, 3, 0, tzinfo=UTC)
    got = list(ex._iter_hours(start, end_excl))
    assert got == [
        datetime(2025, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 1, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 2, 0, tzinfo=UTC),
    ]


def test_iter_hours_rounds_up_to_next_hour_when_start_not_on_boundary() -> None:
    start = datetime(2025, 1, 1, 0, 30, tzinfo=UTC)
    end_excl = datetime(2025, 1, 1, 2, 0, tzinfo=UTC)
    got = list(ex._iter_hours(start, end_excl))
    assert got == [
        datetime(2025, 1, 1, 1, 0, tzinfo=UTC),
    ]


def test_dukascopy_tick_url_month_is_zero_based() -> None:
    # June is month 6 => zero-based "05"
    t = datetime(2025, 6, 1, 0, 0, tzinfo=UTC)
    url = ex._dukascopy_tick_url("EURUSD", t)
    assert url.startswith(ex.BASE_URL)
    assert "/EURUSD/2025/05/01/00h_ticks.bi5" in url


def test_probe_price_format_returns_float_for_plausible_float_prices() -> None:
    # float layout: >i f f f f  (ms, ask, bid, ask_vol, bid_vol)
    raw = struct.pack(">i f f f f", 0, 1.2345, 1.2340, 10.0, 12.0)
    comp = _compress_records(raw)
    assert ex._probe_price_format(comp) == "float"


def test_probe_price_format_returns_int_when_float_decode_is_tiny() -> None:
    # int layout (what we actually want to detect): >i i i f f
    # When these int32 bytes are interpreted as float32, they commonly become tiny values.
    raw = struct.pack(">i i i f f", 0, 100000, 99999, 10.0, 12.0)
    comp = _compress_records(raw)
    assert ex._probe_price_format(comp) == "int"


def test_read_n_tick_records_reads_exact_number_of_records() -> None:
    rec1 = struct.pack(">i f f f f", 0, 1.0, 2.0, 3.0, 4.0)
    rec2 = struct.pack(">i f f f f", 1, 1.1, 2.1, 3.1, 4.1)
    rec3 = struct.pack(">i f f f f", 2, 1.2, 2.2, 3.2, 4.2)
    raw = rec1 + rec2 + rec3
    comp = _compress_records(raw)

    out = ex._read_n_tick_records(comp, 2)
    assert out == raw[: 20 * 2]


def test_ticks_to_candles_empty_returns_empty_frame_with_columns() -> None:
    df = ex._ticks_to_candles([], resample_rule="1min", price_side="bid")
    assert list(df.columns) == ["open", "high", "low", "close", "volume"]
    assert df.empty


def test_ticks_to_candles_bid_ohlcv_resample() -> None:
    base = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    ticks = [
        ex.Tick(
            ts=base + timedelta(seconds=10),
            bid=100.0,
            ask=101.0,
            bid_vol=1.0,
            ask_vol=2.0,
        ),
        ex.Tick(
            ts=base + timedelta(seconds=50),
            bid=102.0,
            ask=103.0,
            bid_vol=3.0,
            ask_vol=4.0,
        ),
        ex.Tick(
            ts=base + timedelta(minutes=1, seconds=5),
            bid=101.0,
            ask=102.0,
            bid_vol=5.0,
            ask_vol=6.0,
        ),
    ]

    df = ex._ticks_to_candles(ticks, resample_rule="1min", price_side="bid")

    # Minute 00:00
    first = df.loc[pd.Timestamp("2025-01-01T00:00:00Z")]
    assert first["open"] == 100.0
    assert first["high"] == 102.0
    assert first["low"] == 100.0
    assert first["close"] == 102.0
    assert first["volume"] == 1.0 + 3.0

    # Minute 00:01
    second = df.loc[pd.Timestamp("2025-01-01T00:01:00Z")]
    assert second["open"] == 101.0
    assert second["high"] == 101.0
    assert second["low"] == 101.0
    assert second["close"] == 101.0
    assert second["volume"] == 5.0


def test_ticks_to_candles_mid_price_and_mid_volume() -> None:
    base = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    ticks = [
        ex.Tick(ts=base + timedelta(seconds=1), bid=100.0, ask=102.0, bid_vol=2.0, ask_vol=4.0),
        ex.Tick(ts=base + timedelta(seconds=2), bid=101.0, ask=103.0, bid_vol=6.0, ask_vol=8.0),
    ]

    df = ex._ticks_to_candles(ticks, resample_rule="1min", price_side="mid")
    row = df.iloc[0]

    # mid prices are (bid+ask)/2: 101.0 then 102.0
    assert row["open"] == 101.0
    assert row["high"] == 102.0
    assert row["low"] == 101.0
    assert row["close"] == 102.0

    # mid volume is (bid_vol+ask_vol)/2: 3.0 then 7.0 => 10.0
    assert row["volume"] == (2.0 + 4.0) / 2.0 + (6.0 + 8.0) / 2.0


def test_ticks_to_candles_invalid_price_side_raises() -> None:
    base = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
    ticks = [ex.Tick(ts=base, bid=1.0, ask=2.0, bid_vol=1.0, ask_vol=1.0)]

    with pytest.raises(ValueError, match="price_side must be one of"):
        ex._ticks_to_candles(ticks, resample_rule="1min", price_side="nope")
