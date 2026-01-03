from datetime import datetime, timezone
import pytest

import tradedesk_dukascopy.export as ex


def test_ticks_to_candles_basic_ohlc_and_volume() -> None:
    # Two ticks in the same 5-min bucket, one in the next
    ticks = [
        ex.Tick(ts=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc), bid=1.0, ask=1.2, bid_vol=2.0, ask_vol=3.0),
        ex.Tick(ts=datetime(2025, 1, 1, 0, 4, 59, tzinfo=timezone.utc), bid=1.1, ask=1.3, bid_vol=5.0, ask_vol=7.0),
        ex.Tick(ts=datetime(2025, 1, 1, 0, 5, 0, tzinfo=timezone.utc), bid=0.9, ask=1.0, bid_vol=11.0, ask_vol=13.0),
    ]

    df = ex._ticks_to_candles(ticks, resample_rule="5min", price_side="bid")

    # Two buckets: 00:00 and 00:05
    assert len(df) == 2

    first = df.iloc[0]
    assert first["open"] == 1.0
    assert first["high"] == 1.1
    assert first["low"] == 1.0
    assert first["close"] == 1.1
    assert first["volume"] == 2.0 + 5.0  # bid_vol

    second = df.iloc[1]
    assert second["open"] == 0.9
    assert second["high"] == 0.9
    assert second["low"] == 0.9
    assert second["close"] == 0.9
    assert second["volume"] == 11.0


def test_ticks_to_candles_mid_price_and_mid_volume() -> None:
    ticks = [
        ex.Tick(ts=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc), bid=1.0, ask=1.2, bid_vol=2.0, ask_vol=6.0),
        ex.Tick(ts=datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc), bid=1.1, ask=1.3, bid_vol=4.0, ask_vol=10.0),
    ]

    df = ex._ticks_to_candles(ticks, resample_rule="5min", price_side="mid")
    row = df.iloc[0]

    # mid prices: (bid+ask)/2 => 1.1 then 1.2
    assert row["open"] == pytest.approx(1.1)
    assert row["close"] == pytest.approx(1.2)
    assert row["high"] == pytest.approx(1.2)
    assert row["low"] == pytest.approx(1.1)

    assert row["volume"] == pytest.approx(((2.0 + 6.0) / 2) + ((4.0 + 10.0) / 2))
