"""Tests for scripts/refetch_fx_cache_corrupt_days.py — RAD-2146 remediation.

The script lives in ``scripts/`` (operator tool). We load it via importlib
and monkey-patch its module-level ``export_range`` reference so the tests
never hit Dukascopy or touch the real cache. Helpers that build daily
candle CSVs use the same zstd format the script reads.
"""
from __future__ import annotations

import importlib.util
import io
import logging
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import zstandard as zstd

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "refetch_fx_cache_corrupt_days.py"


def _load_script():
    import sys
    spec = importlib.util.spec_from_file_location(
        "refetch_fx_cache_corrupt_days", SCRIPT
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dataclass needs to resolve annotations via sys.modules
    spec.loader.exec_module(mod)
    return mod


def _write_day_csv(path: Path, close: float, n_rows: int = 5) -> None:
    """Write a daily candle CSV at *close* level (constant) zstd-compressed.

    Matches the daily candle schema the cache uses: timestamp + ohlc + volume.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-15", periods=n_rows, freq="1min", tz="UTC"),
        "open": [close] * n_rows,
        "high": [close] * n_rows,
        "low": [close] * n_rows,
        "close": [close] * n_rows,
        "volume": [1.0] * n_rows,
    })
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    cctx = zstd.ZstdCompressor()
    path.write_bytes(cctx.compress(buf.getvalue()))


def _candle_path(cache_dir: Path, symbol: str, d: date, side: str) -> Path:
    return cache_dir / symbol / f"{d.year}" / f"{d.month - 1:02d}" / f"{d.day:02d}_{side}.csv.zst"


# -----------------------------------------------------------------------------
# Config table
# -----------------------------------------------------------------------------


def test_fx_config_covers_known_universe_and_excludes_nzdusd():
    mod = _load_script()
    symbols = {c.symbol for c in mod.FX_CONFIG}
    # NZDUSD was remediated by RAD-2132 / PR #53 and must NOT be re-fetched here.
    assert "NZDUSD" not in symbols
    # Spot-check headline symbols from the issue body.
    for s in ("EURUSD", "GBPUSD", "AUDUSD", "USDJPY", "EURGBP", "AUDJPY", "EURSEK", "EURSGD"):
        assert s in symbols, f"{s} missing from FX_CONFIG"


def test_fx_config_divisors_match_tick_format():
    mod = _load_script()
    by_sym = {c.symbol: c for c in mod.FX_CONFIG}
    # 4-decimal FX → 100_000
    for s in ("EURUSD", "GBPUSD", "AUDUSD", "EURGBP", "EURSEK", "EURSGD"):
        assert by_sym[s].price_divisor == 100_000.0
    # JPY → 1_000
    for s in ("USDJPY", "AUDJPY", "GBPJPY", "CHFJPY"):
        assert by_sym[s].price_divisor == 1_000.0


def test_fx_config_envelopes_are_sane():
    mod = _load_script()
    for c in mod.FX_CONFIG:
        assert c.envelope_min < c.envelope_max
        assert c.envelope_min > 0


# -----------------------------------------------------------------------------
# Audit helpers
# -----------------------------------------------------------------------------


def test_find_corrupt_dates_flags_out_of_envelope(tmp_path):
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    sym_dir = tmp_path / "EURUSD"
    # 2024-01-15 corrupt (10000x), 2024-01-16 clean.
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "bid"), 10950.0)
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "ask"), 10950.5)
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 16), "bid"), 1.09)
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 16), "ask"), 1.091)

    corrupt = mod.find_corrupt_dates(cfg, tmp_path)

    assert corrupt == [date(2024, 1, 15)]
    assert sym_dir.exists()


def test_find_corrupt_dates_missing_symbol_dir_returns_empty(tmp_path):
    mod = _load_script()
    cfg = mod.FxScaleConfig("FOOBAR", 100_000.0, 0.30, 2.00)
    assert mod.find_corrupt_dates(cfg, tmp_path) == []


def test_find_corrupt_dates_skips_unreadable_files(tmp_path):
    """Empty/corrupt zstd files are weekends-or-IO-errors and must not be
    treated as scale corruption — matches the audit script's ``err`` bucket."""
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    p = _candle_path(tmp_path, "EURUSD", date(2024, 1, 13), "bid")  # Saturday
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"")  # unreadable as zstd
    assert mod.find_corrupt_dates(cfg, tmp_path) == []


def test_find_corrupt_dates_bid_or_ask_either_triggers(tmp_path):
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    # Bid clean, ask corrupt → still flagged.
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "bid"), 1.09)
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "ask"), 10950.5)
    assert mod.find_corrupt_dates(cfg, tmp_path) == [date(2024, 1, 15)]


# -----------------------------------------------------------------------------
# Delete + refetch
# -----------------------------------------------------------------------------


def test_delete_day_removes_both_sides_and_bi5_dir(tmp_path):
    mod = _load_script()
    d = date(2024, 1, 18)
    bid = _candle_path(tmp_path, "EURUSD", d, "bid")
    ask = _candle_path(tmp_path, "EURUSD", d, "ask")
    bi5_dir = mod._bi5_day_dir(tmp_path, "EURUSD", d)
    for p in (bid, ask):
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")
    bi5_dir.mkdir(parents=True, exist_ok=True)
    (bi5_dir / "00h_ticks.bi5").write_bytes(b"x")
    (bi5_dir / "23h_ticks.bi5").write_bytes(b"x")

    n = mod._delete_day(tmp_path, "EURUSD", d)

    assert not bid.exists() and not ask.exists() and not bi5_dir.exists()
    assert n == 4


def test_delete_day_noop_when_clean(tmp_path):
    mod = _load_script()
    d = date(2024, 1, 18)
    assert mod._delete_day(tmp_path, "EURUSD", d) == 0


def test_refetch_day_success_passes_correct_divisor(tmp_path, monkeypatch):
    mod = _load_script()
    cfg = mod.FxScaleConfig("USDJPY", 1_000.0, 20.0, 250.0)
    d = date(2024, 5, 6)
    captured = {}

    def fake_export_range(**kwargs):
        captured.update(kwargs)
        for side in ("bid", "ask"):
            p = _candle_path(tmp_path, "USDJPY", d, side)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"x")

    monkeypatch.setattr(mod, "export_range", fake_export_range)

    assert mod._refetch_day(tmp_path, cfg, d) is True
    assert captured["symbol"] == "USDJPY"
    assert captured["price_divisor"] == 1_000.0
    assert captured["resample_rule"] is None
    assert captured["cache_dir"] == tmp_path
    assert captured["start_utc"] == datetime(2024, 5, 6, tzinfo=UTC)
    assert captured["end_utc_inclusive"] == datetime(2024, 5, 6, tzinfo=UTC)


def test_refetch_day_failure_when_files_missing(tmp_path, monkeypatch):
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    monkeypatch.setattr(mod, "export_range", lambda **_: None)
    assert mod._refetch_day(tmp_path, cfg, date(2024, 5, 6)) is False


def test_refetch_day_swallows_no_data_runtimeerror_when_files_written(
    tmp_path, monkeypatch, caplog
):
    """Mirrors the NZDUSD precedent: export_range may raise 'No data produced...'
    after a successful _flush_day, in which case the script must still treat
    the day as ok if both candle files exist."""
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    d = date(2024, 5, 6)

    def fake_export_range(**_):
        for side in ("bid", "ask"):
            p = _candle_path(tmp_path, "EURUSD", d, side)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"x")
        raise RuntimeError("No data produced for EURUSD 2024-05-06")

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    with caplog.at_level(logging.DEBUG):
        assert mod._refetch_day(tmp_path, cfg, d) is True


def test_refetch_day_runtimeerror_without_files_returns_false(tmp_path, monkeypatch):
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)

    def fake_export_range(**_):
        raise RuntimeError("No data produced")

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    assert mod._refetch_day(tmp_path, cfg, date(2024, 5, 6)) is False


# -----------------------------------------------------------------------------
# remediate_symbol + main
# -----------------------------------------------------------------------------


def test_remediate_symbol_skips_clean_symbol(tmp_path, monkeypatch, caplog):
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    # All days clean → no work.
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 16), "bid"), 1.09)
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 16), "ask"), 1.091)

    called = []
    monkeypatch.setattr(mod, "export_range", lambda **k: called.append(k))

    with caplog.at_level(logging.INFO):
        ok, failures = mod.remediate_symbol(cfg, tmp_path, logging.getLogger("test"))

    assert ok == 0 and failures == []
    assert called == []  # no re-fetches issued


def test_remediate_symbol_processes_corrupt_dates(tmp_path, monkeypatch):
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    for d in (date(2024, 1, 15), date(2024, 1, 16)):
        _write_day_csv(_candle_path(tmp_path, "EURUSD", d, "bid"), 10950.0)
        _write_day_csv(_candle_path(tmp_path, "EURUSD", d, "ask"), 10950.0)

    fetched: list[date] = []

    def fake_export_range(**kwargs):
        d = kwargs["start_utc"].date()
        fetched.append(d)
        # Simulate fresh write with correct scale.
        for side in ("bid", "ask"):
            _write_day_csv(_candle_path(tmp_path, "EURUSD", d, side), 1.09)

    monkeypatch.setattr(mod, "export_range", fake_export_range)

    ok, failures = mod.remediate_symbol(cfg, tmp_path, logging.getLogger("test"))

    assert ok == 2 and failures == []
    assert fetched == [date(2024, 1, 15), date(2024, 1, 16)]


def test_remediate_symbol_deletes_all_corrupt_days_before_any_refetch(tmp_path, monkeypatch):
    """RAD-2146 hotfix: pass 1 must wipe every corrupt day before pass 2 starts,
    so the RAD-1920 write-time scale sentry has no mis-scaled neighbours to
    compare a freshly-fetched (correct-scale) day against. Without this, 100%-
    systemic corruption triggers the sentry on every refetch and rejects the
    fix.
    """
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    corrupt = [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 17)]
    for d in corrupt:
        _write_day_csv(_candle_path(tmp_path, "EURUSD", d, "bid"), 10950.0)
        _write_day_csv(_candle_path(tmp_path, "EURUSD", d, "ask"), 10950.0)

    # Record the cache state seen at the start of each export_range call.
    snapshots: list[list[date]] = []

    def fake_export_range(**kwargs):
        snapshots.append(sorted(
            mod._parse_day(p)
            for p in mod._iter_day_files(tmp_path / "EURUSD", "bid")
        ))
        d = kwargs["start_utc"].date()
        for side in ("bid", "ask"):
            _write_day_csv(_candle_path(tmp_path, "EURUSD", d, side), 1.09)

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    ok, failures = mod.remediate_symbol(cfg, tmp_path, logging.getLogger("test"))

    assert ok == 3 and failures == []
    # First refetch must see an empty cache (all 3 corrupt days deleted up-front).
    assert snapshots[0] == []
    # Subsequent refetches see only the already-corrected neighbour days.
    assert snapshots[1] == [date(2024, 1, 15)]
    assert snapshots[2] == [date(2024, 1, 15), date(2024, 1, 16)]


def test_remediate_symbol_records_failures(tmp_path, monkeypatch):
    mod = _load_script()
    cfg = mod.FxScaleConfig("EURUSD", 100_000.0, 0.30, 2.00)
    for d in (date(2024, 1, 15), date(2024, 1, 16)):
        _write_day_csv(_candle_path(tmp_path, "EURUSD", d, "bid"), 10950.0)
        _write_day_csv(_candle_path(tmp_path, "EURUSD", d, "ask"), 10950.0)

    def fake_export_range(**kwargs):
        d = kwargs["start_utc"].date()
        if d == date(2024, 1, 15):
            for side in ("bid", "ask"):
                _write_day_csv(_candle_path(tmp_path, "EURUSD", d, side), 1.09)
        # Jan 16 → no files written → counts as failure.

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    ok, failures = mod.remediate_symbol(cfg, tmp_path, logging.getLogger("test"))
    assert ok == 1
    assert failures == [date(2024, 1, 16)]


def test_main_dry_run_does_not_call_export_range(tmp_path, monkeypatch):
    mod = _load_script()
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "bid"), 10950.0)
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "ask"), 10950.0)

    called = []
    monkeypatch.setattr(mod, "export_range", lambda **k: called.append(k))

    rc = mod.main(["--cache-dir", str(tmp_path), "--symbols", "EURUSD", "--dry-run"])
    assert rc == 0
    assert called == []


def test_main_unknown_symbol_returns_two(tmp_path):
    mod = _load_script()
    rc = mod.main(["--cache-dir", str(tmp_path), "--symbols", "ZZZZZZ"])
    assert rc == 2


def test_main_failure_returns_one(tmp_path, monkeypatch):
    mod = _load_script()
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "bid"), 10950.0)
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "ask"), 10950.0)
    monkeypatch.setattr(mod, "export_range", lambda **_: None)
    rc = mod.main(["--cache-dir", str(tmp_path), "--symbols", "EURUSD"])
    assert rc == 1


def test_main_success_returns_zero(tmp_path, monkeypatch):
    mod = _load_script()
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "bid"), 10950.0)
    _write_day_csv(_candle_path(tmp_path, "EURUSD", date(2024, 1, 15), "ask"), 10950.0)

    def fake_export_range(**kwargs):
        d = kwargs["start_utc"].date()
        for side in ("bid", "ask"):
            _write_day_csv(_candle_path(tmp_path, "EURUSD", d, side), 1.09)

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    rc = mod.main(["--cache-dir", str(tmp_path), "--symbols", "EURUSD"])
    assert rc == 0
