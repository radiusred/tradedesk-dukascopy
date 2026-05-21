"""Tests for scripts/refetch_nzdusd_corrupt_days.py — RAD-2132 remediation.

The script lives in ``scripts/`` (operator tool, not a public module). We load
it via importlib and monkey-patch its module-level ``export_range`` reference
so the tests never hit Dukascopy or touch the real cache.
"""
from __future__ import annotations

import importlib.util
import logging
from datetime import UTC, date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "refetch_nzdusd_corrupt_days.py"


def _load_script(tmp_cache: Path):
    """Fresh import per test, with CACHE_DIR rebound to a tmp dir."""
    spec = importlib.util.spec_from_file_location(
        "refetch_nzdusd_corrupt_days", SCRIPT
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.CACHE_DIR = tmp_cache
    return mod


def _touch(p: Path, content: bytes = b"x") -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)


def test_delete_day_removes_both_sides_and_bi5_dir(tmp_path):
    mod = _load_script(tmp_path)
    d = date(2024, 1, 18)
    bid = mod._candle_path(d, "bid")
    ask = mod._candle_path(d, "ask")
    bi5_dir = mod._bi5_day_dir(d)
    _touch(bid)
    _touch(ask)
    _touch(bi5_dir / "00h_ticks.bi5")
    _touch(bi5_dir / "23h_ticks.bi5")

    n = mod._delete_day(d)

    assert not bid.exists()
    assert not ask.exists()
    assert not bi5_dir.exists()  # rmdir succeeded after children removed
    assert n == 4  # bid + ask + 2 bi5 files


def test_delete_day_is_noop_when_clean(tmp_path):
    """Idempotency: a day with no cached files removes nothing and returns 0."""
    mod = _load_script(tmp_path)
    d = date(2024, 1, 18)
    assert mod._delete_day(d) == 0
    assert not mod._candle_path(d, "bid").exists()


def test_delete_day_partial_cache(tmp_path):
    """Only bid present — ask absence should not raise."""
    mod = _load_script(tmp_path)
    d = date(2024, 3, 7)
    _touch(mod._candle_path(d, "bid"))

    n = mod._delete_day(d)

    assert n == 1
    assert not mod._candle_path(d, "bid").exists()


def test_refetch_day_success_when_export_writes_both_sides(tmp_path, monkeypatch):
    mod = _load_script(tmp_path)
    d = date(2024, 5, 6)

    captured = {}

    def fake_export_range(**kwargs):
        captured.update(kwargs)
        _touch(mod._candle_path(d, "bid"))
        _touch(mod._candle_path(d, "ask"))

    monkeypatch.setattr(mod, "export_range", fake_export_range)

    assert mod._refetch_day(d) is True
    assert captured["symbol"] == "NZDUSD"
    assert captured["price_divisor"] == mod.PRICE_DIVISOR == 100_000.0
    assert captured["resample_rule"] is None
    assert captured["cache_dir"] == tmp_path
    assert captured["start_utc"] == datetime(2024, 5, 6, tzinfo=UTC)
    assert captured["end_utc_inclusive"] == datetime(2024, 5, 6, tzinfo=UTC)


def test_refetch_day_failure_when_files_missing(tmp_path, monkeypatch):
    mod = _load_script(tmp_path)
    d = date(2024, 5, 6)
    monkeypatch.setattr(mod, "export_range", lambda **_: None)
    assert mod._refetch_day(d) is False


def test_refetch_day_swallows_no_data_runtimeerror(tmp_path, monkeypatch, caplog):
    """export_range raises 'No data produced...' when resample_rule=None and
    no 1-min frames accumulate; daily candle may still have been flushed.
    The script must not crash — it checks file existence instead."""
    mod = _load_script(tmp_path)
    d = date(2024, 5, 6)

    def fake_export_range(**_):
        # Simulate daily-cache flush happening before the RuntimeError.
        _touch(mod._candle_path(d, "bid"))
        _touch(mod._candle_path(d, "ask"))
        raise RuntimeError("No data produced for NZDUSD 2024-05-06")

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    with caplog.at_level(logging.DEBUG):
        assert mod._refetch_day(d) is True


def test_refetch_day_runtimeerror_without_files_returns_false(tmp_path, monkeypatch):
    """If export_range raises AND no files were written, the day failed."""
    mod = _load_script(tmp_path)
    d = date(2024, 5, 6)

    def fake_export_range(**_):
        raise RuntimeError("No data produced")

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    assert mod._refetch_day(d) is False


def test_main_all_success_returns_zero(tmp_path, monkeypatch):
    mod = _load_script(tmp_path)

    def fake_export_range(*, start_utc, **_):
        d = start_utc.date()
        _touch(mod._candle_path(d, "bid"))
        _touch(mod._candle_path(d, "ask"))

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    monkeypatch.setattr(mod, "CORRUPT_DATES", [date(2024, 1, 18), date(2024, 3, 7)])

    assert mod.main() == 0


def test_main_failure_returns_one_and_logs(tmp_path, monkeypatch, caplog):
    mod = _load_script(tmp_path)

    def fake_export_range(*, start_utc, **_):
        d = start_utc.date()
        # First date succeeds, second leaves files missing → failure.
        if d == date(2024, 1, 18):
            _touch(mod._candle_path(d, "bid"))
            _touch(mod._candle_path(d, "ask"))

    monkeypatch.setattr(mod, "export_range", fake_export_range)
    monkeypatch.setattr(mod, "CORRUPT_DATES", [date(2024, 1, 18), date(2024, 3, 7)])

    with caplog.at_level(logging.ERROR):
        rc = mod.main()

    assert rc == 1
    assert any("REFETCH FAILED" in r.message for r in caplog.records)
    assert any("2024-03-07" in r.message for r in caplog.records)


def test_corrupt_dates_list_matches_audit_envelope():
    """The 45-day list is the contract with RAD-2132 audit output. If the
    audit re-scans and finds more/fewer, the constant must be updated and
    this assertion forces the reviewer to look."""
    mod = _load_script(Path("/tmp"))
    assert len(mod.CORRUPT_DATES) == 45
    assert mod.CORRUPT_DATES == sorted(mod.CORRUPT_DATES)
    assert len(set(mod.CORRUPT_DATES)) == 45  # no duplicates
