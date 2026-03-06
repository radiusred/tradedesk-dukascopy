from datetime import UTC
from pathlib import Path

import tradedesk_dukascopy.cli as cli
import tradedesk_dukascopy.parallel as par
from tradedesk_dukascopy.parallel import ExportResult


def test_parse_ymd_sets_utc_timezone() -> None:
    dt = cli._parse_ymd("2025-07-01")
    assert dt.tzinfo == UTC
    assert dt.year == 2025 and dt.month == 7 and dt.day == 1


def test_main_writes_sidecar_for_both_bid_and_ask(monkeypatch, tmp_path: Path) -> None:
    bid_csv = tmp_path / "EURUSD_5MIN_bid.csv"
    ask_csv = tmp_path / "EURUSD_5MIN_ask.csv"
    bid_csv.touch()
    ask_csv.touch()
    sidecars_written: list[Path] = []

    def fake_run_parallel_exports(tasks, max_workers):
        return [ExportResult(symbol="EURUSD", output_csvs=[bid_csv, ask_csv], success=True)]

    def fake_write_sidecar(_meta, output_csv):
        sidecars_written.append(output_csv)
        return output_csv.with_suffix(output_csv.suffix + ".meta.json")

    monkeypatch.setattr(par, "run_parallel_exports", fake_run_parallel_exports)
    monkeypatch.setattr(cli, "write_sidecar", fake_write_sidecar)

    rc = cli.main(
        [
            "--symbols",
            "EURUSD",
            "--from",
            "2025-07-01",
            "--to",
            "2025-07-01",
            "--resample",
            "5min",
            "--out",
            str(tmp_path),
            "--log-level",
            "info",
        ]
    )

    assert rc == 0
    assert len(sidecars_written) == 2
    assert bid_csv in sidecars_written
    assert ask_csv in sidecars_written


def test_main_does_not_write_sidecar_when_no_output_csvs(monkeypatch, tmp_path: Path) -> None:
    sidecars_written: list[Path] = []

    def fake_run_parallel_exports(tasks, max_workers):
        return [ExportResult(symbol="EURUSD", output_csvs=[], success=True)]

    def fake_write_sidecar(_meta, output_csv):
        sidecars_written.append(output_csv)
        return output_csv.with_suffix(output_csv.suffix + ".meta.json")

    monkeypatch.setattr(par, "run_parallel_exports", fake_run_parallel_exports)
    monkeypatch.setattr(cli, "write_sidecar", fake_write_sidecar)

    rc = cli.main(  # no --resample or --out
        [
            "--symbols",
            "EURUSD",
            "--from",
            "2025-07-01",
            "--to",
            "2025-07-01",
            "--log-level",
            "info",
        ]
    )

    assert rc == 0
    assert sidecars_written == []
