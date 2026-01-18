from datetime import timezone
from pathlib import Path

import tradedesk_dukascopy.cli as cli


def test_parse_ymd_sets_utc_timezone() -> None:
    dt = cli._parse_ymd("2025-07-01")
    assert dt.tzinfo == timezone.utc
    assert dt.year == 2025 and dt.month == 7 and dt.day == 1


def test_main_writes_sidecar_when_export_returns_path(monkeypatch, tmp_path: Path) -> None:
    out_csv = tmp_path / "EURUSD_5MIN.csv"
    called = {"sidecar": False}

    def fake_export_range(**kwargs):
        return out_csv

    def fake_write_sidecar(_meta, output_csv):
        assert output_csv == out_csv
        called["sidecar"] = True
        return out_csv.with_suffix(out_csv.suffix + ".meta.json")

    monkeypatch.setattr(cli, "export_range", fake_export_range)
    monkeypatch.setattr(cli, "write_sidecar", fake_write_sidecar)

    rc = cli.main(
        [
            "--symbols",
            "EURUSD",
            "--from",
            "2025-07-01",
            "--to",
            "2025-07-01",
            "--out",
            str(tmp_path),
            "--log-level",
            "info",
        ]
    )

    assert rc == 0
    assert called["sidecar"] is True
