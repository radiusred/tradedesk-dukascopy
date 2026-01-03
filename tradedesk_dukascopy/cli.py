import argparse
from pathlib import Path

from .metadata import ExportMetadata, now_iso_utc, write_sidecar


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tradedesk-dc-export")
    p.add_argument("--symbol", required=True)
    p.add_argument("--from", dest="date_from", required=True, help="YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=True, help="YYYY-MM-DD")
    p.add_argument("--format", choices=["candles", "ticks"], default="candles")
    p.add_argument("--resample", default="5min", help="pandas resample rule (candles only)")
    p.add_argument("--side", choices=["bid", "ask", "mid"], default="bid")
    p.add_argument("--price-divisor", type=float, default=1.0)
    p.add_argument("--out", required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    # Skeleton only: we write metadata to lock in the export contract.
    out = Path(args.out)
    meta = ExportMetadata(
        schema_version="1",
        source="dukascopy",
        symbol=args.symbol,
        data_type=args.format,
        timestamp_format="iso8601_utc",
        price_divisor=float(args.price_divisor),
        generated_at=now_iso_utc(),
        params={
            "date_from": args.date_from,
            "date_to": args.date_to,
            "resample": args.resample,
            "side": args.side,
        },
    )
    write_sidecar(meta, out)

    # Real export implementation to be added next.
    raise SystemExit("Not implemented: exporter logic not wired yet (skeleton project).")
