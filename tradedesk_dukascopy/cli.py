import argparse, logging, sys
from datetime import datetime, timezone
from pathlib import Path

from .export import export_range
from .metadata import ExportMetadata, now_iso_utc, write_sidecar


def configure_logging(level: str = "INFO", force: bool = False) -> None:
    """
    Configure root logger with console output.

    By default, this is non-destructive: if the root logger already has handlers,
    it will do nothing (assuming the application has configured logging).

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        force: If True, clear existing handlers and force this configuration
    """
    root_logger = logging.getLogger()

    if root_logger.hasHandlers() and not force:
        return

    root_logger.setLevel(level.upper())

    if force:
        root_logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tradedesk-dc-export")
    p.add_argument("--symbol", required=True)
    p.add_argument("--from", dest="date_from", required=True, help="inclusive start date in UTC YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=True, help="inclusive end date in UTC YYYY-MM-DD")
    # later iteration
    # p.add_argument("--format", choices=["candles", "ticks"], default="candles")
    p.add_argument("--resample", default="5min", help="resample rule (candles only) - the sizing of the output candles, e.g. 5min, 1H, 1D")
    p.add_argument("--side", choices=["bid", "ask", "mid"], default="bid")
    p.add_argument("--price-divisor", type=float, default=1.0, help="only used if Dukascopy tick prices are encoded as int32; divisor applied during decode and recorded in metadata")
    p.add_argument("--cache-dir", type=Path, default=Path(".cache/marketdata"), help="Cache directory for .bi5 files (use --no-cache to disable)")
    p.add_argument("--no-cache", action="store_true", help="Disable caching .bi5 tick files and always re-download")
    p.add_argument("--probe", action="store_true", help="Probe one hour and print decoded ticks; no files written.")
    p.add_argument("--probe-ticks", type=int, default=10, help="Number of ticks to print when probing (default: 10)")
    p.add_argument("--out", required=True, help="Output directory for exported CSV and metadata files")
    p.add_argument("--log-level", choices=["fatal", "error", "warn", "info", "debug", "trace"], default="info", help="Logging level (default: info)")
    return p

def _parse_ymd(s: str) -> datetime:
    # Accept YYYY-MM-DD
    dt = datetime.strptime(s.strip(), "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    start_utc = _parse_ymd(args.date_from)
    end_utc = _parse_ymd(args.date_to)

    if end_utc < start_utc:
        raise SystemExit("--to must be >= --from")
    
    configure_logging(level=args.log_level.upper())

    # Skeleton only: we write metadata to lock in the export contract.
    out = Path(args.out)
    meta = ExportMetadata(
        schema_version="1",
        source="dukascopy",
        symbol=args.symbol,
        data_type="candles",  # data_type=args.format,  (later iteration)
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

    try:        
        output_csv = export_range(
            symbol=args.symbol,
            start_utc=start_utc,
            end_utc_inclusive=end_utc,
            # data_type=args.format,  (later iteration)
            resample_rule=args.resample,
            price_side=args.side,
            price_divisor=args.price_divisor,
            cache_dir=None if args.no_cache else args.cache_dir,
            probe=args.probe,
            probe_ticks=args.probe_ticks,
            out=out,
        )
        
        if output_csv is not None:
            write_sidecar(meta, output_csv)
            
    except KeyboardInterrupt:
        logging.getLogger(__name__).warning("Interrupted by user (Ctrl-C).")
        return 130

    return 0
