import argparse, logging, sys, os
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
    p.add_argument("--symbols", nargs="+", required=True, metavar="SYMBOL", help="One or more symbols to export (e.g., EURUSD GBPUSD)")
    p.add_argument("--from", dest="date_from", required=True, help="inclusive start date in UTC YYYY-MM-DD")
    p.add_argument("--to", dest="date_to", required=True, help="inclusive end date in UTC YYYY-MM-DD")
    p.add_argument("--resample", default="5min", help="resample rule (candles only) - the sizing of the output candles, e.g. 5min, 1H, 1D")
    p.add_argument("--side", choices=["bid", "ask", "mid"], default="bid")
    p.add_argument("--price-divisor", type=float, default=1.0, help="only used if Dukascopy tick prices are encoded as int32; divisor applied during decode and recorded in metadata")
    p.add_argument("--cache-dir", type=Path, default=Path(".cache/marketdata"), help="Cache directory for .bi5 files (use --no-cache to disable)")
    p.add_argument("--no-cache", action="store_true", help="Disable caching .bi5 tick files and always re-download")
    p.add_argument("--workers", type=int, default=None, help="Max parallel instrument workers (default: CPU count - 2, min 1)")
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

    # Determine worker count
    if args.workers is None:
        workers = max(1, (os.cpu_count() or 2) - 2)
    else:
        workers = max(1, args.workers)
    
    log = logging.getLogger(__name__)
    log.info(f"Processing {len(args.symbols)} symbols with up to {workers} workers")

    # Handle probe mode (single symbol, single-threaded)
    if args.probe:
        if len(args.symbols) > 1:
            raise SystemExit("--probe mode only supports a single symbol")
        
        symbol = args.symbols[0]
        out = Path(args.out)
        
        try:
            export_range(
                symbol=symbol,
                start_utc=start_utc,
                end_utc_inclusive=end_utc,
                resample_rule=args.resample,
                price_side=args.side,
                price_divisor=args.price_divisor,
                cache_dir=None if args.no_cache else args.cache_dir,
                probe=True,
                probe_ticks=args.probe_ticks,
                out=out,
            )
        except KeyboardInterrupt:
            log.warning("Interrupted by user (Ctrl-C).")
            return 130
        
        return 0

    # Build export tasks
    from tradedesk_dukascopy.parallel import ExportTask, run_parallel_exports
    
    out = Path(args.out)
    cache_dir = None if args.no_cache else args.cache_dir
    
    tasks = [
        ExportTask(
            symbol=symbol,
            start_utc=start_utc,
            end_utc_inclusive=end_utc,
            resample_rule=args.resample,
            price_side=args.side,
            price_divisor=args.price_divisor,
            cache_dir=cache_dir,
            out=out,
        )
        for symbol in args.symbols
    ]

    try:
        results = run_parallel_exports(tasks, max_workers=workers)
        
        # Write metadata for successful exports
        for result in results:
            if result.success and result.output_csv is not None:
                meta = ExportMetadata(
                    schema_version="1",
                    source="dukascopy",
                    symbol=result.symbol,
                    data_type="candles",
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
                write_sidecar(meta, result.output_csv)
        
        # Summary
        succeeded = sum(1 for r in results if r.success)
        failed = len(results) - succeeded
        
        if failed > 0:
            log.warning(f"Completed: {succeeded} succeeded, {failed} failed")
            failed_symbols = [r.symbol for r in results if not r.success]
            log.warning(f"Failed symbols: {', '.join(failed_symbols)}")
            return 1
        else:
            log.info(f"All {succeeded} symbols exported successfully")
            return 0
            
    except KeyboardInterrupt:
        log.warning("Interrupted by user (Ctrl-C).")
        return 130
    