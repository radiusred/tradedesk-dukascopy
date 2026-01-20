"""Parallel execution for multi-symbol exports."""

import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

log = logging.getLogger(__name__)
_cancellation_event = threading.Event()

@dataclass
class ExportTask:
    """Configuration for a single symbol export."""
    symbol: str
    start_utc: datetime
    end_utc_inclusive: datetime
    resample_rule: str
    price_side: str
    price_divisor: float
    cache_dir: Path | None
    out: Path


@dataclass
class ExportResult:
    """Result from exporting a single symbol."""
    symbol: str
    output_csv: Path | None
    success: bool
    error: str | None = None


def _export_worker(task: ExportTask, progress: Progress | None = None) -> ExportResult:
    """Worker function to export a single symbol."""
    from tradedesk_dukascopy.export import export_range
    
    try:
        output_csv = export_range(
            symbol=task.symbol,
            start_utc=task.start_utc,
            end_utc_inclusive=task.end_utc_inclusive,
            resample_rule=task.resample_rule,
            price_side=task.price_side,
            price_divisor=task.price_divisor,
            cache_dir=task.cache_dir,
            probe=False,
            probe_ticks=0,
            out=task.out,
            progress=progress,
        )
        return ExportResult(symbol=task.symbol, output_csv=output_csv, success=True)
    
    except Exception as e:
        log.exception(f"Failed to export {task.symbol}")
        return ExportResult(symbol=task.symbol, output_csv=None, success=False, error=str(e))

def run_parallel_exports(
    tasks: list[ExportTask],
    max_workers: int,
) -> list[ExportResult]:
    """Execute exports in parallel."""
    total = len(tasks)
    results = []
    use_rich = sys.stdout.isatty()
    
    _cancellation_event.clear()
    
    if not use_rich:
        log.info(f"Starting export of {total} symbols with {max_workers} workers")
    
    progress_ctx = Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.fields[symbol]}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
    ) if use_rich else nullcontext()
    
    executor = ThreadPoolExecutor(max_workers=max_workers)
    
    try:
        with progress_ctx as progress:
            futures = {
                executor.submit(_export_worker, task, progress if use_rich else None): task 
                for task in tasks
            }
            
            completed = 0
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                completed += 1
                
                if result.success:
                    if not use_rich:
                        log.info(f"[{completed}/{total}] ✓ {result.symbol} complete")
                else:
                    prefix = f"[{completed}/{total}] " if not use_rich else ""
                    log.error(f"{prefix}✗ {result.symbol} failed: {result.error}")
                    
    except KeyboardInterrupt:
        _cancellation_event.set()
        log.warning("Interrupted - cancelling in-progress downloads: this can take some time to complete...")
        executor.shutdown(wait=False, cancel_futures=True)
        raise

    finally:
        if not _cancellation_event.is_set():
            executor.shutdown(wait=True)
        else:
            executor.shutdown(wait=False, cancel_futures=True)
    
    return results
