"""Parallel execution for multi-symbol exports."""

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)


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


def _export_worker(task: ExportTask) -> ExportResult:
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
        )
        return ExportResult(symbol=task.symbol, output_csv=output_csv, success=True)
    
    except Exception as e:
        log.exception(f"Failed to export {task.symbol}")
        return ExportResult(symbol=task.symbol, output_csv=None, success=False, error=str(e))


def run_parallel_exports(
    tasks: list[ExportTask],
    max_workers: int,
) -> list[ExportResult]:
    """
    Execute exports in parallel.
    
    Simple concurrent execution with periodic status updates.
    """
    total = len(tasks)
    results = []
    completed = 0
    
    log.info(f"Starting parallel export of {total} symbols with {max_workers} workers")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_export_worker, task): task for task in tasks}
        
        for future in as_completed(futures):
            task = futures[future]
            result = future.result()
            results.append(result)
            completed += 1
            
            if result.success:
                log.info(f"[{completed}/{total}] ✓ {result.symbol} complete")
            else:
                log.error(f"[{completed}/{total}] ✗ {result.symbol} failed: {result.error}")
    
    return results
