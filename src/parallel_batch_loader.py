"""
src/parallel_batch_loader.py
────────────────────────────
Loads two log files (Batch 1 = July, Batch 2 = August) in parallel
using threads. Each file is treated as exactly one batch, matching the
project requirement: "data should be loaded in 2 batches, loaded parallelly."

Usage
-----
    from src.parallel_batch_loader import load_files_parallel

    results = load_files_parallel(
        file_paths   = ["data/NASA_access_log_Jul95",
                        "data/NASA_access_log_Aug95"],
        batch_size   = 5000,
        process_fn   = my_callback,   # called for every parsed record list
    )

    # results is a dict with merged counters and per-batch stats.

Alternatively, call `load_files_parallel_raw` to get raw record lists
back from each batch without a callback (used by Pig/Hive/MapReduce
pipelines that need all records in memory before sending to Hadoop).
"""

from __future__ import annotations

import threading
from typing import Callable, Dict, List, Optional

from parser import parse_file_in_batches


# ─── per-batch result container ──────────────────────────────────────────────

class BatchResult:
    def __init__(self, batch_number: int, file_path: str):
        self.batch_number   = batch_number
        self.file_path      = file_path
        self.records: List[dict] = []
        self.total_records  = 0
        self.malformed      = 0
        self.sub_batch_count = 0   # how many parse_file_in_batches chunks
        self.error: Optional[str] = None


# ─── worker ──────────────────────────────────────────────────────────────────

def _load_single_file(
    file_path:    str,
    batch_number: int,
    batch_size:   int,
    process_fn:   Optional[Callable[[int, List[dict], int], None]],
    result:       BatchResult,
    lock:         threading.Lock,
):
    """
    Reads one file in its own thread.
    If process_fn is provided, calls it for each chunk of records
    (useful for streaming inserts, e.g. MongoDB).
    Otherwise accumulates everything in result.records.
    """
    try:
        for sub_id, records, malformed in parse_file_in_batches(
            file_path, batch_size
        ):
            result.sub_batch_count += 1
            result.total_records   += len(records)
            result.malformed       += malformed

            if process_fn:
                with lock:
                    process_fn(batch_number, records, malformed)
            else:
                result.records.extend(records)

            print(
                f"  [Batch {batch_number} | {file_path}] "
                f"chunk {sub_id}: {len(records)} records, "
                f"{malformed} malformed"
            )

    except Exception as exc:
        result.error = str(exc)
        print(f"  ✗ Error loading {file_path}: {exc}")


# ─── public API ──────────────────────────────────────────────────────────────

def load_files_parallel(
    file_paths: List[str],
    batch_size: int,
    process_fn: Optional[Callable[[int, List[dict], int], None]] = None,
) -> Dict[int, BatchResult]:
    """
    Load all files in parallel (one thread per file).
    file_paths[0] → Batch 1, file_paths[1] → Batch 2, etc.

    Parameters
    ----------
    file_paths  : list of local filesystem paths
    batch_size  : records per internal parse chunk
    process_fn  : optional callback(batch_number, records, malformed)
                  called under a lock for thread safety.
                  If None, records are accumulated in BatchResult.records.

    Returns
    -------
    dict mapping batch_number (1-based) → BatchResult
    """
    results: Dict[int, BatchResult] = {}
    threads: List[threading.Thread] = []
    lock = threading.Lock()

    for i, path in enumerate(file_paths, start=1):
        br = BatchResult(batch_number=i, file_path=path)
        results[i] = br
        t = threading.Thread(
            target=_load_single_file,
            args=(path, i, batch_size, process_fn, br, lock),
            name=f"BatchLoader-{i}",
            daemon=True,
        )
        threads.append(t)

    print(f"\n  ▶ Starting {len(threads)} parallel batch loaders…")
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"  ✓ All {len(threads)} batches loaded.\n")

    return results


def merge_batch_results(results: Dict[int, BatchResult]):
    """
    Convenience: flatten all records from every batch into one list
    and compute aggregate counters.

    Returns (all_records, total_records, total_malformed, batch_count)
    """
    all_records   = []
    total_records = 0
    total_malformed = 0

    for br in results.values():
        all_records.extend(br.records)
        total_records   += br.total_records
        total_malformed += br.malformed

    return all_records, total_records, total_malformed, len(results)