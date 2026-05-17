"""
pipelines/pig_pipeline.py
────────────────────────────────────────────────────────────────────
Apache Pig ETL pipeline.

Strategy
────────
• Both data files (July + August) are uploaded to HDFS in parallel
  threads — one thread per file — fulfilling "2 batches loaded
  parallelly".
• A single Pig Latin script (pig_scripts/etl_queries.pig) is executed
  via `pig -x mapreduce`.  The script receives both HDFS paths as
  parameters so Pig itself handles all parsing, cleaning, filtering,
  grouping, and aggregation — no Python substitution.
• Pig produces three output directories on HDFS (one per query).
  The Python layer downloads those outputs, parses the TSV lines,
  post-processes Q2 ranking, and stores everything in PostgreSQL
  via DBManager — identical to what MapReducePipeline does.
"""

import json
import os
import subprocess
import threading
import time
from datetime import datetime

from src.base_pipeline import BasePipeline
from src.parallel_batch_loader import load_files_parallel, merge_batch_results
from config import (
    HDFS_BIN, PIG_BIN,
    HDFS_INPUT, HDFS_OUTPUT,
    PIG_SCRIPTS_DIR,
)


# ─── HDFS helpers (same pattern as MapReducePipeline) ─────────────────────────

def _hdfs(*args) -> subprocess.CompletedProcess:
    cmd = [HDFS_BIN, "dfs"] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


def _upload_file_to_hdfs(
    local_path: str,
    hdfs_path: str,
    batch_number: int,
    results: dict,
    lock: threading.Lock,
):
    """Thread worker: upload one local file to HDFS (mirrors MapReduce impl)."""
    try:
        print(f"  [Batch {batch_number}] Uploading {local_path} → {hdfs_path}")
        r = _hdfs("-put", "-f", local_path, hdfs_path)
        ok = r.returncode == 0
        with lock:
            results[batch_number] = {
                "ok":   ok,
                "path": hdfs_path,
                "err":  r.stderr if not ok else "",
            }
        status = "✓" if ok else "✗"
        print(
            f"  {status} Batch {batch_number} upload "
            f"{'done' if ok else 'FAILED'}: {r.stderr or ''}"
        )
    except Exception as exc:
        with lock:
            results[batch_number] = {"ok": False, "path": hdfs_path, "err": str(exc)}
        print(f"  ✗ Batch {batch_number} upload exception: {exc}")


# ─── Pipeline ─────────────────────────────────────────────────────────────────

class PigPipeline(BasePipeline):

    def __init__(self, data_file, batch_size: int = 5000, db_type: str = "postgresql"):
        super().__init__("Pig", data_file, batch_size, db_type)
        self.data_files = data_file if isinstance(data_file, list) else [data_file]

    # ── main execute ──────────────────────────────────────────────────────────

    def execute(self) -> bool:
        print(f"\n{'='*60}")
        print(f"Executing Apache Pig Pipeline")
        print(f"Files : {self.data_files}")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            return False

        try:
            execution_timestamp = datetime.now()

            # ── Step 1: upload both files to HDFS in parallel ─────────────────
            print("Step 1: Uploading input files to HDFS in parallel…")
            self._prepare_hdfs_input()
            upload_results = self._parallel_upload_to_hdfs()
            if any(not v["ok"] for v in upload_results.values()):
                print("✗ One or more HDFS uploads failed. Aborting.")
                return False

            # batch_count = number of files (= number of HDFS uploads)
            self.batch_count = len(self.data_files)

            # Quick local scan to obtain total_records / malformed counts.
            # This is a lightweight read — actual ETL happens inside Pig.
            print("\n  Counting records locally for metadata…")
            self._count_records_locally()

            # ── Step 2: run the Pig script ────────────────────────────────────
            print("\nStep 2: Submitting Apache Pig job…")
            hdfs_out_base = f"{HDFS_OUTPUT}/pig_{self.run_id}"
            ok = self._run_pig_script(hdfs_out_base)
            if not ok:
                print("✗ Pig job failed.")
                return False

            # ── Step 3: collect TSV output from HDFS ─────────────────────────
            print("\nStep 3: Collecting Pig output from HDFS…")
            q1_rows, q2_rows, q3_rows = self._collect_results(hdfs_out_base)

            # ── Step 4: post-process Q2 ranking ──────────────────────────────
            # Pig already sorts/limits to top-20, but we assign rank here
            # (same as MapReducePipeline) to keep the reporting layer uniform.
            q2_rows.sort(key=lambda r: r["request_count"], reverse=True)
            q2_rows = q2_rows[:20]
            for rank, row in enumerate(q2_rows, 1):
                row["rank"] = rank

            q1_rows.sort(key=lambda r: (r["log_date"], r["status_code"]))
            q3_rows.sort(key=lambda r: (r["log_date"], r["log_hour"]))

            # ── Step 5: store in PostgreSQL ───────────────────────────────────
            print("\nStep 4: Storing results in PostgreSQL…")
            elapsed_ms = int((time.time() - self.start_time) * 1000)
            self.db_manager.insert_parent_result(
                self.run_id, self.pipeline_name,
                self.batch_count, self.batch_size,
                self.total_records, self.malformed_records,
                elapsed_ms, execution_timestamp,
                extra_json={"mode": "pig_mapreduce"},
            )
            self.db_manager.insert_daily_traffic_results(
                q1_rows, self.run_id, self.pipeline_name, 1, execution_timestamp)
            self.db_manager.insert_top_resources_results(
                q2_rows, self.run_id, self.pipeline_name, 1, execution_timestamp)
            self.db_manager.insert_error_analysis_results(
                q3_rows, self.run_id, self.pipeline_name, 1, execution_timestamp)

            self.end_time = time.time()
            self.save_metadata()
            print(self.get_status_string())
            return True

        except Exception as exc:
            print(f"✗ Pig pipeline error: {exc}")
            import traceback; traceback.print_exc()
            return False
        finally:
            self.disconnect_db()

    # ── HDFS setup ────────────────────────────────────────────────────────────

    def _prepare_hdfs_input(self):
        _hdfs("-rm", "-r", "-f", f"{HDFS_INPUT}/{self.run_id}")
        _hdfs("-mkdir", "-p", f"{HDFS_INPUT}/{self.run_id}")

    def _parallel_upload_to_hdfs(self) -> dict:
        """Upload July and August files simultaneously (one thread each)."""
        results: dict = {}
        lock = threading.Lock()
        threads = []
        for i, local_path in enumerate(self.data_files, start=1):
            filename  = os.path.basename(local_path)
            hdfs_path = f"{HDFS_INPUT}/{self.run_id}/{filename}"
            t = threading.Thread(
                target=_upload_file_to_hdfs,
                args=(local_path, hdfs_path, i, results, lock),
                name=f"PigHDFSUpload-{i}",
                daemon=True,
            )
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def _count_records_locally(self):
        """
        Lightweight local scan for record/malformed counts only.
        The actual ETL (parsing + aggregation) is done by Pig.
        """
        from parser import parse_file_in_batches
        for file_path in self.data_files:
            for _, records, malformed in parse_file_in_batches(
                file_path, self.batch_size
            ):
                self.total_records     += len(records)
                self.malformed_records += malformed

    # ── Pig job submission ────────────────────────────────────────────────────

    def _run_pig_script(self, hdfs_out_base: str) -> bool:
        """
        Run pig_scripts/etl_queries.pig via `pig -x mapreduce`.

        Parameters passed to the script (accessible as $VAR in Pig Latin):
          INPUT_DIR   – HDFS directory containing both log files
          OUTPUT_Q1   – HDFS output path for Query 1
          OUTPUT_Q2   – HDFS output path for Query 2
          OUTPUT_Q3   – HDFS output path for Query 3
          ERROR_MIN   – lower bound for error status codes (400)
          ERROR_MAX   – upper bound for error status codes (599)
          TOP_N       – top-N resources for Query 2 (20)
        """
        pig_script = os.path.join(PIG_SCRIPTS_DIR, "etl_queries.pig")
        hdfs_input_dir = f"{HDFS_INPUT}/{self.run_id}"

        cmd = [
            PIG_BIN, "-x", "mapreduce",
            "-param", f"INPUT_DIR={hdfs_input_dir}",
            "-param", f"OUTPUT_Q1={hdfs_out_base}/q1_daily_traffic",
            "-param", f"OUTPUT_Q2={hdfs_out_base}/q2_top_resources",
            "-param", f"OUTPUT_Q3={hdfs_out_base}/q3_hourly_errors",
            "-param", f"ERROR_MIN=400",
            "-param", f"ERROR_MAX=599",
            "-param", f"TOP_N=20",
            pig_script,
        ]

        print(f"  CMD: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode == 0

    # ── Collect & parse TSV output ────────────────────────────────────────────

    def _collect_results(self, hdfs_out_base: str):
        """
        Read the three Pig output directories and parse tab-separated rows.

        Q1 schema : log_date \\t status_code \\t request_count \\t total_bytes
        Q2 schema : resource_path \\t request_count \\t total_bytes \\t distinct_host_count
        Q3 schema : log_date \\t log_hour \\t error_request_count \\t total_request_count
                    \\t error_rate \\t distinct_error_hosts
        """
        q1_rows = self._read_hdfs_tsv(
            f"{hdfs_out_base}/q1_daily_traffic",
            self._parse_q1_row,
        )
        q2_rows = self._read_hdfs_tsv(
            f"{hdfs_out_base}/q2_top_resources",
            self._parse_q2_row,
        )
        q3_rows = self._read_hdfs_tsv(
            f"{hdfs_out_base}/q3_hourly_errors",
            self._parse_q3_row,
        )
        print(
            f"  Q1 rows: {len(q1_rows)}, "
            f"Q2 rows: {len(q2_rows)}, "
            f"Q3 rows: {len(q3_rows)}"
        )
        return q1_rows, q2_rows, q3_rows

    @staticmethod
    def _read_hdfs_tsv(hdfs_path: str, parse_fn) -> list:
        r = _hdfs("-cat", f"{hdfs_path}/part-*")
        if r.returncode != 0:
            raise RuntimeError(f"Could not read HDFS output '{hdfs_path}': {r.stderr}")
        rows = []
        for line in r.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = parse_fn(line)
                if row:
                    rows.append(row)
            except Exception as exc:
                print(f"  ⚠ Skipping malformed output line ({exc}): {line!r}")
        return rows

    # ── Per-query TSV parsers ─────────────────────────────────────────────────

    @staticmethod
    def _parse_q1_row(line: str):
        """log_date \t status_code \t request_count \t total_bytes"""
        parts = line.split("\t")
        if len(parts) < 4:
            return None
        return {
            "log_date":      parts[0],
            "status_code":   int(parts[1]),
            "request_count": int(parts[2]),
            "total_bytes":   int(parts[3]),
        }

    @staticmethod
    def _parse_q2_row(line: str):
        """resource_path \t request_count \t total_bytes \t distinct_host_count"""
        parts = line.split("\t")
        if len(parts) < 4:
            return None
        return {
            "resource_path":      parts[0],
            "request_count":      int(parts[1]),
            "total_bytes":        int(parts[2]),
            "distinct_host_count": int(parts[3]),
        }

    @staticmethod
    def _parse_q3_row(line: str):
        """log_date \t log_hour \t error_req_count \t total_req_count \t error_rate \t distinct_error_hosts"""
        parts = line.split("\t")
        if len(parts) < 6:
            return None
        return {
            "log_date":             parts[0],
            "log_hour":             int(parts[1]),
            "error_request_count":  int(parts[2]),
            "total_request_count":  int(parts[3]),
            "error_rate":           round(float(parts[4]), 4),
            "distinct_error_hosts": int(parts[5]),
        }
