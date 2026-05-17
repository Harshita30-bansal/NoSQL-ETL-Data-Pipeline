"""
pipelines/mapreduce_pipeline.py
────────────────────────────────────────────────────────────────────
Hadoop Streaming MapReduce pipeline.

Strategy
────────
• Both data files (July + August) are uploaded to HDFS in parallel
  threads, fulfilling "2 batches loaded parallelly".
• A single Hadoop Streaming job is submitted with both HDFS input
  files, so MapReduce itself processes them together.
• The reducer output (one JSON per line) is downloaded and parsed,
  then merged / ranked, then stored in PostgreSQL.
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
    HADOOP_BIN, HDFS_BIN, STREAMING_JAR,
    HDFS_INPUT, HDFS_OUTPUT,
    MR_SCRIPTS_DIR,
)


# ─── HDFS helpers ─────────────────────────────────────────────────────────────

def _hdfs(*args) -> subprocess.CompletedProcess:
    cmd = [HDFS_BIN, "dfs"] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True)


def _upload_file_to_hdfs(local_path: str, hdfs_path: str,
                          batch_number: int, results: dict, lock: threading.Lock):
    """Thread worker: upload one local file to HDFS."""
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
        print(f"  {status} Batch {batch_number} upload {'done' if ok else 'FAILED'}: {r.stderr or ''}")
    except Exception as exc:
        with lock:
            results[batch_number] = {"ok": False, "path": hdfs_path, "err": str(exc)}
        print(f"  ✗ Batch {batch_number} upload exception: {exc}")


# ─── Pipeline ─────────────────────────────────────────────────────────────────

class MapReducePipeline(BasePipeline):

    def __init__(self, data_file, batch_size=5000, db_type="postgresql"):
        super().__init__("MapReduce", data_file, batch_size, db_type)
        self.data_files = data_file if isinstance(data_file, list) else [data_file]

    # ── main execute ──────────────────────────────────────────────────────────

    def execute(self) -> bool:
        print(f"\n{'='*60}")
        print(f"Executing MapReduce Pipeline (Hadoop Streaming)")
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
            # total_records: we need to count; do a quick local parse scan
            self._count_records_locally()

            # ── Step 2: Run Hadoop Streaming job ─────────────────────────────
            print("\nStep 2: Submitting Hadoop Streaming job…")
            hdfs_output = f"{HDFS_OUTPUT}/mapreduce_{self.run_id}"
            ok = self._run_streaming_job(hdfs_output)
            if not ok:
                print("✗ MapReduce job failed.")
                return False

            # ── Step 3: Collect & parse reducer output ────────────────────────
            print("\nStep 3: Collecting reducer output from HDFS…")
            q1_rows, q2_rows, q3_rows = self._collect_results(hdfs_output)

            # ── Step 4: Post-process Q2 (rank top 20) ─────────────────────────
            q2_rows.sort(key=lambda r: r["request_count"], reverse=True)
            q2_rows = q2_rows[:20]
            for rank, row in enumerate(q2_rows, 1):
                row["rank"] = rank

            q1_rows.sort(key=lambda r: (r["log_date"], r["status_code"]))
            q3_rows.sort(key=lambda r: (r["log_date"], r["log_hour"]))

            # ── Step 5: Store in PostgreSQL ───────────────────────────────────
            print("\nStep 4: Storing results in PostgreSQL…")
            elapsed_ms = int((time.time() - self.start_time) * 1000)
            self.db_manager.insert_parent_result(
                self.run_id, self.pipeline_name,
                self.batch_count, self.batch_size,
                self.total_records, self.malformed_records,
                elapsed_ms, execution_timestamp,
                extra_json={"mode": "hadoop_streaming"},
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
            print(f"✗ MapReduce pipeline error: {exc}")
            import traceback; traceback.print_exc()
            return False
        finally:
            self.disconnect_db()

    # ── HDFS setup ────────────────────────────────────────────────────────────

    def _prepare_hdfs_input(self):
        _hdfs("-rm", "-r", "-f", f"{HDFS_INPUT}/{self.run_id}")
        _hdfs("-mkdir", "-p", f"{HDFS_INPUT}/{self.run_id}")

    def _parallel_upload_to_hdfs(self) -> dict:
        results: dict = {}
        lock = threading.Lock()
        threads = []
        for i, local_path in enumerate(self.data_files, start=1):
            filename  = os.path.basename(local_path)
            hdfs_path = f"{HDFS_INPUT}/{self.run_id}/{filename}"
            t = threading.Thread(
                target=_upload_file_to_hdfs,
                args=(local_path, hdfs_path, i, results, lock),
                name=f"HDFSUpload-{i}",
                daemon=True,
            )
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def _count_records_locally(self):
        """Quick local scan to get record counts (not the MapReduce step)."""
        from parser import parse_file_in_batches
        for file_path in self.data_files:
            for _, records, malformed in parse_file_in_batches(
                file_path, self.batch_size
            ):
                self.total_records    += len(records)
                self.malformed_records += malformed

    # ── Hadoop Streaming job ──────────────────────────────────────────────────

    def _run_streaming_job(self, hdfs_output: str) -> bool:
        mapper_script  = os.path.join(MR_SCRIPTS_DIR, "mapper.py")
        reducer_script = os.path.join(MR_SCRIPTS_DIR, "reducer.py")
        parser_script  = "parser.py"

        # Build -input args for all uploaded files
        input_args = []
        for local_path in self.data_files:
            filename = os.path.basename(local_path)
            input_args += ["-input", f"{HDFS_INPUT}/{self.run_id}/{filename}"]

        cmd = [
            HADOOP_BIN, "jar", STREAMING_JAR,
            *input_args,
            "-output",  hdfs_output,
            "-mapper",  f"python3 mapper.py",
            "-reducer", f"python3 reducer.py",
            "-file",    mapper_script,
            "-file",    reducer_script,
            "-file",    parser_script,
            # Sort by full line so all query tags group correctly
            "-jobconf", "stream.num.map.output.key.fields=3",
            "-jobconf", "mapreduce.job.reduces=4",
        ]

        print(f"  CMD: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode == 0

    # ── Collect results ───────────────────────────────────────────────────────

    def _collect_results(self, hdfs_output: str):
        r = _hdfs("-cat", f"{hdfs_output}/part-*")
        if r.returncode != 0:
            raise RuntimeError(f"Could not read HDFS output: {r.stderr}")

        q1_rows, q2_rows, q3_rows = [], [], []
        for line in r.stdout.splitlines():
            if not line.strip():
                continue
            tag, _, json_str = line.partition("\t")
            try:
                obj = json.loads(json_str)
            except json.JSONDecodeError:
                continue
            if tag == "RESULT_Q1":
                q1_rows.append(obj)
            elif tag == "RESULT_Q2":
                q2_rows.append(obj)
            elif tag == "RESULT_Q3":
                q3_rows.append(obj)

        print(f"  Q1 rows: {len(q1_rows)}, Q2 rows: {len(q2_rows)}, Q3 rows: {len(q3_rows)}")
        return q1_rows, q2_rows, q3_rows