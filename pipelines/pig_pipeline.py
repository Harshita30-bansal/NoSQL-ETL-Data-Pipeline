"""
pipelines/pig_pipeline.py
────────────────────────────────────────────────────────────────────
Apache Pig ETL pipeline.

Strategy
────────
• Runs in -x local mode: Pig reads directly from the local filesystem,
  no Hadoop/HDFS required. This keeps Pig clearly distinct from the
  MapReduce pipeline which handles the Hadoop/HDFS story.
• Both data files (July + August) are passed to the Pig script as
  parameters. Pig treats them as Batch 1 and Batch 2 internally.
• All parsing, cleaning, filtering, grouping, and aggregation happens
  inside the Pig Latin script (pig_scripts/etl_queries.pig).
• The Pig script writes four output directories to a local temp dir:
    q1_daily_traffic/
    q2_top_resources/
    q3_hourly_errors/
    metadata/          <- total_records and malformed_records counts
• The Python layer reads those outputs, parses the TSV rows, and
  stores everything in PostgreSQL via DBManager.
• No _count_records_locally() — record counts come from Pig itself.
"""

import os
import subprocess
import tempfile
import time
from datetime import datetime

from src.base_pipeline import BasePipeline
from config import PIG_BIN, PIG_SCRIPTS_DIR


class PigPipeline(BasePipeline):

    def __init__(self, data_file, batch_size: int = 5000, db_type: str = "postgresql", query_name="all"):
        super().__init__("Pig", data_file, batch_size, db_type, query_name)
        self.data_files = data_file if isinstance(data_file, list) else [data_file]

    # ── main execute ──────────────────────────────────────────────────────────

    def execute(self) -> bool:
        print(f"\n{'='*60}")
        print(f"Executing Apache Pig Pipeline (-x local)")
        print(f"Files : {self.data_files}")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            return False

        # Use a temp directory for all Pig output so runs don't collide
        out_dir = tempfile.mkdtemp(prefix=f"pig_{self.run_id}_")

        try:
            execution_timestamp = datetime.now()

            # Both files = 2 logical batches (July + August)
            self.batch_count = len(self.data_files)

            # ── Step 1: Run Pig script ────────────────────────────────────────
            print("Step 1: Submitting Apache Pig job (-x local)...")
            ok = self._run_pig_script(out_dir)
            if not ok:
                print("x Pig job failed.")
                return False

            # ── Step 2: Read metadata (record counts) from Pig output ─────────
            print("\nStep 2: Reading record counts from Pig metadata output...")
            self._read_metadata(out_dir)

            # Store logical batch metadata
            for batch_id, file_path in enumerate(self.data_files, start=1):

                self.db_manager.insert_batch_metadata(
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    batch_id=batch_id,
                    batch_size=self.batch_size,
                    records_processed=self.total_records // self.batch_count,
                    malformed_records=self.malformed_records // self.batch_count,
                    execution_timestamp=execution_timestamp,
                )

                self.db_manager.insert_malformed_summary(
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    batch_id=batch_id,
                    malformed_count=self.malformed_records // self.batch_count,
                    execution_timestamp=execution_timestamp,
                )

            # ── Step 3: Collect TSV results ───────────────────────────────────
            print("\nStep 3: Collecting Pig query results...")
            q1_rows, q2_rows, q3_rows = self._collect_results(out_dir)

            # Keep only selected query
            if self.query_name == "query1":
                q2_rows = []
                q3_rows = []

            elif self.query_name == "query2":
                q1_rows = []
                q3_rows = []

            elif self.query_name == "query3":
                q1_rows = []
                q2_rows = []

            # Post-process only if needed
            if q2_rows:
                for rank, row in enumerate(q2_rows, 1):
                    row["rank"] = rank

            # ── Step 5: Store in PostgreSQL ───────────────────────────────────
            print("\nStep 4: Storing results in PostgreSQL...")
            elapsed_ms = int((time.time() - self.start_time) * 1000)
            self.db_manager.insert_parent_result(
                run_id=self.run_id,
                pipeline_name=self.pipeline_name,
                query_name=self.query_name,
                batch_count=self.batch_count,
                batch_size=self.batch_size,
                total_records=self.total_records,
                malformed=self.malformed_records,
                elapsed_ms=elapsed_ms,
                execution_timestamp=execution_timestamp,
                extra_json={"mode": "pig_local"},
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
            print(f"x Pig pipeline error: {exc}")
            import traceback; traceback.print_exc()
            return False
        finally:
            self.disconnect_db()
            self._cleanup_output(out_dir)

    # ── Pig job submission ────────────────────────────────────────────────────

    def _run_pig_script(self, out_dir: str) -> bool:
        """
        Run pig_scripts/etl_queries.pig in local mode.

        Parameters passed to the script:
          INPUT_JULY   - local path to July log file  (Batch 1)
          INPUT_AUGUST - local path to August log file (Batch 2)
          OUTPUT_Q1    - local output dir for Query 1
          OUTPUT_Q2    - local output dir for Query 2
          OUTPUT_Q3    - local output dir for Query 3
          OUTPUT_META  - local output dir for metadata (record counts)
          ERROR_MIN    - lower bound for error status codes (400)
          ERROR_MAX    - upper bound for error status codes (599)
          TOP_N        - top-N resources for Query 2 (20)
        """
        script_map = {
            "query1": "query1.pig",
            "query2": "query2.pig",
            "query3": "query3.pig",
            "all": "all_queries.pig",
        }

        pig_script = os.path.join(
            PIG_SCRIPTS_DIR,
            script_map.get(self.query_name, "all_queries.pig")
        )

        # Resolve absolute paths so Pig local mode can find the files
        july_path   = os.path.abspath(self.data_files[0])
        august_path = os.path.abspath(self.data_files[1]) if len(self.data_files) > 1 else july_path

        cmd = [
            PIG_BIN, "-x", "local",
            "-param", f"INPUT_JULY={july_path}",
            "-param", f"INPUT_AUGUST={august_path}",
            "-param", f"OUTPUT_META={out_dir}/metadata",
            "-param", f"ERROR_MIN=400",
            "-param", f"ERROR_MAX=599",
            "-param", f"TOP_N=20",
        ]

        if self.query_name in ("query1", "all"):
            cmd += [
                "-param",
                f"OUTPUT_Q1={out_dir}/q1_daily_traffic"
            ]

        if self.query_name in ("query2", "all"):
            cmd += [
                "-param",
                f"OUTPUT_Q2={out_dir}/q2_top_resources"
            ]

        if self.query_name in ("query3", "all"):
            cmd += [
                "-param",
                f"OUTPUT_Q3={out_dir}/q3_hourly_errors"
            ]

        cmd.append(pig_script)

        print(f"  CMD: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode == 0

    # ── Read metadata written by Pig ──────────────────────────────────────────

    def _read_metadata(self, out_dir: str):
        """
        Read total_records and malformed_records from the metadata output
        written by the Pig script.
        Schema: total_records \\t malformed_records
        """
        meta_dir = os.path.join(out_dir, "metadata")
        lines = self._read_local_tsv_lines(meta_dir)
        for line in lines:
            parts = line.split("\t")
            if len(parts) >= 2:
                try:
                    self.total_records     = int(parts[0])
                    self.malformed_records = int(parts[1])
                    print(
                        f"  Records: {self.total_records:,} valid, "
                        f"{self.malformed_records:,} malformed"
                    )
                except ValueError:
                    pass

    # ── Collect & parse TSV output ────────────────────────────────────────────

    def _collect_results(self, out_dir: str):
        q1_rows = []
        q2_rows = []
        q3_rows = []

        if self.query_name in ("query1", "all"):
            q1_rows = self._read_local_tsv(
                os.path.join(out_dir, "q1_daily_traffic"),
                self._parse_q1_row,
            )

        if self.query_name in ("query2", "all"):
            q2_rows = self._read_local_tsv(
                os.path.join(out_dir, "q2_top_resources"),
                self._parse_q2_row,
            )

        if self.query_name in ("query3", "all"):
            q3_rows = self._read_local_tsv(
                os.path.join(out_dir, "q3_hourly_errors"),
                self._parse_q3_row,
            )

        print(
            f"  Q1 rows: {len(q1_rows)}, "
            f"Q2 rows: {len(q2_rows)}, "
            f"Q3 rows: {len(q3_rows)}"
        )
        return q1_rows, q2_rows, q3_rows

    @staticmethod
    def _read_local_tsv_lines(directory: str) -> list:
        """Read all part-* files from a local Pig output directory."""
        lines = []
        if not os.path.isdir(directory):
            raise RuntimeError(f"Pig output directory not found: {directory}")
        for fname in sorted(os.listdir(directory)):
            if fname.startswith("part-"):
                fpath = os.path.join(directory, fname)
                with open(fpath, "r", encoding="utf-8", errors="replace") as fh:
                    for line in fh:
                        line = line.strip()
                        if line:
                            lines.append(line)
        return lines

    @staticmethod
    def _read_local_tsv(directory: str, parse_fn) -> list:
        rows = []
        for line in PigPipeline._read_local_tsv_lines(directory):
            try:
                row = parse_fn(line)
                if row:
                    rows.append(row)
            except Exception as exc:
                print(f"  Skipping malformed output line ({exc}): {line!r}")
        return rows

    # ── Per-query TSV parsers ─────────────────────────────────────────────────

    @staticmethod
    def _parse_q1_row(line: str):
        """log_date \\t status_code \\t request_count \\t total_bytes"""
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
        """resource_path \\t request_count \\t total_bytes \\t distinct_host_count"""
        parts = line.split("\t")
        if len(parts) < 4:
            return None
        return {
            "resource_path":       parts[0],
            "request_count":       int(parts[1]),
            "total_bytes":         int(parts[2]),
            "distinct_host_count": int(parts[3]),
        }

    @staticmethod
    def _parse_q3_row(line: str):
        """log_date \\t log_hour \\t error_req_count \\t total_req_count \\t error_rate \\t distinct_error_hosts"""
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

    # ── Cleanup ───────────────────────────────────────────────────────────────

    @staticmethod
    def _cleanup_output(out_dir: str):
        """Remove the temp output directory after results are stored."""
        try:
            import shutil
            shutil.rmtree(out_dir, ignore_errors=True)
            print(f"  Cleaned up temp output: {out_dir}")
        except Exception:
            pass
