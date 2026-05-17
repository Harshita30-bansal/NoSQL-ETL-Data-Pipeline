"""
db_manager.py – PostgreSQL-only DB manager.
Replaces the old db_loader.py. Used by base_pipeline and all pipelines.
"""

import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from config import DB_CONFIG


class DBManager:
    """Handles all PostgreSQL operations for the ETL framework."""

    def __init__(self, db_type: str = "postgresql"):
        if db_type != "postgresql":
            raise ValueError("Only 'postgresql' is supported.")
        self._config = DB_CONFIG["postgresql"]
        self._conn   = None

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            self._conn = psycopg2.connect(**self._config)
            print("✓ Connected to PostgreSQL")
            return True
        except Exception as e:
            print(f"✗ PostgreSQL connection failed: {e}")
            return False

    def disconnect(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            print("✓ Disconnected from PostgreSQL")

    # ── Generic helpers ───────────────────────────────────────────────────────

    def execute_query(self, sql: str, params=None) -> list:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    def execute_update(self, sql: str, params=None):
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
        self._conn.commit()

    # ── run lifecycle ─────────────────────────────────────────────────────────

    def create_run(self, run_id: str, pipeline: str, batch_size: int,
                   data_file: str):
        """Insert a placeholder row into execution_metadata."""
        now = datetime.now(timezone.utc)
        self.execute_update(
            """
            INSERT INTO execution_metadata (
                run_id, pipeline_name, batch_size, total_batches,
                total_records, malformed_records, avg_batch_size,
                execution_start, execution_end, execution_time_ms, data_file
            ) VALUES (%s,%s,%s,0,0,0,0,%s,%s,0,%s)
            """,
            (run_id, pipeline, batch_size, now, now, data_file),
        )

    def finish_run(self, run_id: str, total_records: int, malformed: int,
                   batch_count: int, avg_batch_size: float, runtime_s: float):
        """Update execution_metadata with final statistics."""
        self.execute_update(
            """
            UPDATE execution_metadata
            SET total_records     = %s,
                malformed_records = %s,
                total_batches     = %s,
                avg_batch_size    = %s,
                execution_time_ms = %s,
                execution_end     = %s
            WHERE run_id = %s
            """,
            (
                total_records, malformed, batch_count,
                avg_batch_size, int(runtime_s * 1000),
                datetime.now(timezone.utc), run_id,
            ),
        )

    # ── Query result inserters ────────────────────────────────────────────────

    def insert_parent_result(self, run_id, pipeline_name, batch_count,
                              batch_size, total_records, malformed,
                              elapsed_ms, execution_timestamp, extra_json=None):
        """Insert the single parent row in query_results."""
        import json
        avg = total_records / batch_count if batch_count else 0
        self.execute_update(
            """
            INSERT INTO query_results (
                run_id, pipeline_name, query_id, query_name,
                batch_id, batch_size, avg_batch_size,
                execution_time_ms, execution_timestamp,
                malformed_records, total_records_processed, result_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                run_id, pipeline_name, 1, "all_queries",
                batch_count, batch_size, avg,
                elapsed_ms, execution_timestamp,
                malformed, total_records,
                json.dumps(extra_json or {}),
            ),
        )

    def insert_daily_traffic_results(self, rows: list, run_id: str,
                                      pipeline_name: str, batch_id: int,
                                      execution_timestamp):
        if not rows:
            return
        data = [
            (run_id, pipeline_name, r["log_date"], r["status_code"],
             r["request_count"], r["total_bytes"], batch_id, execution_timestamp)
            for r in rows
        ]
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO daily_traffic_summary
                    (run_id, pipeline_name, log_date, status_code,
                     request_count, total_bytes, batch_id, execution_timestamp)
                VALUES %s
                """,
                data,
            )
        self._conn.commit()
        print(f"  ✓ Stored {len(rows)} rows → daily_traffic_summary")

    def insert_top_resources_results(self, rows: list, run_id: str,
                                      pipeline_name: str, batch_id: int,
                                      execution_timestamp):
        if not rows:
            return
        data = [
            (run_id, pipeline_name, r["resource_path"], r["request_count"],
             r["total_bytes"], r["distinct_host_count"], r["rank"],
             batch_id, execution_timestamp)
            for r in rows
        ]
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO top_resources
                    (run_id, pipeline_name, resource_path, request_count,
                     total_bytes, distinct_host_count, rank,
                     batch_id, execution_timestamp)
                VALUES %s
                """,
                data,
            )
        self._conn.commit()
        print(f"  ✓ Stored {len(rows)} rows → top_resources")

    def insert_error_analysis_results(self, rows: list, run_id: str,
                                       pipeline_name: str, batch_id: int,
                                       execution_timestamp):
        if not rows:
            return
        data = [
            (run_id, pipeline_name, r["log_date"], r["log_hour"],
             r["error_request_count"], r["total_request_count"],
             r["error_rate"], r["distinct_error_hosts"],
             batch_id, execution_timestamp)
            for r in rows
        ]
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO hourly_error_analysis
                    (run_id, pipeline_name, log_date, log_hour,
                     error_request_count, total_request_count,
                     error_rate, distinct_error_hosts,
                     batch_id, execution_timestamp)
                VALUES %s
                """,
                data,
            )
        self._conn.commit()
        print(f"  ✓ Stored {len(rows)} rows → hourly_error_analysis")