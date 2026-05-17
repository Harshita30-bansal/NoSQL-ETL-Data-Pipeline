"""
reports/reporter.py – Reporting layer for all pipeline results.
Reads from PostgreSQL; works for any of the 4 pipelines.
"""

from src.db_manager import DBManager


class Reporter:
    def __init__(self, db_type: str = "postgresql"):
        self.db_manager = DBManager("postgresql")
        self.db_manager.connect()

    # ── full report ───────────────────────────────────────────────────────────

    def generate_full_report(self, run_id: str):
        print(f"\n{'='*60}")
        print(f"  Report for Run ID: {run_id}")
        print(f"{'='*60}")

        # Metadata
        meta = self.db_manager.execute_query(
            "SELECT * FROM execution_metadata WHERE run_id = %s", (run_id,)
        )
        if not meta:
            print("  No metadata found for this run ID.")
            return
        m = meta[0]
        print(f"\n  Pipeline      : {m['pipeline_name']}")
        print(f"  Start         : {m['execution_start']}")
        print(f"  End           : {m['execution_end']}")
        print(f"  Runtime (ms)  : {m['execution_time_ms']:,}")
        print(f"  Total records : {m['total_records']:,}")
        print(f"  Malformed     : {m['malformed_records']:,}")
        print(f"  Batches       : {m['total_batches']}")
        print(f"  Avg batch     : {m['avg_batch_size']:,.1f}")
        print(f"  Data file     : {m['data_file']}")

        self._report_q1(run_id)
        self._report_q2(run_id)
        self._report_q3(run_id)

    def _report_q1(self, run_id: str):
        rows = self.db_manager.execute_query(
            """
            SELECT log_date, status_code,
                   SUM(request_count) AS request_count,
                   SUM(total_bytes)   AS total_bytes
            FROM   daily_traffic_summary
            WHERE  run_id = %s
            GROUP  BY log_date, status_code
            ORDER  BY log_date, status_code
            LIMIT  20
            """,
            (run_id,),
        )
        print(f"\n  Query 1 – Daily Traffic Summary (first 20 rows)")
        print(f"  {'Date':<12} {'Status':>6} {'Requests':>10} {'Bytes':>15}")
        print(f"  {'-'*12} {'-'*6} {'-'*10} {'-'*15}")
        for r in rows:
            print(
                f"  {str(r['log_date']):<12} "
                f"{r['status_code']:>6} "
                f"{r['request_count']:>10,} "
                f"{r['total_bytes']:>15,}"
            )

    def _report_q2(self, run_id: str):
        rows = self.db_manager.execute_query(
            """
            SELECT resource_path, request_count, total_bytes,
                   distinct_host_count, rank
            FROM   top_resources
            WHERE  run_id = %s
            ORDER  BY rank
            LIMIT  20
            """,
            (run_id,),
        )
        print(f"\n  Query 2 – Top 20 Requested Resources")
        print(f"  {'Rank':>4}  {'Requests':>10}  {'Hosts':>6}  Path")
        print(f"  {'─'*4}  {'─'*10}  {'─'*6}  {'─'*40}")
        for r in rows:
            path = r["resource_path"][:50]
            print(
                f"  {r['rank']:>4}  {r['request_count']:>10,}  "
                f"{r['distinct_host_count']:>6}  {path}"
            )

    def _report_q3(self, run_id: str):
        rows = self.db_manager.execute_query(
            """
            SELECT log_date, log_hour, error_request_count,
                   total_request_count, error_rate, distinct_error_hosts
            FROM   hourly_error_analysis
            WHERE  run_id = %s
            ORDER  BY log_date, log_hour
            LIMIT  20
            """,
            (run_id,),
        )
        print(f"\n  Query 3 – Hourly Error Analysis (first 20 rows)")
        print(
            f"  {'Date':<12} {'Hour':>4} {'Errors':>8} "
            f"{'Total':>8} {'Rate':>7} {'ErrHosts':>9}"
        )
        print(f"  {'-'*12} {'-'*4} {'-'*8} {'-'*8} {'-'*7} {'-'*9}")
        for r in rows:
            print(
                f"  {str(r['log_date']):<12} "
                f"{r['log_hour']:>4} "
                f"{r['error_request_count']:>8,} "
                f"{r['total_request_count']:>8,} "
                f"{r['error_rate']:>7.4f} "
                f"{r['distinct_error_hosts']:>9}"
            )

    # ── list all executions ───────────────────────────────────────────────────

    def list_all_executions(self):
        rows = self.db_manager.execute_query(
            """
            SELECT run_id, pipeline_name, total_records, malformed_records,
                   execution_time_ms, created_at
            FROM   execution_metadata
            ORDER  BY created_at DESC
            LIMIT  20
            """
        )
        print(f"\n  {'Run ID':<14} {'Pipeline':<12} {'Records':>10} "
              f"{'Malformed':>10} {'ms':>8}  Timestamp")
        print(f"  {'─'*14} {'─'*12} {'─'*10} {'─'*10} {'─'*8}  {'─'*19}")
        for r in rows:
            print(
                f"  {r['run_id']:<14} "
                f"{r['pipeline_name']:<12} "
                f"{r['total_records']:>10,} "
                f"{r['malformed_records']:>10,} "
                f"{r['execution_time_ms']:>8,}  "
                f"{str(r['created_at'])[:19]}"
            )

    # ── compare pipelines ─────────────────────────────────────────────────────

    def compare_pipelines(self):
        rows = self.db_manager.execute_query(
            """
            SELECT pipeline_name,
                   COUNT(*)                                AS runs,
                   AVG(execution_time_ms)::BIGINT          AS avg_ms,
                   MIN(execution_time_ms)                  AS min_ms,
                   MAX(execution_time_ms)                  AS max_ms,
                   SUM(total_records)                      AS total_records
            FROM   execution_metadata
            GROUP  BY pipeline_name
            ORDER  BY avg_ms
            """
        )
        print(f"\n  Pipeline Comparison")
        print(
            f"  {'Pipeline':<12} {'Runs':>4} {'Avg ms':>10} "
            f"{'Min ms':>10} {'Max ms':>10} {'Total Records':>15}"
        )
        print(
            f"  {'─'*12} {'─'*4} {'─'*10} {'─'*10} {'─'*10} {'─'*15}"
        )
        for r in rows:
            print(
                f"  {r['pipeline_name']:<12} "
                f"{r['runs']:>4} "
                f"{r['avg_ms']:>10,} "
                f"{r['min_ms']:>10,} "
                f"{r['max_ms']:>10,} "
                f"{r['total_records']:>15,}"
            )