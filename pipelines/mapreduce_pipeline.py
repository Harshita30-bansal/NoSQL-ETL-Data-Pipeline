"""
MapReduce Pipeline - ETL using Python-based MapReduce paradigm
Implements the same three queries as the MongoDB pipeline using
Map → Shuffle/Sort → Reduce phases executed in-process with batching.
"""

import time
import json
from collections import defaultdict
from datetime import datetime
from itertools import groupby

from src.base_pipeline import BasePipeline
from parser import parse_file_in_batches
from config import ERROR_STATUS_MIN, ERROR_STATUS_MAX


# ─────────────────────────────────────────────────────────────────────────────
# MAPPER FUNCTIONS
# Each mapper receives one parsed log record and emits (key, value) pairs.
# ─────────────────────────────────────────────────────────────────────────────

def mapper_query1(record):
    """
    Query 1 – Daily Traffic Summary
    Key  : (log_date, status_code)
    Value: (bytes_transferred, 1)
    """
    key = (record["log_date"], record["status_code"])
    value = (record["bytes_transferred"], 1)
    return key, value


def mapper_query2(record):
    """
    Query 2 – Top Requested Resources
    Key  : resource_path
    Value: (bytes_transferred, 1, host)
    """
    key = record["resource_path"]
    value = (record["bytes_transferred"], 1, record["host"])
    return key, value


def mapper_query3(record):
    """
    Query 3 – Hourly Error Analysis
    Key  : (log_date, log_hour)
    Value: (is_error_int, host, 1)
    """
    is_error = 1 if ERROR_STATUS_MIN <= record["status_code"] <= ERROR_STATUS_MAX else 0
    key = (record["log_date"], record["log_hour"])
    value = (is_error, record["host"], 1)
    return key, value


# ─────────────────────────────────────────────────────────────────────────────
# REDUCER FUNCTIONS
# Each reducer receives a key and an iterable of values, and returns one
# aggregated output dict for that key.
# ─────────────────────────────────────────────────────────────────────────────

def reducer_query1(key, values):
    """
    Reduce for Query 1.
    Accumulate total_bytes and request_count for (log_date, status_code).
    """
    log_date, status_code = key
    total_bytes = 0
    request_count = 0
    for bytes_transferred, count in values:
        total_bytes += bytes_transferred
        request_count += count
    return {
        "log_date": log_date,
        "status_code": status_code,
        "request_count": request_count,
        "total_bytes": total_bytes,
    }


def reducer_query2(key, values):
    """
    Reduce for Query 2.
    Accumulate request_count, total_bytes and distinct hosts per resource_path.
    """
    resource_path = key
    total_bytes = 0
    request_count = 0
    hosts = set()
    for bytes_transferred, count, host in values:
        total_bytes += bytes_transferred
        request_count += count
        hosts.add(host)
    return {
        "resource_path": resource_path,
        "request_count": request_count,
        "total_bytes": total_bytes,
        "distinct_host_count": len(hosts),
    }


def reducer_query3(key, values):
    """
    Reduce for Query 3.
    Accumulate error counts, total counts and distinct error hosts per (date, hour).
    """
    log_date, log_hour = key
    error_request_count = 0
    total_request_count = 0
    error_hosts = set()
    for is_error, host, count in values:
        total_request_count += count
        if is_error:
            error_request_count += count
            error_hosts.add(host)
    error_rate = round(error_request_count / total_request_count, 4) if total_request_count > 0 else 0.0
    return {
        "log_date": log_date,
        "log_hour": log_hour,
        "error_request_count": error_request_count,
        "total_request_count": total_request_count,
        "error_rate": error_rate,
        "distinct_error_hosts": len(error_hosts),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SHUFFLE / SORT (in-process)
# Groups intermediate (key, value) pairs by key before passing to reducers.
# ─────────────────────────────────────────────────────────────────────────────

def shuffle_and_sort(pairs):
    """
    Collect all (key, value) pairs into a dict keyed by key.
    Returns {key: [value, ...]} ready for the reduce phase.
    """
    grouped = defaultdict(list)
    for key, value in pairs:
        grouped[key].append(value)
    return grouped


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE CLASS
# ─────────────────────────────────────────────────────────────────────────────

class MapReducePipeline(BasePipeline):
    """
    MapReduce-style ETL pipeline.

    Phases per batch:
        MAP    – apply mapper to every parsed record → emit (key, value) pairs
        SHUFFLE– group pairs by key (in-process)
        REDUCE – apply reducer per key group → produce partial aggregates

    After all batches are processed, a final merge reduce combines partial
    aggregates from all batches into the final result sets.
    """

    def __init__(self, data_file, batch_size=5000, db_type="postgresql"):
        super().__init__("MapReduce", data_file, batch_size, db_type)

        # Support single file or list of files (same as MongoDB pipeline)
        if isinstance(data_file, list):
            self.data_files = data_file
        else:
            self.data_files = [data_file]

        # Global intermediate stores – accumulated across all batches
        # Structure: {key: [value, ...]}
        self._intermediate_q1 = defaultdict(list)
        self._intermediate_q2 = defaultdict(list)
        self._intermediate_q3 = defaultdict(list)

    # ── Public entry point ────────────────────────────────────────────────────

    def execute(self):
        print(f"\n{'='*60}")
        print(f"Executing {self.pipeline_name} Pipeline")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            print("✗ Failed to connect to relational database")
            return False

        try:
            execution_timestamp = datetime.now()

            # ── Phase 1: Map + Shuffle (per batch, per file) ─────────────────
            print("  Phase 1: Map + Shuffle ...")
            for file_path in self.data_files:
                print(f"    Processing file: {file_path}")
                self._map_and_shuffle_file(file_path)

            print(f"  ✓ Map+Shuffle complete | "
                  f"batches={self.batch_count} | "
                  f"records={self.total_records} | "
                  f"malformed={self.malformed_records}")

            # ── Phase 2: Reduce ───────────────────────────────────────────────
            print("  Phase 2: Reduce ...")
            results_q1 = self._reduce_all(self._intermediate_q1, reducer_query1)
            results_q2 = self._reduce_all(self._intermediate_q2, reducer_query2)
            results_q3 = self._reduce_all(self._intermediate_q3, reducer_query3)

            # Query 2 post-processing: keep top 20 by request_count, add rank
            results_q2.sort(key=lambda r: r["request_count"], reverse=True)
            results_q2 = results_q2[:20]
            for rank, row in enumerate(results_q2, 1):
                row["rank"] = rank

            # Query 3 post-processing: sort by date then hour
            results_q3.sort(key=lambda r: (r["log_date"], r["log_hour"]))

            # Query 1 post-processing: sort by date then status
            results_q1.sort(key=lambda r: (r["log_date"], r["status_code"]))

            print(f"  ✓ Reduce complete | "
                  f"Q1 rows={len(results_q1)} | "
                  f"Q2 rows={len(results_q2)} | "
                  f"Q3 rows={len(results_q3)}")

            # ── Phase 3: Load into relational DB ─────────────────────────────
            print("  Phase 3: Loading results into relational database ...")
            self._load_results(results_q1, results_q2, results_q3, execution_timestamp)

            self.end_time = time.time()
            self.save_metadata()

            print(self.get_status_string())
            return True

        except Exception as e:
            print(f"✗ Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()
            self.end_time = time.time()
            return False

        finally:
            self.disconnect_db()

    # ── Internal Map+Shuffle ──────────────────────────────────────────────────

    def _map_and_shuffle_file(self, file_path):
        """
        Stream one file in batches, apply all three mappers per record,
        and accumulate the intermediate (key→values) stores.
        """
        for batch_id, records, malformed in parse_file_in_batches(file_path, self.batch_size):
            self.batch_count += 1
            self.total_records += len(records)
            self.malformed_records += malformed

            # MAP phase for this batch
            pairs_q1 = []
            pairs_q2 = []
            pairs_q3 = []

            for record in records:
                k1, v1 = mapper_query1(record)
                pairs_q1.append((k1, v1))

                k2, v2 = mapper_query2(record)
                pairs_q2.append((k2, v2))

                k3, v3 = mapper_query3(record)
                pairs_q3.append((k3, v3))

            # SHUFFLE phase – merge into global intermediate stores
            for key, value in pairs_q1:
                self._intermediate_q1[key].append(value)
            for key, value in pairs_q2:
                self._intermediate_q2[key].append(value)
            for key, value in pairs_q3:
                self._intermediate_q3[key].append(value)

            print(f"    Batch {self.batch_count}: mapped {len(records)} records "
                  f"({malformed} malformed)")

    # ── Internal Reduce ───────────────────────────────────────────────────────

    @staticmethod
    def _reduce_all(intermediate, reducer_fn):
        """
        Apply reducer_fn to each (key, values) group in intermediate dict.
        Returns list of result dicts.
        """
        results = []
        for key, values in intermediate.items():
            result = reducer_fn(key, values)
            results.append(result)
        return results

    # ── Load results into relational DB ──────────────────────────────────────

    def _load_results(self, results_q1, results_q2, results_q3, execution_timestamp):
        """
        Insert a parent row into query_results, then insert per-query result rows.
        Mirrors the load pattern used by the MongoDB pipeline.
        """
        elapsed_ms = int((time.time() - self.start_time) * 1000)

        # Parent row in query_results
        self.db_manager.execute_update("""
            INSERT INTO query_results (
                run_id, pipeline_name, query_id, query_name,
                batch_id, batch_size, avg_batch_size,
                execution_time_ms, execution_timestamp,
                malformed_records, total_records_processed, result_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            self.run_id,
            self.pipeline_name,
            1,
            "all_queries",
            self.batch_count,
            self.batch_size,
            self.total_records / self.batch_count if self.batch_count else 0,
            elapsed_ms,
            execution_timestamp,
            self.malformed_records,
            self.total_records,
            json.dumps({"summary": "all queries executed via MapReduce"})
        ))

        # Query-specific result rows
        self.db_manager.insert_daily_traffic_results(
            results_q1, self.run_id, self.pipeline_name, 1, execution_timestamp
        )
        self.db_manager.insert_top_resources_results(
            results_q2, self.run_id, self.pipeline_name, 1, execution_timestamp
        )
        self.db_manager.insert_error_analysis_results(
            results_q3, self.run_id, self.pipeline_name, 1, execution_timestamp
        )

        print("  ✓ Results stored in relational database")