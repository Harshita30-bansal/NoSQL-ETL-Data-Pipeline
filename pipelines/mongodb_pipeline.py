"""
pipelines/mongodb_pipeline.py
──────────────────────────────────────────────────────────────────
MongoDB ETL pipeline.

• Both log files are loaded into MongoDB in parallel (2 batches).
• batch_size controls the insert chunk size for memory management,
  but the logical batch count is always 2 (one per file).
• Three aggregation pipelines produce Q1/Q2/Q3 results.
• Results are stored in PostgreSQL via DBManager.
"""

import time
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from src.base_pipeline import BasePipeline
from src.parallel_batch_loader import load_files_parallel
from config import MONGO_CONFIG, ERROR_STATUS_MIN, ERROR_STATUS_MAX


class MongoDBPipeline(BasePipeline):

    def __init__(self, data_file, batch_size=5000, db_type="postgresql", query_name="all"):
        super().__init__("MongoDB", data_file, batch_size, db_type, query_name)
        self.data_files   = data_file if isinstance(data_file, list) else [data_file]
        self.mongo_client = None
        self.mongo_db     = None

    # ── execute ───────────────────────────────────────────────────────────────

    def execute(self) -> bool:
        print(f"\n{'='*60}")
        print(f"Executing MongoDB Pipeline")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            return False

        if not self._connect_mongodb():
            self.disconnect_db()
            return False

        try:
            execution_timestamp = datetime.now()

            # Step 1: Init collection
            print("Step 1: Initializing MongoDB collection…")
            self._initialize_collection()

            # Step 2: Load both files in parallel.
            # Each file = one logical batch (Batch 1 = July, Batch 2 = August).
            # batch_size only controls how many records are inserted per
            # insert_many call — it does NOT affect the logical batch count.
            print("\nStep 2: Loading logs into MongoDB in parallel (2 batches)…")
            collection = self.mongo_db[MONGO_CONFIG["collections"]["logs"]]

            # Track which batch numbers we've already counted so that
            # batch_count = number of files (2), not number of insert chunks.
            batch_numbers_seen = set()
            batch_record_totals = {}
            batch_malformed_totals = {}

            def _insert_batch(batch_number: int, records: list, malformed: int):
                """Called from parallel_batch_loader for each insert chunk."""
                if records:
                    collection.insert_many(records, ordered=False)
                self.total_records     += len(records)
                self.malformed_records += malformed

                batch_record_totals[batch_number] = (
                    batch_record_totals.get(batch_number, 0) + len(records)
                )

                batch_malformed_totals[batch_number] = (
                    batch_malformed_totals.get(batch_number, 0) + malformed
                )

                # Only count each file (batch_number) once, not every chunk
                if batch_number not in batch_numbers_seen:
                    batch_numbers_seen.add(batch_number)
                    self.batch_count += 1
                print(
                    f"  [Batch {batch_number}] inserted {len(records)} records "
                    f"({malformed} malformed)"
                )

            load_files_parallel(
                file_paths=self.data_files,
                batch_size=self.batch_size,
                process_fn=_insert_batch,
            )
            print(f"  Total inserted : {self.total_records:,} records")
            print(f"  Logical batches: {self.batch_count} (July + August)")

            # Store ONE metadata row per logical batch (file)
            for batch_id in batch_record_totals:

                self.db_manager.insert_batch_metadata(
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    batch_id=batch_id,
                    batch_size=self.batch_size,
                    records_processed=batch_record_totals[batch_id],
                    malformed_records=batch_malformed_totals[batch_id],
                    execution_timestamp=execution_timestamp,
                )

                self.db_manager.insert_malformed_summary(
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    batch_id=batch_id,
                    malformed_count=batch_malformed_totals[batch_id],
                    execution_timestamp=execution_timestamp,
                )

            # Step 3: Run aggregation queries
            print("\nStep 3: Running MongoDB aggregations…")
            q1_rows = []
            q2_rows = []
            q3_rows = []

            if self.query_name in ("query1", "all"):
                q1_rows = self._query1(collection)

            if self.query_name in ("query2", "all"):
                q2_rows = self._query2(collection)

            if self.query_name in ("query3", "all"):
                q3_rows = self._query3(collection)

            # Rank Q2
            for rank, row in enumerate(q2_rows, 1):
                row["rank"] = rank

            # Step 4: Store in PostgreSQL
            print("\nStep 4: Storing results in PostgreSQL…")
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
                extra_json={"mode": "mongodb_aggregation"},
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
            print(f"✗ MongoDB pipeline failed: {exc}")
            import traceback; traceback.print_exc()
            return False

        finally:
            self.disconnect_db()
            self._disconnect_mongodb()

    # ── MongoDB helpers ───────────────────────────────────────────────────────

    def _connect_mongodb(self) -> bool:
        try:
            self.mongo_client = MongoClient(
                MONGO_CONFIG["host"],
                MONGO_CONFIG["port"],
                serverSelectionTimeoutMS=5000,
            )
            self.mongo_db = self.mongo_client[MONGO_CONFIG["database"]]
            self.mongo_db.command("ping")
            print("✓ Connected to MongoDB")
            return True
        except ConnectionFailure as exc:
            print(f"✗ MongoDB connection failed: {exc}")
            return False

    def _disconnect_mongodb(self):
        if self.mongo_client:
            self.mongo_client.close()
            print("✓ Disconnected from MongoDB")

    def _initialize_collection(self):
        col = self.mongo_db[MONGO_CONFIG["collections"]["logs"]]
        col.drop()
        col.create_index([("log_date", 1), ("log_hour", 1)])
        col.create_index([("resource_path", 1)])
        col.create_index([("host", 1)])
        col.create_index([("status_code", 1)])
        print("  ✓ MongoDB collection initialised")

    # ── Aggregation queries ───────────────────────────────────────────────────

    def _query1(self, col) -> list:
        rows = list(col.aggregate([
            {"$group": {
                "_id": {"log_date": "$log_date", "status_code": "$status_code"},
                "request_count": {"$sum": 1},
                "total_bytes":   {"$sum": "$bytes_transferred"},
            }},
            {"$project": {
                "_id": 0,
                "log_date":      "$_id.log_date",
                "status_code":   "$_id.status_code",
                "request_count": 1,
                "total_bytes":   1,
            }},
        ]))
        print(f"  Q1: {len(rows)} rows")
        return rows

    def _query2(self, col) -> list:
        rows = list(col.aggregate([
            {"$match": {"resource_path": {"$ne": None}}},
            {"$group": {
                "_id":           "$resource_path",
                "request_count": {"$sum": 1},
                "total_bytes":   {"$sum": "$bytes_transferred"},
                "hosts":         {"$addToSet": "$host"},
            }},
            {"$project": {
                "_id": 0,
                "resource_path":       "$_id",
                "request_count":       1,
                "total_bytes":         1,
                "distinct_host_count": {"$size": "$hosts"},
            }},
            {"$sort": {"request_count": -1}},
            {"$limit": 20},
        ]))
        print(f"  Q2: {len(rows)} rows")
        return rows

    def _query3(self, col) -> list:
        rows = list(col.aggregate([
            {"$group": {
                "_id": {"log_date": "$log_date", "log_hour": "$log_hour"},
                "total_request_count": {"$sum": 1},
                "error_request_count": {
                    "$sum": {
                        "$cond": [
                            {"$and": [
                                {"$gte": ["$status_code", ERROR_STATUS_MIN]},
                                {"$lte": ["$status_code", ERROR_STATUS_MAX]},
                            ]},
                            1, 0,
                        ]
                    }
                },
                "error_hosts": {
                    "$addToSet": {
                        "$cond": [
                            {"$and": [
                                {"$gte": ["$status_code", ERROR_STATUS_MIN]},
                                {"$lte": ["$status_code", ERROR_STATUS_MAX]},
                            ]},
                            "$host", "$$REMOVE",
                        ]
                    }
                },
            }},
            {"$project": {
                "_id": 0,
                "log_date":             "$_id.log_date",
                "log_hour":             "$_id.log_hour",
                "error_request_count":  1,
                "total_request_count":  1,
                "error_rate": {
                    "$cond": [
                        {"$gt": ["$total_request_count", 0]},
                        {"$divide": ["$error_request_count",
                                     "$total_request_count"]},
                        0,
                    ]
                },
                "distinct_error_hosts": {"$size": "$error_hosts"},
            }},
        ]))
        for r in rows:
            r["error_rate"] = round(r["error_rate"], 4)
        print(f"  Q3: {len(rows)} rows")
        return rows
