"""
pipelines/mongodb/mongo_pipeline.py
────────────────────────────────────
MongoDB execution pipeline.

Strategy
--------
1. Connect to MongoDB, drop & recreate the raw_logs collection.
2. Read log files in batches using the shared parser.
3. Insert each batch into MongoDB via bulk_write.
4. After all batches are loaded, run three aggregation pipelines
   to compute Q1, Q2, Q3.
5. Push results through db_loader into PostgreSQL.

Why aggregation pipelines?
  MongoDB's $group + $sort operators mirror SQL GROUP BY semantics and
  execute server-side, so they are a genuine NoSQL computation – not
  Python-side aggregation.
"""

import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pymongo import MongoClient, InsertOne
from tqdm import tqdm

import config
from parser import parse_file_batched
from db import db_loader


# ─── Aggregation pipelines ────────────────────────────────────────────────────

Q1_PIPELINE = [
    {"$group": {
        "_id":           {"log_date": "$log_date", "status_code": "$status_code"},
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
    {"$sort": {"log_date": 1, "status_code": 1}},
]

Q2_PIPELINE = [
    {"$group": {
        "_id":           "$resource_path",
        "request_count": {"$sum": 1},
        "total_bytes":   {"$sum": "$bytes_transferred"},
        "hosts":         {"$addToSet": "$host"},
    }},
    {"$project": {
        "_id": 0,
        "resource_path":      "$_id",
        "request_count":      1,
        "total_bytes":        1,
        "distinct_host_count": {"$size": "$hosts"},
    }},
    {"$sort": {"request_count": -1}},
    {"$limit": 20},
]

Q3_PIPELINE = [
    {"$group": {
        "_id": {"log_date": "$log_date", "log_hour": "$log_hour"},
        "total_request_count": {"$sum": 1},
        "error_request_count": {
            "$sum": {
                "$cond": [
                    {"$and": [
                        {"$gte": ["$status_code", 400]},
                        {"$lte": ["$status_code", 599]},
                    ]},
                    1, 0,
                ]
            }
        },
        "error_hosts": {
            "$addToSet": {
                "$cond": [
                    {"$and": [
                        {"$gte": ["$status_code", 400]},
                        {"$lte": ["$status_code", 599]},
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
                {"$divide": ["$error_request_count", "$total_request_count"]},
                0,
            ]
        },
        "distinct_error_hosts": {"$size": "$error_hosts"},
    }},
    {"$sort": {"log_date": 1, "log_hour": 1}},
]


# ─── Main entry point ─────────────────────────────────────────────────────────

def run(batch_size: int = config.DEFAULT_BATCH_SIZE,
        log_files: list = None):

    if log_files is None:
        log_files = config.LOG_FILES

    # Check files exist
    existing = [f for f in log_files if os.path.isfile(f)]
    if not existing:
        print(f"[MongoDB] ERROR: No log files found in {config.DATA_DIR}")
        print("          Run scripts/download_data.sh first.")
        return

    # ── MongoDB connection ────────────────────────────────────────────────
    client = MongoClient(config.MONGO_URI)
    db     = client[config.MONGO_DB]
    col    = db[config.MONGO_COL]
    col.drop()   # fresh collection each run

    # ── PostgreSQL run registration ───────────────────────────────────────
    run_id = db_loader.create_run("mongodb", batch_size)
    print(f"[MongoDB] Run ID: {run_id} | Batch size: {batch_size}")

    total_records   = 0
    total_malformed = 0
    batch_count     = 0
    t_start         = time.perf_counter()

    # ── Phase 1: Load raw logs into MongoDB in batches ────────────────────
    for filepath in existing:
        print(f"[MongoDB] Loading {os.path.basename(filepath)} ...")
        for batch_id, records, malformed in tqdm(
            parse_file_batched(filepath, batch_size),
            desc="  batches", unit="batch"
        ):
            if records:
                ops = [InsertOne(r) for r in records]
                col.bulk_write(ops, ordered=False)

            total_records   += len(records)
            total_malformed += malformed
            batch_count     += 1

    # ── Phase 2: Aggregation queries ─────────────────────────────────────
    print("[MongoDB] Running aggregation pipelines ...")

    # Q1
    q1_rows = list(col.aggregate(Q1_PIPELINE, allowDiskUse=True))
    q1_out = [
        {"log_date": r["log_date"], "status_code": r["status_code"],
         "request_count": r["request_count"], "total_bytes": r["total_bytes"]}
        for r in q1_rows
    ]
    db_loader.load_q1(run_id, "mongodb", batch_count, q1_out)
    print(f"[MongoDB] Q1: {len(q1_out)} rows")

    # Q2
    q2_rows = list(col.aggregate(Q2_PIPELINE, allowDiskUse=True))
    q2_out = [
        {"resource_path": r["resource_path"], "request_count": r["request_count"],
         "total_bytes": r["total_bytes"], "distinct_host_count": r["distinct_host_count"]}
        for r in q2_rows
    ]
    db_loader.load_q2(run_id, "mongodb", batch_count, q2_out)
    print(f"[MongoDB] Q2: {len(q2_out)} rows")

    # Q3
    q3_rows = list(col.aggregate(Q3_PIPELINE, allowDiskUse=True))
    q3_out = [
        {"log_date": r["log_date"], "log_hour": r["log_hour"],
         "error_request_count": r["error_request_count"],
         "total_request_count": r["total_request_count"],
         "error_rate": round(r.get("error_rate", 0), 6),
         "distinct_error_hosts": r["distinct_error_hosts"]}
        for r in q3_rows
    ]
    db_loader.load_q3(run_id, "mongodb", batch_count, q3_out)
    print(f"[MongoDB] Q3: {len(q3_out)} rows")

    # ── Finish run ────────────────────────────────────────────────────────
    runtime = time.perf_counter() - t_start
    avg_bs  = round(total_records / batch_count, 2) if batch_count else 0

    db_loader.finish_run(run_id, total_records, total_malformed,
                         batch_count, avg_bs, round(runtime, 3))

    print(f"\n[MongoDB] Done in {runtime:.2f}s | "
          f"{total_records:,} records | {total_malformed:,} malformed | "
          f"{batch_count} batches")
    client.close()
    return run_id
