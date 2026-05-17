#!/usr/bin/env python3
"""
mr_scripts/mapper.py
──────────────────────────────────────────────────────────────────
Hadoop Streaming mapper.
Reads raw NASA Combined Log Format lines from stdin.
Emits tab-separated key\tvalue pairs for three queries:

  Q1:  q1\t<log_date>\t<status_code>\t<bytes>
  Q2:  q2\t<resource_path>\t<bytes>\t<host>
  Q3:  q3\t<log_date>\t<log_hour>\t<is_error>\t<host>

Run standalone:   cat access_log | python3 mapper.py
Run via Hadoop:   hadoop streaming … -mapper mapper.py
"""

import sys
import os

# Allow import of shared parser when running under Hadoop Streaming.
# Hadoop Streaming puts all -files into the task working directory.
sys.path.insert(0, os.path.dirname(__file__))

from parser import parse_line

ERROR_STATUS_MIN = 400
ERROR_STATUS_MAX = 599


def emit(query_tag: str, *fields):
    key_value = "\t".join(str(f) for f in fields)
    print(f"{query_tag}\t{key_value}")


for raw_line in sys.stdin:
    record = parse_line(raw_line)
    if record is None:
        continue

    # ── Query 1: daily traffic summary ───────────────────────────────────────
    # key = (q1, log_date, status_code)   value = bytes
    emit("q1", record["log_date"], record["status_code"],
         record["bytes_transferred"])

    # ── Query 2: top requested resources ─────────────────────────────────────
    # key = (q2, resource_path)   value = bytes, host
    emit("q2", record["resource_path"],
         record["bytes_transferred"], record["host"])

    # ── Query 3: hourly error analysis ────────────────────────────────────────
    # key = (q3, log_date, log_hour)   value = is_error (0/1), host
    is_error = (
        1 if ERROR_STATUS_MIN <= record["status_code"] <= ERROR_STATUS_MAX
        else 0
    )
    emit("q3", record["log_date"], record["log_hour"], is_error, record["host"])