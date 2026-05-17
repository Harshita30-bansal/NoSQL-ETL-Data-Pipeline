#!/usr/bin/env python3
"""
mr_scripts/reducer.py
──────────────────────────────────────────────────────────────────
Hadoop Streaming reducer.
Reads sorted key\tvalue lines from stdin (Hadoop guarantees sort).
Emits one JSON object per group to stdout; the pipeline driver
parses these and loads them into PostgreSQL.

Output format (one JSON per line, prefixed by query tag):
  RESULT_Q1\t<json>
  RESULT_Q2\t<json>
  RESULT_Q3\t<json>
"""

import sys
import json
from collections import defaultdict


def flush_q1(log_date, status_code, total_bytes, count):
    if log_date is None:
        return
    obj = {
        "log_date":      log_date,
        "status_code":   int(status_code),
        "request_count": count,
        "total_bytes":   total_bytes,
    }
    print(f"RESULT_Q1\t{json.dumps(obj)}")


def flush_q2(resource_path, total_bytes, count, hosts: set):
    if resource_path is None:
        return
    obj = {
        "resource_path":      resource_path,
        "request_count":      count,
        "total_bytes":        total_bytes,
        "distinct_host_count": len(hosts),
    }
    print(f"RESULT_Q2\t{json.dumps(obj)}")


def flush_q3(log_date, log_hour, err_count, total_count, err_hosts: set):
    if log_date is None:
        return
    error_rate = round(err_count / total_count, 4) if total_count > 0 else 0.0
    obj = {
        "log_date":             log_date,
        "log_hour":             int(log_hour),
        "error_request_count":  err_count,
        "total_request_count":  total_count,
        "error_rate":           error_rate,
        "distinct_error_hosts": len(err_hosts),
    }
    print(f"RESULT_Q3\t{json.dumps(obj)}")


# ── main reduce loop ─────────────────────────────────────────────────────────

current_key = None

# Q1 accumulators
q1_log_date = q1_status = None
q1_bytes = q1_count = 0

# Q2 accumulators
q2_path = None
q2_bytes = q2_count = 0
q2_hosts: set = set()

# Q3 accumulators
q3_date = q3_hour = None
q3_err_count = q3_total = 0
q3_err_hosts: set = set()


for line in sys.stdin:
    line = line.rstrip("\n")
    parts = line.split("\t")
    if len(parts) < 2:
        continue

    query_tag = parts[0]

    # ── Q1 ──────────────────────────────────────────────────────────────────
    if query_tag == "q1":
        # parts: q1, log_date, status_code, bytes
        _, log_date, status_code, bytes_str = parts
        key = ("q1", log_date, status_code)

        if key != current_key:
            flush_q1(q1_log_date, q1_status, q1_bytes, q1_count)
            current_key = key
            q1_log_date, q1_status = log_date, status_code
            q1_bytes, q1_count = 0, 0

        q1_bytes += int(bytes_str) if bytes_str.isdigit() else 0
        q1_count += 1

    # ── Q2 ──────────────────────────────────────────────────────────────────
    elif query_tag == "q2":
        # parts: q2, resource_path, bytes, host
        _, resource_path, bytes_str, host = parts
        key = ("q2", resource_path)

        if key != current_key:
            flush_q2(q2_path, q2_bytes, q2_count, q2_hosts)
            current_key = key
            q2_path = resource_path
            q2_bytes, q2_count = 0, 0
            q2_hosts = set()

        q2_bytes += int(bytes_str) if bytes_str.isdigit() else 0
        q2_count += 1
        q2_hosts.add(host)

    # ── Q3 ──────────────────────────────────────────────────────────────────
    elif query_tag == "q3":
        # parts: q3, log_date, log_hour, is_error, host
        _, log_date, log_hour, is_error_str, host = parts
        key = ("q3", log_date, log_hour)

        if key != current_key:
            flush_q3(q3_date, q3_hour, q3_err_count, q3_total, q3_err_hosts)
            current_key = key
            q3_date, q3_hour = log_date, log_hour
            q3_err_count, q3_total = 0, 0
            q3_err_hosts = set()

        is_error = int(is_error_str)
        q3_total += 1
        if is_error:
            q3_err_count += 1
            q3_err_hosts.add(host)

# ── flush last group ─────────────────────────────────────────────────────────

flush_q1(q1_log_date, q1_status, q1_bytes, q1_count)
flush_q2(q2_path, q2_bytes, q2_count, q2_hosts)
flush_q3(q3_date, q3_hour, q3_err_count, q3_total, q3_err_hosts)