"""
parser.py – Shared log-line parser for NASA Combined Log Format.

Each line looks like:
  host - - [timestamp] "METHOD /path HTTP/1.x" status bytes

Returns a dict with extracted fields, or None if unparseable.
Malformed lines are counted – never silently dropped.
"""

import re
from datetime import datetime

# ─── Regex ───────────────────────────────────────────────────────────────────
LOG_PATTERN = re.compile(
    r'^(?P<host>\S+)'           # host
    r'\s+-\s+-\s+'              # - -
    r'\[(?P<timestamp>[^\]]+)\]'# [timestamp]
    r'\s+"(?P<request>[^"]*)"'  # "request"
    r'\s+(?P<status>\d{3}|-)'   # status code
    r'\s+(?P<bytes>\S+)'        # bytes (or -)
)

REQUEST_PATTERN = re.compile(
    r'^(?P<method>[A-Z]+)\s+(?P<path>\S+)(?:\s+(?P<protocol>HTTP/\S+))?$'
)

TIMESTAMP_FORMAT = "%d/%b/%Y:%H:%M:%S %z"


def parse_line(line: str):
    """
    Parse a single log line.

    Returns:
        dict  – structured record on success
        None  – if the line cannot be matched (caller counts as malformed)
    """
    line = line.strip()
    if not line:
        return None

    m = LOG_PATTERN.match(line)
    if not m:
        return None

    host      = m.group("host")
    ts_raw    = m.group("timestamp")
    request   = m.group("request")
    status    = m.group("status")
    bytes_raw = m.group("bytes")

    # ── timestamp ──────────────────────────────────────────────────────────
    try:
        dt       = datetime.strptime(ts_raw, TIMESTAMP_FORMAT)
        log_date = dt.strftime("%Y-%m-%d")
        log_hour = dt.hour
        timestamp_iso = dt.isoformat()
    except ValueError:
        return None

    # ── status ─────────────────────────────────────────────────────────────
    if status == "-":
        return None
    status_code = int(status)

    # ── bytes ──────────────────────────────────────────────────────────────
    bytes_transferred = 0
    if bytes_raw not in ("-", ""):
        try:
            bytes_transferred = int(bytes_raw)
        except ValueError:
            bytes_transferred = 0

    # ── request string ─────────────────────────────────────────────────────
    rm = REQUEST_PATTERN.match(request.strip())
    if rm:
        http_method      = rm.group("method")
        resource_path    = rm.group("path")
        protocol_version = rm.group("protocol") or "UNKNOWN"
    else:
        # Request field is malformed – record as UNKNOWN but don't drop
        http_method      = "UNKNOWN"
        resource_path    = request or "/"
        protocol_version = "UNKNOWN"

    return {
        "host":             host,
        "timestamp":        timestamp_iso,
        "log_date":         log_date,
        "log_hour":         log_hour,
        "http_method":      http_method,
        "resource_path":    resource_path,
        "protocol_version": protocol_version,
        "status_code":      status_code,
        "bytes_transferred": bytes_transferred,
    }


def parse_file_batched(filepath: str, batch_size: int):
    """
    Generator: yields (batch_id, records, malformed_count) tuples.

    Each batch contains up to `batch_size` log lines.
    Malformed lines are counted per batch – never silently dropped.
    """
    batch_id       = 0
    batch_records  = []
    batch_malformed = 0

    with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            record = parse_line(line)
            if record is None:
                batch_malformed += 1
            else:
                batch_records.append(record)

            if (len(batch_records) + batch_malformed) >= batch_size:
                batch_id += 1
                yield batch_id, batch_records, batch_malformed
                batch_records   = []
                batch_malformed = 0

    # Yield the final (possibly smaller) batch
    if batch_records or batch_malformed:
        batch_id += 1
        yield batch_id, batch_records, batch_malformed
