"""
parser.py – Shared NASA Combined Log Format parser.
Used by all four pipelines and by the Hadoop Streaming mapper.
"""

import re
from datetime import datetime

# ─── Regex ────────────────────────────────────────────────────────────────────
LOG_PATTERN = re.compile(
    r'^(?P<host>\S+)'
    r'\s+-\s+-\s+'
    r'\[(?P<timestamp>[^\]]+)\]'
    r'\s+"(?P<request>[^"]*)"'
    r'\s+(?P<status>\d{3}|-)'
    r'\s+(?P<bytes>\S+)'
)

REQUEST_PATTERN = re.compile(
    r'^(?P<method>[A-Z]+)\s+(?P<path>\S+)(?:\s+(?P<protocol>HTTP/\S+))?$'
)

TIMESTAMP_FORMAT = "%d/%b/%Y:%H:%M:%S %z"


def parse_line(line: str):
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

    try:
        dt               = datetime.strptime(ts_raw, TIMESTAMP_FORMAT)
        log_date         = dt.strftime("%Y-%m-%d")
        log_hour         = dt.hour
        timestamp_iso    = dt.isoformat()
    except ValueError:
        return None

    if status == "-":
        return None
    status_code = int(status)

    bytes_transferred = 0
    if bytes_raw not in ("-", ""):
        try:
            bytes_transferred = int(bytes_raw)
        except ValueError:
            bytes_transferred = 0

    rm = REQUEST_PATTERN.match(request.strip())
    if rm:
        http_method      = rm.group("method")
        resource_path    = rm.group("path")
        protocol_version = rm.group("protocol") or "UNKNOWN"
    else:
        http_method      = "UNKNOWN"
        resource_path    = request or "/"
        protocol_version = "UNKNOWN"

    return {
        "host":              host,
        "timestamp":         timestamp_iso,
        "log_date":          log_date,
        "log_hour":          log_hour,
        "http_method":       http_method,
        "resource_path":     resource_path,
        "protocol_version":  protocol_version,
        "status_code":       status_code,
        "bytes_transferred": bytes_transferred,
    }


def parse_file_in_batches(filepath: str, batch_size: int):
    """
    Generator: yields (batch_id, records_list, malformed_count) tuples.
    Each file = logically 1 batch (at the pipeline level). Internally
    chunks are read in `batch_size` slices to manage memory.
    """
    batch_id      = 0
    batch_records = []
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

    if batch_records or batch_malformed:
        batch_id += 1
        yield batch_id, batch_records, batch_malformed


# backward compat alias
parse_file_batched = parse_file_in_batches