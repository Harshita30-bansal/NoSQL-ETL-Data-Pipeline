"""
pig_scripts/pig_udf.py
──────────────────────
Jython UDF used by all three Pig scripts.
Must be on the same node as the Pig script (shipped via -file or REGISTER).

parse_line(raw_line) → tuple or None
"""

import re
from datetime import datetime

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


@outputSchema("t:(host:chararray, log_date:chararray, log_hour:int, "
              "http_method:chararray, resource_path:chararray, "
              "protocol_version:chararray, status_code:int, "
              "bytes_transferred:long)")
def parse_line(raw_line):
    if raw_line is None:
        return None
    line = raw_line.strip()
    m = LOG_PATTERN.match(line)
    if not m:
        return None

    host     = m.group("host")
    ts_raw   = m.group("timestamp")
    request  = m.group("request")
    status   = m.group("status")
    bytes_raw = m.group("bytes")

    try:
        dt = datetime.strptime(ts_raw, TIMESTAMP_FORMAT)
        log_date = dt.strftime("%Y-%m-%d")
        log_hour = dt.hour
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
        http_method       = rm.group("method")
        resource_path     = rm.group("path")
        protocol_version  = rm.group("protocol") or "UNKNOWN"
    else:
        http_method       = "UNKNOWN"
        resource_path     = request or "/"
        protocol_version  = "UNKNOWN"

    return (host, log_date, log_hour, http_method,
            resource_path, protocol_version, status_code, bytes_transferred)