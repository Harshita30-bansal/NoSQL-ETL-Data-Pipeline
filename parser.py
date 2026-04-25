import re
from datetime import datetime

# Month abbreviation to number mapping
MONTH_MAP = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
}

# Main regex pattern — matches a complete valid log line
# Breakdown:
#   (\S+)          → host
#   \S+ \S+        → ident and auth (skip)
#   \[(\d{2})/(\w{3})/(\d{4}):(\d{2}):\d{2}:\d{2} [^\]]+\]  → timestamp parts
#   "([A-Z]+)?     → http method (optional)
#   \s*(\S+)?      → resource path (optional)
#   \s*(HTTP[^\s"]*)?"   → protocol version (optional)
#   \s+(\d{3})     → status code
#   \s+(\S+)       → bytes (number or dash)

LOG_PATTERN = re.compile(
    r'(\S+) \S+ \S+ '
    r'\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):\d{2}:\d{2} [^\]]+\] '
    r'"([A-Z]+)? ?([^\s"]+)? ?(HTTP[^\s"]*)?" '
    r'(\d{3}) (\S+)'
)


def parse_line(line):
    """
    Parse one raw log line.

    Returns a dict with all fields if successful.
    Returns None if the line is malformed and cannot be parsed.

    Never raises an exception — all errors are caught and return None.
    """
    line = line.strip()

    if not line:
        return None

    try:
        match = LOG_PATTERN.match(line)

        if not match:
            # Line does not match expected format at all
            return None

        (host, day, month_str, year, hour,
         method, resource, protocol,
         status_str, bytes_str) = match.groups()

        # Convert month abbreviation to number
        month = MONTH_MAP.get(month_str)
        if month is None:
            return None  # Unknown month abbreviation

        # Build log_date as a proper date string YYYY-MM-DD
        log_date = f"{year}-{month}-{day.zfill(2)}"

        # log_hour is just the integer hour (0–23)
        log_hour = int(hour)

        # Status code as integer
        status_code = int(status_str)

        # Bytes: dash means 0, otherwise parse as integer
        if bytes_str == '-':
            bytes_transferred = 0
        else:
            try:
                bytes_transferred = int(bytes_str)
            except ValueError:
                bytes_transferred = 0  # Some lines have malformed bytes

        # Return the fully parsed record as a dict
        return {
            'host':             host,
            'log_date':         log_date,       # e.g. "1995-07-01"
            'log_hour':         log_hour,        # e.g. 0
            'http_method':      method,          # e.g. "GET" or None
            'resource_path':    resource,        # e.g. "/history/apollo/"
            'protocol_version': protocol,        # e.g. "HTTP/1.0" or None
            'status_code':      status_code,     # e.g. 200
            'bytes_transferred': bytes_transferred  # e.g. 6245
        }

    except Exception:
        # Catch-all: anything unexpected means malformed
        return None


def parse_file_in_batches(filepath, batch_size):
    """
    Generator function that reads the log file and yields one batch at a time.

    Each batch is a tuple: (batch_id, good_records, malformed_count)
    - batch_id: integer starting from 1
    - good_records: list of parsed dicts
    - malformed_count: number of lines in this batch that failed parsing
    """
    batch_id = 0
    good_records = []
    malformed_count = 0
    records_in_current_batch = 0

    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            # Count this line toward the current batch regardless of parse result
            records_in_current_batch += 1

            parsed = parse_line(line)
            if parsed is None:
                malformed_count += 1
            else:
                good_records.append(parsed)

            # When batch is full, yield it
            if records_in_current_batch == batch_size:
                batch_id += 1
                yield (batch_id, good_records, malformed_count)
                # Reset for next batch
                good_records = []
                malformed_count = 0
                records_in_current_batch = 0

    # Yield the final partial batch if it has any lines
    if records_in_current_batch > 0:
        batch_id += 1
        yield (batch_id, good_records, malformed_count)
