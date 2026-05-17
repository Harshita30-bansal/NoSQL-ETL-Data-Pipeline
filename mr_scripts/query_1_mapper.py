
import sys
import re
from datetime import datetime

MONTH_MAP = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
             'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
             'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

LOG_PATTERN = re.compile(
    r'(\S+) \S+ \S+ '
    r'\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):\d{2}:\d{2} [^\]]+\] '
    r'"([A-Z]+)? ?([^\s"]+)? ?(HTTP[^\s"]*)?" '
    r'(\d{3}) (\S+)'
)

def parse_line(line):
    match = LOG_PATTERN.match(line.strip())
    if not match:
        return None
    groups = match.groups()
    host, day, month_str, year, hour, method, resource, protocol, status_str, bytes_str = groups
    month = MONTH_MAP.get(month_str)
    if not month:
        return None
    log_date = f"{year}-{month}-{day.zfill(2)}"
    status_code = int(status_str)
    return {'log_date': log_date, 'status_code': status_code, 'bytes': 0 if bytes_str == '-' else int(bytes_str)}

for line in sys.stdin:
    parsed = parse_line(line)
    if parsed:
        key = f"{parsed['log_date']}\t{parsed['status_code']}"
        value = f"1\t{parsed['bytes']}"
        print(f"{key}\t{value}")
