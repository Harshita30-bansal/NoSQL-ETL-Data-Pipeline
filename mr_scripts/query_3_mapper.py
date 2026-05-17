
import sys
import re

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
    is_error = 1 if 400 <= status_code < 600 else 0
    return {'log_date': log_date, 'log_hour': int(hour), 'is_error': is_error, 'host': host, 'status_code': status_code}

for line in sys.stdin:
    parsed = parse_line(line)
    if parsed:
        key = f"{parsed['log_date']}\t{parsed['log_hour']}"
        error_flag = parsed['is_error']
        host_info = f"{parsed['host']}:{error_flag}"
        print(f"{key}\t1\t{error_flag}\t{host_info}")
