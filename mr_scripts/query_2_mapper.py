
import sys
import re

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
    if not resource:
        return None
    bytes_val = 0 if bytes_str == '-' else int(bytes_str)
    return {'host': host, 'resource': resource, 'bytes': bytes_val}

for line in sys.stdin:
    parsed = parse_line(line)
    if parsed:
        print(f"{parsed['resource']}\t1\t{parsed['bytes']}\t{parsed['host']}")
