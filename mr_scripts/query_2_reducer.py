
import sys

current_resource = None
current_count = 0
current_bytes = 0
current_hosts = set()

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t')
    if len(parts) >= 4:
        resource = parts[0]
        count = int(parts[1])
        bytes_val = int(parts[2])
        host = parts[3]
        
        if resource != current_resource:
            if current_resource:
                print(f"{current_resource}\t{current_count}\t{current_bytes}\t{len(current_hosts)}")
            current_resource = resource
            current_count = 0
            current_bytes = 0
            current_hosts = set()
        
        current_count += count
        current_bytes += bytes_val
        current_hosts.add(host)

if current_resource:
    print(f"{current_resource}\t{current_count}\t{current_bytes}\t{len(current_hosts)}")
