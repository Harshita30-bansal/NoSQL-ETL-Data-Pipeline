
import sys

current_key = None
total_count = 0
error_count = 0
error_hosts = set()

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t')
    if len(parts) >= 4:
        key = f"{parts[0]}\t{parts[1]}"
        count = int(parts[2])
        error_flag = int(parts[3])
        host_info = parts[4]
        
        if key != current_key:
            if current_key:
                error_rate = (error_count / total_count) if total_count > 0 else 0
                print(f"{current_key}\t{error_count}\t{total_count}\t{error_rate:.4f}\t{len(error_hosts)}")
            current_key = key
            total_count = 0
            error_count = 0
            error_hosts = set()
        
        total_count += count
        error_count += error_flag
        if error_flag == 1:
            host = host_info.split(':')[0]
            error_hosts.add(host)

if current_key:
    error_rate = (error_count / total_count) if total_count > 0 else 0
    print(f"{current_key}\t{error_count}\t{total_count}\t{error_rate:.4f}\t{len(error_hosts)}")
