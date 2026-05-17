
import sys

current_key = None
current_count = 0
current_bytes = 0

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\t')
    if len(parts) >= 3:
        key = f"{parts[0]}\t{parts[1]}"
        count = int(parts[2])
        bytes_val = int(parts[3])
        
        if key != current_key:
            if current_key:
                print(f"{current_key}\t{current_count}\t{current_bytes}")
            current_key = key
            current_count = 0
            current_bytes = 0
        
        current_count += count
        current_bytes += bytes_val

if current_key:
    print(f"{current_key}\t{current_count}\t{current_bytes}")
