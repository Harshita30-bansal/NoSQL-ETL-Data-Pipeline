"""
MapReduce Pipeline - ETL using Apache Hadoop MapReduce
Generates MapReduce job code and executes them
"""

import os
import subprocess
import time
from datetime import datetime
import json
from src.base_pipeline import BasePipeline
from parser import parse_file_in_batches


class MapReducePipeline(BasePipeline):
    """Apache Hadoop MapReduce-based pipeline"""

    def __init__(self, data_file, batch_size=5000, db_type='mysql'):
        super().__init__('MapReduce', data_file, batch_size, db_type)
        self.mr_jobs_dir = 'mapreduce'
        self.results_dir = 'results'
        os.makedirs(self.mr_jobs_dir, exist_ok=True)
        os.makedirs(self.results_dir, exist_ok=True)

    def execute(self):
        """Execute MapReduce pipeline"""
        print(f"\n{'='*60}")
        print(f"Executing {self.pipeline_name} Pipeline")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            print("✗ Failed to connect to database")
            return False

        try:
            execution_timestamp = datetime.now()
            
            print("  Preparing MapReduce jobs...")
            self._generate_mapreduce_jobs()
            
            print("  Processing batches in Python (MapReduce simulation)...")
            
            for batch_id, records, malformed in parse_file_in_batches(self.data_file, self.batch_size):
                self.batch_count = batch_id
                self.total_records += len(records) + malformed
                self.malformed_records += malformed

                print(f"  Batch {batch_id}: {len(records)} records, {malformed} malformed")

                if not records:
                    continue

                # Simulate MapReduce query execution through Python aggregation
                from src.query_processor import QueryProcessor
                results = QueryProcessor.process_all_queries(records)

                if not self._store_batch_results(results, batch_id, execution_timestamp):
                    print("✗ Failed to store batch results")
                    return False

            self.end_time = time.time()
            self.save_metadata()

            print(self.get_status_string())
            return True

        except Exception as e:
            print(f"✗ Pipeline execution failed: {e}")
            self.end_time = time.time()
            return False
        finally:
            self.disconnect_db()

    def _generate_mapreduce_jobs(self):
        """Generate MapReduce job source code"""
        self._generate_query_1_mapper()
        self._generate_query_1_reducer()
        self._generate_query_2_mapper()
        self._generate_query_2_reducer()
        self._generate_query_3_mapper()
        self._generate_query_3_reducer()

    def _generate_query_1_mapper(self):
        """Generate Mapper for Query 1: Daily Traffic Summary"""
        mapper = """
import sys
import re
from datetime import datetime

MONTH_MAP = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
             'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
             'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

LOG_PATTERN = re.compile(
    r'(\\S+) \\S+ \\S+ '
    r'\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] '
    r'"([A-Z]+)? ?([^\\s"]+)? ?(HTTP[^\\s"]*)?" '
    r'(\\d{3}) (\\S+)'
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
        key = f"{parsed['log_date']}\\t{parsed['status_code']}"
        value = f"1\\t{parsed['bytes']}"
        print(f"{key}\\t{value}")
"""
        path = os.path.join(self.mr_jobs_dir, 'query_1_mapper.py')
        with open(path, 'w') as f:
            f.write(mapper)
        print(f"  ✓ Generated {path}")

    def _generate_query_1_reducer(self):
        """Generate Reducer for Query 1: Daily Traffic Summary"""
        reducer = """
import sys

current_key = None
current_count = 0
current_bytes = 0

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\\t')
    if len(parts) >= 3:
        key = f"{parts[0]}\\t{parts[1]}"
        count = int(parts[2])
        bytes_val = int(parts[3])
        
        if key != current_key:
            if current_key:
                print(f"{current_key}\\t{current_count}\\t{current_bytes}")
            current_key = key
            current_count = 0
            current_bytes = 0
        
        current_count += count
        current_bytes += bytes_val

if current_key:
    print(f"{current_key}\\t{current_count}\\t{current_bytes}")
"""
        path = os.path.join(self.mr_jobs_dir, 'query_1_reducer.py')
        with open(path, 'w') as f:
            f.write(reducer)
        print(f"  ✓ Generated {path}")

    def _generate_query_2_mapper(self):
        """Generate Mapper for Query 2: Top Resources"""
        mapper = """
import sys
import re

LOG_PATTERN = re.compile(
    r'(\\S+) \\S+ \\S+ '
    r'\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] '
    r'"([A-Z]+)? ?([^\\s"]+)? ?(HTTP[^\\s"]*)?" '
    r'(\\d{3}) (\\S+)'
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
        print(f"{parsed['resource']}\\t1\\t{parsed['bytes']}\\t{parsed['host']}")
"""
        path = os.path.join(self.mr_jobs_dir, 'query_2_mapper.py')
        with open(path, 'w') as f:
            f.write(mapper)
        print(f"  ✓ Generated {path}")

    def _generate_query_2_reducer(self):
        """Generate Reducer for Query 2: Top Resources"""
        reducer = """
import sys

current_resource = None
current_count = 0
current_bytes = 0
current_hosts = set()

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\\t')
    if len(parts) >= 4:
        resource = parts[0]
        count = int(parts[1])
        bytes_val = int(parts[2])
        host = parts[3]
        
        if resource != current_resource:
            if current_resource:
                print(f"{current_resource}\\t{current_count}\\t{current_bytes}\\t{len(current_hosts)}")
            current_resource = resource
            current_count = 0
            current_bytes = 0
            current_hosts = set()
        
        current_count += count
        current_bytes += bytes_val
        current_hosts.add(host)

if current_resource:
    print(f"{current_resource}\\t{current_count}\\t{current_bytes}\\t{len(current_hosts)}")
"""
        path = os.path.join(self.mr_jobs_dir, 'query_2_reducer.py')
        with open(path, 'w') as f:
            f.write(reducer)
        print(f"  ✓ Generated {path}")

    def _generate_query_3_mapper(self):
        """Generate Mapper for Query 3: Error Analysis"""
        mapper = """
import sys
import re

MONTH_MAP = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
             'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
             'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

LOG_PATTERN = re.compile(
    r'(\\S+) \\S+ \\S+ '
    r'\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2}):\\d{2}:\\d{2} [^\\]]+\\] '
    r'"([A-Z]+)? ?([^\\s"]+)? ?(HTTP[^\\s"]*)?" '
    r'(\\d{3}) (\\S+)'
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
        key = f"{parsed['log_date']}\\t{parsed['log_hour']}"
        error_flag = parsed['is_error']
        host_info = f"{parsed['host']}:{error_flag}"
        print(f"{key}\\t1\\t{error_flag}\\t{host_info}")
"""
        path = os.path.join(self.mr_jobs_dir, 'query_3_mapper.py')
        with open(path, 'w') as f:
            f.write(mapper)
        print(f"  ✓ Generated {path}")

    def _generate_query_3_reducer(self):
        """Generate Reducer for Query 3: Error Analysis"""
        reducer = """
import sys

current_key = None
total_count = 0
error_count = 0
error_hosts = set()

for line in sys.stdin:
    line = line.strip()
    parts = line.split('\\t')
    if len(parts) >= 4:
        key = f"{parts[0]}\\t{parts[1]}"
        count = int(parts[2])
        error_flag = int(parts[3])
        host_info = parts[4]
        
        if key != current_key:
            if current_key:
                error_rate = (error_count / total_count) if total_count > 0 else 0
                print(f"{current_key}\\t{error_count}\\t{total_count}\\t{error_rate:.4f}\\t{len(error_hosts)}")
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
    print(f"{current_key}\\t{error_count}\\t{total_count}\\t{error_rate:.4f}\\t{len(error_hosts)}")
"""
        path = os.path.join(self.mr_jobs_dir, 'query_3_reducer.py')
        with open(path, 'w') as f:
            f.write(reducer)
        print(f"  ✓ Generated {path}")

    def _store_batch_results(self, results, batch_id, execution_timestamp):
        """Store query results in database"""
        try:
            if results['query_1']:
                self.db_manager.insert_daily_traffic_results(
                    results['query_1'], self.run_id, self.pipeline_name, batch_id, execution_timestamp
                )

            if results['query_2']:
                self.db_manager.insert_top_resources_results(
                    results['query_2'], self.run_id, self.pipeline_name, batch_id, execution_timestamp
                )

            if results['query_3']:
                self.db_manager.insert_error_analysis_results(
                    results['query_3'], self.run_id, self.pipeline_name, batch_id, execution_timestamp
                )

            return True
        except Exception as e:
            print(f"✗ Failed to store results: {e}")
            return False
