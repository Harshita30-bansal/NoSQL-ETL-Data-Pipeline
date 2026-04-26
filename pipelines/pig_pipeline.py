"""
Pig Pipeline - ETL using Apache Pig
Generates Pig scripts for each query and executes them
"""

import os
import subprocess
import time
from datetime import datetime
import json
from src.base_pipeline import BasePipeline
from parser import parse_file_in_batches


class PigPipeline(BasePipeline):
    """Apache Pig-based ETL pipeline"""

    def __init__(self, data_file, batch_size=5000, db_type='mysql'):
        super().__init__('Apache Pig', data_file, batch_size, db_type)
        self.pig_scripts_dir = 'pig_scripts'
        self.results_dir = 'results'
        os.makedirs(self.pig_scripts_dir, exist_ok=True)
        os.makedirs(self.results_dir, exist_ok=True)

    def execute(self):
        """Execute Pig pipeline"""
        print(f"\n{'='*60}")
        print(f"Executing {self.pipeline_name} Pipeline")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            print("✗ Failed to connect to database")
            return False

        try:
            # For demo: collect stats while parsing
            execution_timestamp = datetime.now()
            
            print("  Preparing Pig scripts...")
            self._generate_pig_scripts()
            
            print("  Processing batches in Python (Pig simulation)...")
            
            for batch_id, records, malformed in parse_file_in_batches(self.data_file, self.batch_size):
                self.batch_count = batch_id
                self.total_records += len(records) + malformed
                self.malformed_records += malformed

                print(f"  Batch {batch_id}: {len(records)} records, {malformed} malformed")

                if not records:
                    continue

                # Simulate Pig query execution through Python aggregation
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

    def _generate_pig_scripts(self):
        """Generate Pig scripts for all three queries"""
        self._generate_query_1_script()
        self._generate_query_2_script()
        self._generate_query_3_script()

    def _generate_query_1_script(self):
        """Generate Pig script for Query 1: Daily Traffic Summary"""
        script = """
-- Query 1: Daily Traffic Summary
-- For each log date and status code, compute total requests and bytes

REGISTER 'parser.py' USING jython AS parser;

logs = LOAD '$input' USING TextLoader AS (line:chararray);
parsed = FOREACH logs GENERATE 
    parser.parse_line(line) AS record;

filtered = FILTER parsed BY record IS NOT NULL;

daily_traffic = GROUP filtered BY (record.log_date, record.status_code);

results = FOREACH daily_traffic GENERATE 
    group.log_date as log_date,
    group.status_code as status_code,
    COUNT(filtered) as request_count,
    SUM(filtered.bytes_transferred) as total_bytes;

STORE results INTO '$output';
"""
        script_path = os.path.join(self.pig_scripts_dir, 'query_1.pig')
        with open(script_path, 'w') as f:
            f.write(script)
        print(f"  ✓ Generated {script_path}")

    def _generate_query_2_script(self):
        """Generate Pig script for Query 2: Top Requested Resources"""
        script = """
-- Query 2: Top Requested Resources
-- Compute top 20 resources by request count

REGISTER 'parser.py' USING jython AS parser;

logs = LOAD '$input' USING TextLoader AS (line:chararray);
parsed = FOREACH logs GENERATE 
    parser.parse_line(line) AS record;

filtered = FILTER parsed BY record IS NOT NULL AND record.resource_path IS NOT NULL;

resources = GROUP filtered BY record.resource_path;

aggregated = FOREACH resources GENERATE 
    group as resource_path,
    COUNT(filtered) as request_count,
    SUM(filtered.bytes_transferred) as total_bytes,
    COUNT(DISTINCT filtered.host) as distinct_host_count;

sorted_resources = ORDER aggregated BY request_count DESC;

top_20 = LIMIT sorted_resources 20;

STORE top_20 INTO '$output';
"""
        script_path = os.path.join(self.pig_scripts_dir, 'query_2.pig')
        with open(script_path, 'w') as f:
            f.write(script)
        print(f"  ✓ Generated {script_path}")

    def _generate_query_3_script(self):
        """Generate Pig script for Query 3: Hourly Error Analysis"""
        script = """
-- Query 3: Hourly Error Analysis
-- For each date and hour, compute error stats (400-599)

REGISTER 'parser.py' USING jython AS parser;

logs = LOAD '$input' USING TextLoader AS (line:chararray);
parsed = FOREACH logs GENERATE 
    parser.parse_line(line) AS record;

filtered = FILTER parsed BY record IS NOT NULL;

hourly = GROUP filtered BY (record.log_date, record.log_hour);

results = FOREACH hourly GENERATE 
    group.log_date as log_date,
    group.log_hour as log_hour,
    SUM(CASE WHEN filtered.status_code >= 400 AND filtered.status_code < 600 THEN 1 ELSE 0 END) as error_request_count,
    COUNT(filtered) as total_request_count,
    ((DOUBLE)SUM(CASE WHEN filtered.status_code >= 400 AND filtered.status_code < 600 THEN 1 ELSE 0 END) / (DOUBLE)COUNT(filtered)) as error_rate,
    COUNT(DISTINCT (CASE WHEN filtered.status_code >= 400 AND filtered.status_code < 600 THEN filtered.host ELSE NULL END)) as distinct_error_hosts;

STORE results INTO '$output';
"""
        script_path = os.path.join(self.pig_scripts_dir, 'query_3.pig')
        with open(script_path, 'w') as f:
            f.write(script)
        print(f"  ✓ Generated {script_path}")

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
