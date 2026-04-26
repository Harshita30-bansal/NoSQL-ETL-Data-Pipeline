"""
Hive Pipeline - ETL using Apache Hive
Generates Hive SQL scripts and executes them
"""

import os
import subprocess
import time
from datetime import datetime
import json
from src.base_pipeline import BasePipeline
from parser import parse_file_in_batches


class HivePipeline(BasePipeline):
    """Apache Hive-based ETL pipeline"""

    def __init__(self, data_file, batch_size=5000, db_type='mysql'):
        super().__init__('Apache Hive', data_file, batch_size, db_type)
        self.hive_scripts_dir = 'hive_scripts'
        self.results_dir = 'results'
        os.makedirs(self.hive_scripts_dir, exist_ok=True)
        os.makedirs(self.results_dir, exist_ok=True)

    def execute(self):
        """Execute Hive pipeline"""
        print(f"\n{'='*60}")
        print(f"Executing {self.pipeline_name} Pipeline")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            print("✗ Failed to connect to database")
            return False

        try:
            execution_timestamp = datetime.now()
            
            print("  Preparing Hive scripts...")
            self._generate_hive_scripts()
            
            print("  Processing batches in Python (Hive simulation)...")
            
            for batch_id, records, malformed in parse_file_in_batches(self.data_file, self.batch_size):
                self.batch_count = batch_id
                self.total_records += len(records) + malformed
                self.malformed_records += malformed

                print(f"  Batch {batch_id}: {len(records)} records, {malformed} malformed")

                if not records:
                    continue

                # Simulate Hive query execution through Python aggregation
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

    def _generate_hive_scripts(self):
        """Generate Hive scripts for all three queries"""
        self._generate_create_table_script()
        self._generate_query_1_script()
        self._generate_query_2_script()
        self._generate_query_3_script()

    def _generate_create_table_script(self):
        """Generate Hive table creation script"""
        script = """
-- Create external table for NASA logs
CREATE EXTERNAL TABLE IF NOT EXISTS nasa_logs (
    host STRING,
    log_date STRING,
    log_hour INT,
    http_method STRING,
    resource_path STRING,
    protocol_version STRING,
    status_code INT,
    bytes_transferred INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '/data/nasa_logs';

-- Create table for Query 1 results
CREATE TABLE IF NOT EXISTS daily_traffic_results (
    log_date STRING,
    status_code INT,
    request_count INT,
    total_bytes BIGINT
)
STORED AS ORC;

-- Create table for Query 2 results
CREATE TABLE IF NOT EXISTS top_resources_results (
    resource_path STRING,
    request_count INT,
    total_bytes BIGINT,
    distinct_host_count INT
)
STORED AS ORC;

-- Create table for Query 3 results
CREATE TABLE IF NOT EXISTS error_analysis_results (
    log_date STRING,
    log_hour INT,
    error_request_count INT,
    total_request_count INT,
    error_rate FLOAT,
    distinct_error_hosts INT
)
STORED AS ORC;
"""
        script_path = os.path.join(self.hive_scripts_dir, 'create_tables.hql')
        with open(script_path, 'w') as f:
            f.write(script)
        print(f"  ✓ Generated {script_path}")

    def _generate_query_1_script(self):
        """Generate Hive script for Query 1: Daily Traffic Summary"""
        script = """
-- Query 1: Daily Traffic Summary
-- For each log date and status code, compute total requests and bytes

INSERT INTO TABLE daily_traffic_results
SELECT 
    log_date,
    status_code,
    COUNT(*) as request_count,
    SUM(bytes_transferred) as total_bytes
FROM nasa_logs
WHERE log_date IS NOT NULL AND status_code IS NOT NULL
GROUP BY log_date, status_code
ORDER BY log_date, status_code;
"""
        script_path = os.path.join(self.hive_scripts_dir, 'query_1.hql')
        with open(script_path, 'w') as f:
            f.write(script)
        print(f"  ✓ Generated {script_path}")

    def _generate_query_2_script(self):
        """Generate Hive script for Query 2: Top Requested Resources"""
        script = """
-- Query 2: Top Requested Resources (Top 20 by request count)

INSERT INTO TABLE top_resources_results
SELECT 
    resource_path,
    COUNT(*) as request_count,
    SUM(bytes_transferred) as total_bytes,
    COUNT(DISTINCT host) as distinct_host_count
FROM nasa_logs
WHERE resource_path IS NOT NULL
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;
"""
        script_path = os.path.join(self.hive_scripts_dir, 'query_2.hql')
        with open(script_path, 'w') as f:
            f.write(script)
        print(f"  ✓ Generated {script_path}")

    def _generate_query_3_script(self):
        """Generate Hive script for Query 3: Hourly Error Analysis"""
        script = """
-- Query 3: Hourly Error Analysis
-- For each date and hour, compute error stats (400-599)

INSERT INTO TABLE error_analysis_results
SELECT 
    log_date,
    log_hour,
    SUM(CASE WHEN status_code >= 400 AND status_code < 600 THEN 1 ELSE 0 END) as error_request_count,
    COUNT(*) as total_request_count,
    CAST(SUM(CASE WHEN status_code >= 400 AND status_code < 600 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as error_rate,
    COUNT(DISTINCT CASE WHEN status_code >= 400 AND status_code < 600 THEN host ELSE NULL END) as distinct_error_hosts
FROM nasa_logs
WHERE log_date IS NOT NULL AND log_hour IS NOT NULL
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour;
"""
        script_path = os.path.join(self.hive_scripts_dir, 'query_3.hql')
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
