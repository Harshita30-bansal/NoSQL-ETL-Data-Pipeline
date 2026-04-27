"""
Database Manager - handles all database operations for result storage
Supports both MySQL and PostgreSQL
"""

import pymysql
import psycopg2
from datetime import datetime
import json
from config import DB_CONFIG, DEFAULT_DB


class DatabaseManager:
    def __init__(self, db_type=DEFAULT_DB):
        self.db_type = db_type
        self.connection = None
        self.config = DB_CONFIG[db_type]

    def connect(self):
        """Establish database connection"""
        try:
            if self.db_type == 'mysql':
                self.connection = pymysql.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['user'],
                    password=self.config['password'],
                    database=self.config['database'],
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor
                )
            elif self.db_type == 'postgresql':
                self.connection = psycopg2.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    user=self.config['user'],
                    password=self.config['password'],
                    database=self.config['database']
                )
            print(f"✓ Connected to {self.db_type}")
            return True
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print(f"✓ Disconnected from {self.db_type}")

    def execute_query(self, query, params=None):
        """Execute a SELECT query and return results as list of dicts"""
        try:
            # PostgreSQL requires a DictCursor/RealDictCursor for result['column'] access
            if self.db_type == 'postgresql':
                import psycopg2.extras
                cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            else:
                cursor = self.connection.cursor()
                
            cursor.execute(query, params) if params else cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"✗ Query failed: {e}")
            return None

    def execute_update(self, query, params=None):
        """Execute INSERT/UPDATE/DELETE query with explicit error logging"""
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            # THIS PRINT IS CRITICAL - it will tell us the exact column causing the fail
            print(f"✗ Database Error: {e}")
            if self.connection:
                self.connection.rollback() # Crucial: Clean the state for the next insert
            return False

    def insert_execution_metadata(self, run_id, pipeline_name, batch_size, total_batches,
                                   total_records, malformed_records, avg_batch_size,
                                   execution_time_ms, data_file):
        """Store execution metadata"""
        query = """
            INSERT INTO execution_metadata 
            (run_id, pipeline_name, batch_size, total_batches, total_records, 
             malformed_records, avg_batch_size, execution_start, execution_end, 
             execution_time_ms, data_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        now = datetime.now()
        params = (run_id, pipeline_name, batch_size, total_batches, total_records,
                  malformed_records, avg_batch_size, now, now, execution_time_ms, data_file)
        return self.execute_update(query, params)

    def insert_daily_traffic_results(self, results_list, run_id, pipeline_name, batch_id, execution_timestamp):
        # ADD THIS PRINT
        print(f"  DEBUG: Attempting to insert {len(results_list)} rows for Run: {run_id}")
        
        query = """
            INSERT INTO daily_traffic_summary 
            (run_id, pipeline_name, log_date, status_code, request_count, total_bytes, batch_id, execution_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for result in results_list:
            params = (run_id, pipeline_name, result['log_date'], result['status_code'],
                     result['request_count'], result['total_bytes'], batch_id, execution_timestamp)
            if not self.execute_update(query, params):
                return False
        return True

    def insert_top_resources_results(self, results_list, run_id, pipeline_name, batch_id, execution_timestamp):
        if not results_list: return True
        query = """
            INSERT INTO top_resources 
            (run_id, pipeline_name, resource_path, request_count, total_bytes, distinct_host_count, rank, batch_id, execution_timestamp)
            VALUES %s
        """
        data = [(run_id, pipeline_name, r['resource_path'], r['request_count'], r['total_bytes'], r['distinct_host_count'], r.get('rank', i+1), batch_id, execution_timestamp) for i, r in enumerate(results_list)]
        print(f"  DEBUG: Sending {len(data)} rows to top_resources...")
        return self._execute_batch(query, data)

    def insert_error_analysis_results(self, results_list, run_id, pipeline_name, batch_id, execution_timestamp):
        if not results_list: return True
        query = """
            INSERT INTO hourly_error_analysis 
            (run_id, pipeline_name, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts, batch_id, execution_timestamp)
            VALUES %s
        """
        data = [(run_id, pipeline_name, r['log_date'], r['log_hour'], r['error_request_count'], r['total_request_count'], r['error_rate'], r['distinct_error_hosts'], batch_id, execution_timestamp) for r in results_list]
        print(f"  DEBUG: Sending {len(data)} rows to hourly_error_analysis...")
        return self._execute_batch(query, data)

    def _execute_batch(self, query, data):
        """Helper for bulk inserts in PostgreSQL"""
        try:
            import psycopg2.extras
            cursor = self.connection.cursor()
            psycopg2.extras.execute_values(cursor, query, data)
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"  ✗ Batch Insert Failed: {e}")
            self.connection.rollback()
            return False   
    

    def get_execution_history(self, pipeline_name=None):
        """Retrieve execution history"""
        if pipeline_name:
            query = "SELECT * FROM execution_metadata WHERE pipeline_name = %s ORDER BY created_at DESC"
            return self.execute_query(query, (pipeline_name,))
        else:
            query = "SELECT * FROM execution_metadata ORDER BY created_at DESC"
            return self.execute_query(query)

    def get_query_results(self, run_id, query_id):
        """Retrieve query results for a specific run"""
        query = "SELECT * FROM query_results WHERE run_id = %s AND query_id = %s"
        return self.execute_query(query, (run_id, query_id))
