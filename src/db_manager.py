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
        """Execute a SELECT query"""
        try:
            
            import psycopg2.extras
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f"✗ Query execution failed: {e}")
            return None

    def execute_update(self, query, params=None):
        """Execute INSERT/UPDATE/DELETE query"""
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
            print(f"✗ Update execution failed: {e}")
            self.connection.rollback()
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

    def insert_daily_traffic_results(self, results_list, run_id, pipeline_name, 
                                      batch_id, execution_timestamp):
        """Insert daily traffic summary results"""
        query = """
            INSERT INTO daily_traffic_summary 
            (run_id, pipeline_name, log_date, status_code, request_count, 
             total_bytes, batch_id, execution_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for result in results_list:
            params = (run_id, pipeline_name, result['log_date'], result['status_code'],
                     result['request_count'], result['total_bytes'], batch_id, execution_timestamp)
            if not self.execute_update(query, params):
                return False
        return True

    def insert_top_resources_results(self, results_list, run_id, pipeline_name,
                                      batch_id, execution_timestamp):
        """Insert top resources results"""
        query = """
            INSERT INTO top_resources 
            (run_id, pipeline_name, resource_path, request_count, total_bytes, 
             distinct_host_count, rank, batch_id, execution_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for rank, result in enumerate(results_list, 1):
            params = (run_id, pipeline_name, result['resource_path'], 
                     result['request_count'], result['total_bytes'],
                     result['distinct_host_count'], rank, batch_id, execution_timestamp)
            if not self.execute_update(query, params):
                return False
        return True

    def insert_error_analysis_results(self, results_list, run_id, pipeline_name,
                                       batch_id, execution_timestamp):
        """Insert hourly error analysis results"""
        query = """
            INSERT INTO hourly_error_analysis 
            (run_id, pipeline_name, log_date, log_hour, error_request_count, 
             total_request_count, error_rate, distinct_error_hosts, batch_id, execution_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for result in results_list:
            params = (run_id, pipeline_name, result['log_date'], result['log_hour'],
                     result['error_request_count'], result['total_request_count'],
                     result['error_rate'], result['distinct_error_hosts'], batch_id, execution_timestamp)
            if not self.execute_update(query, params):
                return False
        return True

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
