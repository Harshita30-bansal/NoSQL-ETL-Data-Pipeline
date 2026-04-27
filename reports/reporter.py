"""
Reporting Module - generates formatted reports from query results
"""

from tabulate import tabulate
from datetime import datetime
from src.db_manager import DatabaseManager


class Reporter:
    """Generate formatted reports from query results"""

    def __init__(self, db_type='mysql'):
        self.db_manager = DatabaseManager(db_type)
        self.db_manager.connect()

    def __del__(self):
        """Cleanup on destruction"""
        if self.db_manager:
            self.db_manager.disconnect()

    def generate_execution_summary(self, run_id):
        """Generate summary of a pipeline execution"""
        query = "SELECT * FROM execution_metadata WHERE run_id = %s"
        results = self.db_manager.execute_query(query, (run_id,))
        
        if not results:
            print(f"No execution found for run ID: {run_id}")
            return

        exec_meta = results[0]
        
        print(f"EXECUTION SUMMARY REPORT - Run ID: {run_id}")
        print("\n\n\n")

        summary_data = [
            ['Pipeline', exec_meta['pipeline_name']],
            ['Execution Timestamp', exec_meta['created_at']],
            ['Data File', exec_meta['data_file']],
            ['Batch Size', f"{exec_meta['batch_size']:,}"],
            ['Total Batches', f"{exec_meta['total_batches']:,}"],
            ['Total Records Processed', f"{exec_meta['total_records']:,}"],
            ['Malformed Records', f"{exec_meta['malformed_records']:,}"],
            ['Average Batch Size', f"{exec_meta['avg_batch_size']:.2f}"],
            ['Execution Time (ms)', f"{exec_meta['execution_time_ms']:,}"],
        ]
        
        print(tabulate(summary_data, headers=['Metric', 'Value'], tablefmt='grid'))

    def generate_query_1_report(self, run_id, limit=20):
        """Generate report for Query 1: Daily Traffic Summary"""
        query = """
            SELECT log_date, status_code, request_count, total_bytes
            FROM daily_traffic_summary
            WHERE run_id = %s
            ORDER BY log_date, status_code
            LIMIT %s
        """
        results = self.db_manager.execute_query(query, (run_id, limit))
        
        if not results:
            print("No results for Query 1")
            return

        print("QUERY 1: DAILY TRAFFIC SUMMARY")
        print("\n\n\n")

        table_data = []
        for row in results:
            table_data.append([
                row['log_date'],
                row['status_code'],
                f"{row['request_count']:,}",
                f"{row['total_bytes']:,}"
            ])

        print(tabulate(table_data, 
                      headers=['Date', 'Status Code', 'Requests', 'Total Bytes'],
                      tablefmt='grid'))

    def generate_query_2_report(self, run_id):
        """Generate report for Query 2: Top Requested Resources"""
        query = """
            SELECT resource_path, request_count, total_bytes, distinct_host_count, rank
            FROM top_resources
            WHERE run_id = %s
            ORDER BY rank
        """
        results = self.db_manager.execute_query(query, (run_id,))
        
        if not results:
            print("No results for Query 2")
            return

        print("QUERY 2: TOP 20 REQUESTED RESOURCES")
        print("\n\n\n")

        table_data = []
        for row in results:
            table_data.append([
                row['rank'],
                row['resource_path'][:50],  # Truncate long paths
                f"{row['request_count']:,}",
                f"{row['total_bytes']:,}",
                f"{row['distinct_host_count']:,}"
            ])

        print(tabulate(table_data, 
                      headers=['Rank', 'Resource Path', 'Requests', 'Total Bytes', 'Distinct Hosts'],
                      tablefmt='grid'))

    def generate_query_3_report(self, run_id, limit=20):
        """Generate report for Query 3: Hourly Error Analysis"""
        query = """
            SELECT log_date, log_hour, error_request_count, total_request_count, 
                   error_rate, distinct_error_hosts
            FROM hourly_error_analysis
            WHERE run_id = %s
            ORDER BY log_date, log_hour
            LIMIT %s
        """
        results = self.db_manager.execute_query(query, (run_id, limit))
        
        if not results:
            print("No results for Query 3")
            return

        print("QUERY 3: HOURLY ERROR ANALYSIS (Status Codes 400-599)")
        print("\n\n\n")

        table_data = []
        for row in results:
            table_data.append([
                row['log_date'],
                row['log_hour'],
                f"{row['error_request_count']:,}",
                f"{row['total_request_count']:,}",
                f"{row['error_rate']:.4f}",
                f"{row['distinct_error_hosts']:,}"
            ])

        print(tabulate(table_data, 
                      headers=['Date', 'Hour', 'Error Requests', 'Total Requests', 'Error Rate', 'Error Hosts'],
                      tablefmt='grid'))

    def generate_full_report(self, run_id):
        """Generate complete report for a run"""
        self.generate_execution_summary(run_id)
        self.generate_query_1_report(run_id, limit=10)
        self.generate_query_2_report(run_id)
        self.generate_query_3_report(run_id, limit=10)

        print("\n\n\n")

    def list_all_executions(self):
        """List all pipeline executions"""
        query = """
            SELECT run_id, pipeline_name, total_records, malformed_records, 
                   total_batches, execution_time_ms, created_at
            FROM execution_metadata
            ORDER BY created_at DESC
        """
        results = self.db_manager.execute_query(query)
        
        if not results:
            print("No executions found")
            return

        print("ALL PIPELINE EXECUTIONS : ")
        print("\n\n\n")

        table_data = []
        for row in results:
            table_data.append([
                row['run_id'],
                row['pipeline_name'],
                f"{row['total_records']:,}",
                f"{row['malformed_records']:,}",
                f"{row['total_batches']:,}",
                f"{row['execution_time_ms']:,} ms",
                row['created_at']
            ])

        print(tabulate(table_data, 
                      headers=['Run ID', 'Pipeline', 'Total Records', 'Malformed', 'Batches', 'Exec Time', 'Created'],
                      tablefmt='grid'))

    def compare_pipelines(self):
        """Compare performance metrics across pipelines"""
        query = """
            SELECT pipeline_name, AVG(execution_time_ms) as avg_time, 
                   MAX(execution_time_ms) as max_time, MIN(execution_time_ms) as min_time,
                   COUNT(*) as run_count, AVG(total_records) as avg_records
            FROM execution_metadata
            GROUP BY pipeline_name
        """
        results = self.db_manager.execute_query(query)
        
        if not results:
            print("No data for comparison")
            return

        print(f"\n{'='*100}")
        print("PIPELINE COMPARISON")
        print(f"{'='*100}\n")

        table_data = []
        for row in results:
            table_data.append([
                row['pipeline_name'],
                f"{row['run_count']:,}",
                f"{row['avg_records']:,.0f}",
                f"{row['avg_time']:,.0f} ms",
                f"{row['min_time']:,.0f} ms",
                f"{row['max_time']:,.0f} ms"
            ])

        print(tabulate(table_data, 
                      headers=['Pipeline', 'Runs', 'Avg Records', 'Avg Time', 'Min Time', 'Max Time'],
                      tablefmt='grid'))
