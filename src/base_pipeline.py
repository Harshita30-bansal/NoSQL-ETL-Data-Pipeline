"""
Base Pipeline - abstract base class for all pipeline implementations
Defines the interface that all concrete pipelines must implement
"""

from abc import ABC, abstractmethod
from datetime import datetime
import time
import uuid
from parser import parse_file_in_batches
from src.db_manager import DatabaseManager
from src.query_processor import QueryProcessor


class BasePipeline(ABC):
    """Abstract base class for ETL pipelines"""

    def __init__(self, pipeline_name, data_file, batch_size=5000, db_type='mysql'):
        self.pipeline_name = pipeline_name
        self.data_file = data_file
        self.batch_size = batch_size
        self.db_type = db_type
        self.run_id = str(uuid.uuid4())[:8]
        self.db_manager = DatabaseManager(db_type)
        
        # Statistics
        self.total_records = 0
        self.malformed_records = 0
        self.batch_count = 0
        self.start_time = None
        self.end_time = None

    @abstractmethod
    def execute(self):
        """Execute the pipeline - must be implemented by subclasses"""
        pass

    def connect_db(self):
        """Connect to database"""
        return self.db_manager.connect()

    def disconnect_db(self):
        """Disconnect from database"""
        self.db_manager.disconnect()

    def get_execution_time_ms(self):
        """Get execution time in milliseconds"""
        if self.start_time and self.end_time:
            return int((self.end_time - self.start_time) * 1000)
        return 0

    def get_avg_batch_size(self):
        """Calculate average batch size"""
        return self.total_records / self.batch_count if self.batch_count > 0 else 0

    def save_metadata(self):
        """Save execution metadata to database"""
        self.db_manager.insert_execution_metadata(
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            batch_size=self.batch_size,
            total_batches=self.batch_count,
            total_records=self.total_records,
            malformed_records=self.malformed_records,
            avg_batch_size=self.get_avg_batch_size(),
            execution_time_ms=self.get_execution_time_ms(),
            data_file=self.data_file
        )

    def get_status_string(self):
        """Get formatted status string"""
        return f"""
┌─ Pipeline Execution Complete ─────────────────────┐
│ Pipeline: {self.pipeline_name:<40} │
│ Run ID: {self.run_id:<43} │
│ Total Records: {self.total_records:<35} │
│ Malformed Records: {self.malformed_records:<30} │
│ Batches Processed: {self.batch_count:<32} │
│ Avg Batch Size: {self.get_avg_batch_size():<33.2f} │
│ Execution Time: {self.get_execution_time_ms():<33} ms │
└───────────────────────────────────────────────────┘
"""


class PythonNativePipeline(BasePipeline):
    """
    Python-native pipeline for all three queries
    Used for testing and as baseline for comparison
    """

    def execute(self):
        """Execute Python-native ETL pipeline"""
        print(f"\n{'='*60}")
        print(f"Executing {self.pipeline_name} Pipeline")
        print(f"{'='*60}\n")

        self.start_time = time.time()

        if not self.connect_db():
            print("✗ Failed to connect to database")
            return False

        try:
            execution_timestamp = datetime.now()
            
            # Process file in batches
            for batch_id, records, malformed in parse_file_in_batches(self.data_file, self.batch_size):
                self.batch_count = batch_id
                self.total_records += len(records) + malformed
                self.malformed_records += malformed

                print(f"  Processing Batch {batch_id}: {len(records)} good records, {malformed} malformed")

                if not records:
                    continue

                # Process all queries on this batch
                results = QueryProcessor.process_all_queries(records)

                # Store results in database
                if not self._store_batch_results(results, batch_id, execution_timestamp):
                    print("✗ Failed to store batch results")
                    return False

            self.end_time = time.time()

            # Save metadata
            self.save_metadata()

            print(self.get_status_string())
            return True

        except Exception as e:
            print(f"✗ Pipeline execution failed: {e}")
            self.end_time = time.time()
            return False
        finally:
            self.disconnect_db()

    def _store_batch_results(self, results, batch_id, execution_timestamp):
        """Store query results in database"""
        try:
            # Store Query 1 results
            if results['query_1']:
                self.db_manager.insert_daily_traffic_results(
                    results['query_1'], self.run_id, self.pipeline_name, batch_id, execution_timestamp
                )

            # Store Query 2 results
            if results['query_2']:
                self.db_manager.insert_top_resources_results(
                    results['query_2'], self.run_id, self.pipeline_name, batch_id, execution_timestamp
                )

            # Store Query 3 results
            if results['query_3']:
                self.db_manager.insert_error_analysis_results(
                    results['query_3'], self.run_id, self.pipeline_name, batch_id, execution_timestamp
                )

            return True
        except Exception as e:
            print(f"✗ Failed to store results: {e}")
            return False
