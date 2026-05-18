"""
Hive Pipeline - ETL using Java-based Hive analytics.
The Python wrapper compiles and launches a Java engine that processes log files,
returns query results as JSON, and stores them in PostgreSQL.
"""
import json
import os
import subprocess
import time
from datetime import datetime
from src.base_pipeline import BasePipeline
from config import HIVE_SCRIPTS_DIR
class HivePipeline(BasePipeline):
    """Apache Hive-like pipeline implemented in Java."""
    def __init__(self, data_file, batch_size=5000, db_type='postgresql' ,query_name='all'):
        super().__init__('Apache Hive', data_file, batch_size, db_type)
        self.query_name = query_name
        self.data_files = data_file if isinstance(data_file, list) else [data_file]
        self.java_dir = os.path.join(HIVE_SCRIPTS_DIR, 'java_hive')
        self.java_source = os.path.join(self.java_dir, 'HiveAnalytics.java')
        self.java_class = 'HiveAnalytics'
    def execute(self) -> bool:
        print(f"\n{'='*60}")
        print(f"Executing {self.pipeline_name} Pipeline")
        print(f"{'='*60}\n")
        self.start_time = time.time()
        if not self.connect_db():
            print("✗ Failed to connect to database")
            return False
        if not self._compile_java():
            print("✗ Failed to compile Java Hive analytics")
            self.disconnect_db()
            return False
        try:
            print("  Executing Java Hive analytics pipeline...")
            result = self._run_java_pipeline()
            if not result:
                return False
            q1_rows = result.get('q1', [])
            q2_rows = result.get('q2', [])
            q3_rows = result.get('q3', [])
            self.total_records = result.get('total_records', 0)
            self.malformed_records = result.get('malformed_records', 0)
            self.batch_count = result.get('batch_count', len(self.data_files))
            avg_batch_size = self.total_records / self.batch_count
            largest_batch_size = avg_batch_size
            print(
                f"  ✓ Java analytics returned {self.total_records:,} records "
                f"({self.malformed_records:,} malformed)"
            )
            elapsed_ms = int((time.time() - self.start_time) * 1000)
            execution_timestamp = datetime.now()

            for batch_id, file_path in enumerate(self.data_files, start=1):

                estimated_batch_size = (
                    self.total_records // self.batch_count
                )

                estimated_malformed = (
                    self.malformed_records // self.batch_count
                )

                self.db_manager.insert_batch_metadata(
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    batch_id=batch_id,
                    batch_size=estimated_batch_size,
                    records_processed=estimated_batch_size,
                    malformed_records=estimated_malformed,
                    execution_timestamp=execution_timestamp,
                )

                self.db_manager.insert_malformed_summary(
                    run_id=self.run_id,
                    pipeline_name=self.pipeline_name,
                    batch_id=batch_id,
                    malformed_count=estimated_malformed,
                    execution_timestamp=execution_timestamp,
                )
           
          
            self.db_manager.insert_parent_result(
                run_id=self.run_id,
                pipeline_name=self.pipeline_name,
                query_name=self.query_name,
                execution_timestamp=execution_timestamp,
                extra_json={
                    "mode": "java_hive",
                    "batch_count": self.batch_count,
                    "batch_size": self.batch_size,
                    "total_records": self.total_records,
                    "malformed_records": self.malformed_records,
                    "elapsed_ms": elapsed_ms
                },
            )
            self.db_manager.insert_daily_traffic_results(
                q1_rows,
                self.run_id,
                self.pipeline_name
            )
            self.db_manager.insert_top_resources_results(
                q2_rows,
                self.run_id,
                self.pipeline_name
            )
            self.db_manager.insert_error_analysis_results(
                q3_rows,
                self.run_id,
                self.pipeline_name
            )
            self.end_time = time.time()
            self.batch_size = largest_batch_size
            self.avg_batch_size = avg_batch_size
            self.save_metadata()
            print(self.get_status_string())
            return True
        except Exception as exc:
            print(f"✗ Hive pipeline execution failed: {exc}")
            import traceback; traceback.print_exc()
            return False
        finally:
            self.disconnect_db()
    def _compile_java(self) -> bool:
        if not os.path.exists(self.java_source):
            print(f"✗ Java source not found: {self.java_source}")
            return False
        result = subprocess.run(
            ["javac", os.path.basename(self.java_source)],
            cwd=self.java_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("✗ Java compilation failed:")
            print(result.stderr.strip())
            return False
        print("✓ Compiled Java Hive analytics")
        return True
    def _run_java_pipeline(self) -> dict:
        java_args = ["java", "-cp", ".", self.java_class , self.query_name] + [
            os.path.abspath(path) for path in self.data_files
        ]
        result = subprocess.run(
            java_args,
            cwd=self.java_dir,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("✗ Java Hive analytics failed:")
            print(result.stderr.strip())
            return None
        try:
            return json.loads(result.stdout.strip())
        except json.JSONDecodeError as exc:
            print("✗ Failed to parse Java output as JSON:")
            print(result.stdout)
            print(exc)
            return None
