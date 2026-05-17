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
    def __init__(self, data_file, batch_size=5000, db_type='postgresql'):
        super().__init__('Apache Hive', data_file, batch_size, db_type)
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
            print(
                f"  ✓ Java analytics returned {self.total_records:,} records "
                f"({self.malformed_records:,} malformed)"
            )
            elapsed_ms = int((time.time() - self.start_time) * 1000)
            execution_timestamp = datetime.now()
            self.db_manager.insert_parent_result(
                self.run_id, self.pipeline_name,
                self.batch_count, self.batch_size,
                self.total_records, self.malformed_records,
                elapsed_ms, execution_timestamp,
                extra_json={"mode": "java_hive"},
            )
            self.db_manager.insert_daily_traffic_results(
                q1_rows, self.run_id, self.pipeline_name, 1, execution_timestamp)
            self.db_manager.insert_top_resources_results(
                q2_rows, self.run_id, self.pipeline_name, 1, execution_timestamp)
            self.db_manager.insert_error_analysis_results(
                q3_rows, self.run_id, self.pipeline_name, 1, execution_timestamp)
            self.end_time = time.time()
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
        java_args = ["java", "-cp", ".", self.java_class] + [
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
