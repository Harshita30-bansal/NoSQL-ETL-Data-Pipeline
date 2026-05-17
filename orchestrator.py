"""
orchestrator.py – Central CLI controller for the ETL pipeline framework.
Supports: Pig, MapReduce (Hadoop), MongoDB, Hive
Database: PostgreSQL only
Batching: July file = Batch 1, August file = Batch 2 (loaded in parallel)
"""

import os
import sys
from config import PIPELINES, DATA_FILES, BATCH_SIZES
from pipelines.pig_pipeline       import PigPipeline
from pipelines.mapreduce_pipeline import MapReducePipeline
from pipelines.mongodb_pipeline   import MongoDBPipeline
from pipelines.hive_pipeline      import HivePipeline
from reports.reporter             import Reporter


class Orchestrator:
    """Main orchestrator for ETL pipeline execution."""

    def __init__(self):
        self.current_data_files = list(DATA_FILES.values())  # both files by default
        self.current_batch_size = BATCH_SIZES[1]             # 5000
        self.db_type            = "postgresql"
        self.last_run_id        = None

    # ── menus ─────────────────────────────────────────────────────────────────

    def display_menu(self):
        print("\n" + "="*55)
        print("  NASA Log ETL Framework  |  DB: PostgreSQL")
        print("="*55)
        print("  Batching : July  → Batch 1")
        print("             August → Batch 2  (loaded in parallel)")
        print(f"  Batch size : {self.current_batch_size:,} records per chunk")
        print("="*55)
        print("  1. Exit")
        print("  2. Execute Apache Pig Pipeline")
        print("  3. Execute MapReduce (Hadoop Streaming) Pipeline")
        print("  4. Execute MongoDB Pipeline")
        print("  5. Execute Apache Hive Pipeline")
        print("  6. View Execution History")
        print("  7. Generate Report")
        print("  8. Compare Pipelines")
        print("  9. Change Batch Chunk Size")
        print("="*55)

    def change_batch_size(self):
        print("\nSelect batch chunk size (records per parse chunk):")
        for i, size in enumerate(BATCH_SIZES, 1):
            print(f"  {i}. {size:,}")
        choice = input(f"Enter choice (1-{len(BATCH_SIZES)}): ").strip()
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(BATCH_SIZES):
                self.current_batch_size = BATCH_SIZES[idx]
                print(f"  ✓ Batch chunk size set to {self.current_batch_size:,}")
                return
        except ValueError:
            pass
        print("  Invalid choice – keeping previous setting.")

    # ── pipeline execution ────────────────────────────────────────────────────

    def execute_pipeline(self, pipeline_class):
        """Instantiate and run a pipeline with current settings."""
        print(f"\n  Starting: {pipeline_class.__name__}")
        print(f"  Files   : {self.current_data_files}")
        print(f"  Batch chunk size: {self.current_batch_size:,}")
        print(f"  Database: {self.db_type}\n")

        # Validate files exist
        for f in self.current_data_files:
            if not os.path.exists(f):
                print(f"  ✗ Data file not found: {f}")
                return

        try:
            pipeline = pipeline_class(
                data_file  = self.current_data_files,
                batch_size = self.current_batch_size,
                db_type    = self.db_type,
            )
            self.last_run_id = pipeline.run_id
            success = pipeline.execute()

            if success:
                print(f"\n  ✓ Pipeline completed. Run ID: {self.last_run_id}")
                choice = input("\n  Generate report now? (y/n): ").strip().lower()
                if choice == "y":
                    self.generate_report(self.last_run_id)
            else:
                print("\n  ✗ Pipeline failed.")

        except Exception as exc:
            print(f"  ✗ Unexpected error: {exc}")
            import traceback; traceback.print_exc()

    # ── reporting ─────────────────────────────────────────────────────────────

    def generate_report(self, run_id: str = None):
        if not run_id:
            run_id = input("  Enter Run ID (blank = latest): ").strip()
        try:
            reporter = Reporter(self.db_type)
            if run_id:
                reporter.generate_full_report(run_id)
            else:
                results = reporter.db_manager.execute_query(
                    "SELECT run_id FROM execution_metadata "
                    "ORDER BY created_at DESC LIMIT 1"
                )
                if results:
                    reporter.generate_full_report(results[0]["run_id"])
                else:
                    print("  No executions found.")
        except Exception as exc:
            print(f"  ✗ Report error: {exc}")

    def view_execution_history(self):
        try:
            reporter = Reporter(self.db_type)
            reporter.list_all_executions()
        except Exception as exc:
            print(f"  ✗ History error: {exc}")

    def compare_pipelines(self):
        try:
            reporter = Reporter(self.db_type)
            reporter.compare_pipelines()
        except Exception as exc:
            print(f"  ✗ Comparison error: {exc}")

    # ── main loop ─────────────────────────────────────────────────────────────

    def run(self):
        while True:
            self.display_menu()
            choice = input("  Enter choice (1-9): ").strip()

            if choice == "1":
                print("\n  Goodbye!\n")
                break
            elif choice == "2":
                self.execute_pipeline(PigPipeline)
            elif choice == "3":
                self.execute_pipeline(MapReducePipeline)
            elif choice == "4":
                self.execute_pipeline(MongoDBPipeline)
            elif choice == "5":
                self.execute_pipeline(HivePipeline)
            elif choice == "6":
                self.view_execution_history()
            elif choice == "7":
                self.generate_report()
            elif choice == "8":
                self.compare_pipelines()
            elif choice == "9":
                self.change_batch_size()
            else:
                print("  Invalid choice. Please enter 1-9.")

            input("\n  Press Enter to continue…")


def main():
    orchestrator = Orchestrator()
    orchestrator.run()


if __name__ == "__main__":
    main()