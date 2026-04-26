"""
Main Orchestrator - central controller for pipeline execution
Provides CLI interface for selecting pipelines and running ETL jobs
"""

import os
import sys
import time
from datetime import datetime
from config import PIPELINES, DATA_FILES, BATCH_SIZES, QUERIES
from src.base_pipeline import PythonNativePipeline
from pipelines.pig_pipeline import PigPipeline
from pipelines.mapreduce_pipeline import MapReducePipeline
from pipelines.mongodb_pipeline import MongoDBPipeline
from pipelines.hive_pipeline import HivePipeline
from reports.reporter import Reporter


class Orchestrator:
    """Main orchestrator for ETL pipeline execution"""

    def __init__(self):
        self.current_pipeline = None
        self.current_data_file = None
        self.current_batch_size = None
        self.current_db = 'mysql'
        self.run_id = None

    def display_menu(self):
        """Display main menu"""
        print("\n" + "="*70)
        print("MULTI-PIPELINE ETL AND REPORTING FRAMEWORK")
        print("NASA HTTP Web Server Log Analytics")
        print("="*70 + "\n")

        print("1. Execute Python Native Pipeline (Baseline)")
        print("2. Execute Apache Pig Pipeline")
        print("3. Execute MapReduce Pipeline")
        print("4. Execute MongoDB Pipeline")
        print("5. Execute Apache Hive Pipeline")
        print("6. View Execution History")
        print("7. Generate Report")
        print("8. Compare Pipelines")
        print("9. Settings")
        print("0. Exit")
        print()

    def display_settings(self):
        """Display current settings"""
        print("\n" + "-"*70)
        print("CURRENT SETTINGS:")
        print("-"*70)
        print(f"Database Type: {self.current_db}")
        print(f"Default Data File: {self.current_data_file or 'Not selected'}")
        print(f"Default Batch Size: {self.current_batch_size or 'Not selected'}")
        print()

    def select_data_file(self):
        """Select input data file"""
        print("\nSelect data file:")
        for i, (key, path) in enumerate(DATA_FILES.items(), 1):
            print(f"  {i}. {key.upper()}: {path}")
        
        choice = input("Enter choice (1-{}): ".format(len(DATA_FILES)))
        
        files = list(DATA_FILES.items())
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(files):
                self.current_data_file = list(DATA_FILES.values())
                print("✓ Selected: ALL FILES (July + August)")
                return True
        except ValueError:
            pass
        
        print("✗ Invalid choice")
        return False

    def select_batch_size(self):
        """Select batch size"""
        print("\nSelect batch size:")
        for i, size in enumerate(BATCH_SIZES, 1):
            print(f"  {i}. {size:,} records")
        
        choice = input("Enter choice (1-{}): ".format(len(BATCH_SIZES)))
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(BATCH_SIZES):
                self.current_batch_size = BATCH_SIZES[idx]
                print(f"✓ Selected: {self.current_batch_size:,} records per batch")
                return True
        except ValueError:
            pass
        
        print("✗ Invalid choice")
        return False

    def select_database(self):
        """Select database type"""
        print("\nSelect database:")
        print("  1. MySQL")
        print("  2. PostgreSQL")
        
        choice = input("Enter choice (1-2): ")
        
        if choice == '1':
            self.current_db = 'mysql'
            print("✓ Selected: MySQL")
            return True
        elif choice == '2':
            self.current_db = 'postgresql'
            print("✓ Selected: PostgreSQL")
            return True
        
        print("✗ Invalid choice")
        return False

    def settings_menu(self):
        """Settings menu"""
        while True:
            self.display_settings()
            print("1. Change Database Type")
            print("2. Change Data File")
            print("3. Change Batch Size")
            print("4. Back to Main Menu")
            
            choice = input("Enter choice (1-4): ")
            
            if choice == '1':
                self.select_database()
            elif choice == '2':
                self.select_data_file()
            elif choice == '3':
                self.select_batch_size()
            elif choice == '4':
                break
            else:
                print("✗ Invalid choice")

    def execute_pipeline(self, pipeline_class):
        """Execute a pipeline"""
        if not self.current_data_file:
            print("✗ Data file not selected. Going to settings...")
            self.select_data_file()
        
        if not self.current_batch_size:
            print("✗ Batch size not selected. Going to settings...")
            self.select_batch_size()

        if isinstance(self.current_data_file, list):
            for f in self.current_data_file:
                if not os.path.exists(f):
                    print(f"✗ Data file not found: {f}")
                    return
        else:
            if not os.path.exists(self.current_data_file):
                print(f"✗ Data file not found: {self.current_data_file}")
                return

        print("\n⏳ Starting pipeline execution...")
        print(f"   Pipeline: {pipeline_class.__name__}")
        print(f"   Data File: {self.current_data_file}")
        print(f"   Batch Size: {self.current_batch_size:,}")
        print(f"   Database: {self.current_db}")

        try:
            pipeline = pipeline_class(
                data_file=self.current_data_file,
                batch_size=self.current_batch_size,
                db_type=self.current_db
            )
            
            self.run_id = pipeline.run_id
            
            success = pipeline.execute()
            
            if success:
                print(f"\n✓ Pipeline execution completed successfully!")
                print(f"  Run ID: {self.run_id}")
                
                # Offer to generate report
                choice = input("\nWould you like to generate a report? (y/n): ")
                if choice.lower() == 'y':
                    self.generate_report(self.run_id)
            else:
                print(f"\n✗ Pipeline execution failed!")
        
        except Exception as e:
            print(f"✗ Error during pipeline execution: {e}")

    def generate_report(self, run_id=None):
        """Generate execution report"""
        if not run_id:
            run_id = input("Enter Run ID (or press Enter for latest): ").strip()

        try:
            reporter = Reporter(self.current_db)
            
            if run_id:
                reporter.generate_full_report(run_id)
            else:
                # Get latest run
                query = "SELECT run_id FROM execution_metadata ORDER BY created_at DESC LIMIT 1"
                results = reporter.db_manager.execute_query(query)
                if results:
                    reporter.generate_full_report(results[0]['run_id'])
                else:
                    print("No executions found")
        
        except Exception as e:
            print(f"✗ Error generating report: {e}")

    def view_execution_history(self):
        """View execution history"""
        try:
            reporter = Reporter(self.current_db)
            reporter.list_all_executions()
        except Exception as e:
            print(f"✗ Error retrieving history: {e}")

    def compare_pipelines(self):
        """Compare pipeline performance"""
        try:
            reporter = Reporter(self.current_db)
            reporter.compare_pipelines()
        except Exception as e:
            print(f"✗ Error comparing pipelines: {e}")

    def run(self):
        """Main loop"""
        # Set defaults
        self.current_data_file = list(DATA_FILES.values())
        self.current_batch_size = BATCH_SIZES[1]  # Default to 5000

        while True:
            self.display_menu()
            choice = input("Enter choice (0-9): ").strip()

            if choice == '1':
                self.execute_pipeline(PythonNativePipeline)
            elif choice == '2':
                self.execute_pipeline(PigPipeline)
            elif choice == '3':
                self.execute_pipeline(MapReducePipeline)
            elif choice == '4':
                self.execute_pipeline(MongoDBPipeline)
            elif choice == '5':
                self.execute_pipeline(HivePipeline)
            elif choice == '6':
                self.view_execution_history()
            elif choice == '7':
                self.generate_report()
            elif choice == '8':
                self.compare_pipelines()
            elif choice == '9':
                self.settings_menu()
            elif choice == '0':
                print("\n✓ Thank you for using the ETL Framework. Goodbye!")
                break
            else:
                print("✗ Invalid choice. Please try again.")

            input("\nPress Enter to continue...")


def main():
    """Entry point"""
    orchestrator = Orchestrator()
    orchestrator.run()


if __name__ == "__main__":
    main()
