"""
QUICKSTART - Get up and running in 5 minutes
"""

# ==============================================================================
# STEP 1: Install Dependencies
# ==============================================================================

# Option A: Using PowerShell (Windows)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Option B: Using bash (Linux/macOS)
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


# ==============================================================================
# STEP 2: Initialize Database (One-time)
# ==============================================================================

python setup.py

# When prompted, select:
# - Option 1 for MySQL (or 2 for PostgreSQL)
# - Creates 'nosql_etl_project' database
# - Creates all required tables


# ==============================================================================
# STEP 3: Verify Data Files
# ==============================================================================

# Should have these files:
# - data/NASA_access_log_Jul95   (1.89M records)
# - data/NASA_access_log_Aug95   (1.57M records)

# If missing, download from:
# https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
# https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz


# ==============================================================================
# STEP 4: Run the Framework
# ==============================================================================

python orchestrator.py

# This opens an interactive menu where you can:
# 1. Run any of the 4 pipelines
# 2. View execution history
# 3. Generate reports
# 4. Compare pipeline performance


# ==============================================================================
# STEP 5: Run Tests (Optional)
# ==============================================================================

python test_parser.py          # Parser unit tests
python tests/parser_smoke_test.py  # Smoke test on actual data


# ==============================================================================
# EXAMPLE WORKFLOW
# ==============================================================================

# 1. python setup.py
#    ↓ Initialize database

# 2. python orchestrator.py
#    ↓ Open CLI menu

# 3. Press 9 (Settings)
#    ↓ Select July data, batch size 5000, MySQL

# 4. Press 1 (Python Native Pipeline)
#    ↓ Processes all batches, stores results

# 5. Press 7 (Generate Report)
#    ↓ Shows query results + execution stats

# 6. Press 2 (Pig Pipeline)
#    ↓ Generates .pig scripts, runs pipeline

# 7. Press 8 (Compare Pipelines)
#    ↓ Shows performance metrics side-by-side

# 8. Press 0 (Exit)


# ==============================================================================
# MENU REFERENCE
# ==============================================================================

1 = Python Native Pipeline (baseline, fastest)
2 = Apache Pig Pipeline (high-level data flow)
3 = MapReduce Pipeline (distributed computing)
4 = MongoDB Pipeline (NoSQL document store)
5 = Apache Hive Pipeline (SQL on Hadoop)
6 = View Execution History (all past runs)
7 = Generate Report (display query results)
8 = Compare Pipelines (performance stats)
9 = Settings (database, batch size, data file)
0 = Exit


# ==============================================================================
# CONFIGURATION (if needed)
# ==============================================================================

# Edit config.py to change:
# - Database credentials
# - Batch sizes to test
# - Data file locations
# - Error status code range


# ==============================================================================
# TROUBLESHOOTING
# ==============================================================================

# Connection Error → Check DB credentials in config.py
# Data file missing → Download from ita.ee.lbl.gov
# Import error → Verify .venv activated
# MongoDB error → Start MongoDB: mongod
# Permission error → Run with admin/sudo


# ==============================================================================
# KEY FILES
# ==============================================================================

parser.py              = Log parser (core logic)
config.py              = Configuration
orchestrator.py        = Main CLI interface
setup.py               = Database initialization

src/base_pipeline.py   = Base pipeline class
src/db_manager.py      = Database operations
src/query_processor.py = Query logic (3 queries)

pipelines/*_pipeline.py = 4 pipeline implementations
reports/reporter.py     = Report generation

db/schema.sql          = Database schema


# ==============================================================================
# EXPECTED OUTPUT
# ==============================================================================

When you run a pipeline, you should see:

  ============================================================
  Executing Apache Pig Pipeline
  ============================================================

  Preparing Pig scripts...
  ✓ Generated pig_scripts/query_1.pig
  ✓ Generated pig_scripts/query_2.pig
  ✓ Generated pig_scripts/query_3.pig
  Processing batches in Python (Pig simulation)...
  Batch 1: 5000 records, 12 malformed
  Batch 2: 5000 records, 8 malformed
  ...

  ┌─ Pipeline Execution Complete ─────────────────────┐
  │ Pipeline: Apache Pig                              │
  │ Run ID: a1b2c3d4                                  │
  │ Total Records: 1,891,715                          │
  │ Malformed Records: 523                            │
  │ Batches Processed: 379                            │
  │ Avg Batch Size: 4,991.81                          │
  │ Execution Time: 23,456 ms                         │
  └───────────────────────────────────────────────────┘

  Would you like to generate a report? (y/n): y


# ==============================================================================
# QUICK REFERENCE - Database Queries
# ==============================================================================

# View all executions:
SELECT run_id, pipeline_name, execution_time_ms 
FROM execution_metadata 
ORDER BY created_at DESC;

# View Query 1 results:
SELECT log_date, status_code, request_count, total_bytes
FROM daily_traffic_summary
WHERE run_id = 'YOUR_RUN_ID'
ORDER BY log_date;

# Compare pipeline speeds:
SELECT pipeline_name, AVG(execution_time_ms) as avg_time
FROM execution_metadata
GROUP BY pipeline_name
ORDER BY avg_time;


# ==============================================================================
# THAT'S IT!
# ==============================================================================

# You now have a complete, working multi-pipeline ETL framework!
# Enjoy exploring different data processing paradigms!
