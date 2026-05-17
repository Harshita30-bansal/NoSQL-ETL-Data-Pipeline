"""
config.py – Central configuration for the ETL pipeline framework.
PostgreSQL only. Hadoop, Hive, Pig, MongoDB, MapReduce settings all here.
"""

import os

# ─── Data Files (each file = one batch) ──────────────────────────────────────
DATA_FILES = {
    "july":   "data/NASA_access_log_Jul95",
    "august": "data/NASA_access_log_Aug95",
}

# ─── Batch Sizes ─────────────────────────────────────────────────────────────
BATCH_SIZES = [1000, 5000, 10000, 50000]

# ─── Error Status Range ───────────────────────────────────────────────────────
ERROR_STATUS_MIN = 400
ERROR_STATUS_MAX = 599

# ─── PostgreSQL Config ────────────────────────────────────────────────────────
DB_CONFIG = {
    "postgresql": {
        "host":     os.getenv("PG_HOST",     "localhost"),
        "port":     int(os.getenv("PG_PORT", "5432")),
        "database": os.getenv("PG_DB",       "nasa_etl"),
        "user":     os.getenv("PG_USER",     "postgres"),
        "password": os.getenv("PG_PASSWORD", "postgres"),
    }
}

# ─── MongoDB Config ───────────────────────────────────────────────────────────
MONGO_CONFIG = {
    "host":     os.getenv("MONGO_HOST", "localhost"),
    "port":     int(os.getenv("MONGO_PORT", "27017")),
    "database": os.getenv("MONGO_DB",   "nasa_logs"),
    "collections": {
        "logs": "access_logs",
    },
}

# ─── Hadoop / HDFS Config ─────────────────────────────────────────────────────
HADOOP_HOME   = os.getenv("HADOOP_HOME",   "/usr/local/hadoop")
HADOOP_BIN    = os.path.join(HADOOP_HOME, "bin", "hadoop")
HDFS_BIN      = os.path.join(HADOOP_HOME, "bin", "hdfs")
MAPRED_BIN    = os.path.join(HADOOP_HOME, "bin", "mapred")

HDFS_BASE_DIR = "/user/nasa_etl"
HDFS_INPUT    = f"{HDFS_BASE_DIR}/input"
HDFS_OUTPUT   = f"{HDFS_BASE_DIR}/output"

# Hadoop Streaming JAR – adjust version if needed
STREAMING_JAR = os.getenv(
    "HADOOP_STREAMING_JAR",
    f"{HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
)

# ─── Pig Config ───────────────────────────────────────────────────────────────
PIG_HOME   = os.getenv("PIG_HOME",   "/usr/local/pig")
PIG_BIN    = os.path.join(PIG_HOME,  "bin", "pig")
PIG_SCRIPTS_DIR = "pig_scripts"     # local directory holding .pig files

# ─── Hive Config ─────────────────────────────────────────────────────────────
HIVE_HOME    = os.getenv("HIVE_HOME",    "/usr/local/hive")
BEELINE_BIN  = os.path.join(HIVE_HOME,  "bin", "beeline")
HIVE_JDBC    = os.getenv("HIVE_JDBC",   "jdbc:hive2://localhost:10000/default")
HIVE_USER    = os.getenv("HIVE_USER",   "hive")
HIVE_PASSWORD = os.getenv("HIVE_PASSWORD", "")
HIVE_SCRIPTS_DIR = "hive_scripts"   # local directory holding .hql files

# ─── MapReduce Hadoop Streaming scripts ──────────────────────────────────────
MR_SCRIPTS_DIR = "mr_scripts"       # local directory holding mapper/reducer .py

# ─── Pipelines registry (used by orchestrator display) ───────────────────────
PIPELINES = {
    "pig":       "Apache Pig",
    "mapreduce": "Hadoop MapReduce",
    "mongodb":   "MongoDB",
    "hive":      "Apache Hive",
}

# ─── Canned SQL Queries (for reporter) ───────────────────────────────────────
QUERIES = {
    "daily_traffic": """
        SELECT pipeline_name, log_date, status_code,
               SUM(request_count) AS request_count,
               SUM(total_bytes)   AS total_bytes
        FROM   daily_traffic_summary
        WHERE  run_id = %s
        GROUP BY pipeline_name, log_date, status_code
        ORDER BY log_date, status_code
    """,
    "top_resources": """
        SELECT pipeline_name, resource_path, request_count,
               total_bytes, distinct_host_count, rank
        FROM   top_resources
        WHERE  run_id = %s
        ORDER BY rank
    """,
    "error_analysis": """
        SELECT pipeline_name, log_date, log_hour,
               error_request_count, total_request_count,
               error_rate, distinct_error_hosts
        FROM   hourly_error_analysis
        WHERE  run_id = %s
        ORDER BY log_date, log_hour
    """,
}