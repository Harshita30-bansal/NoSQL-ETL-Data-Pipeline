"""
Configuration file for all pipeline and database settings
"""

# Database Configuration
DB_CONFIG = {
    'mysql': {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'root',
        'database': 'nosql_etl_project'
    },
    'postgresql': {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres',
        'database': 'nosql_etl_project'
    }
}

# Default database choice
DEFAULT_DB = 'postgresql'

# Data file paths
DATA_FILES = {
    'july': 'data/NASA_access_log_Jul95',
    'august': 'data/NASA_access_log_Aug95'
}

# Batch Configuration
BATCH_SIZES = [1000, 5000, 10000]
DEFAULT_BATCH_SIZE = 5000

# Pipeline choices
PIPELINES = {
    'pig': 'Apache Pig',
    'mapreduce': 'MapReduce',
    'mongodb': 'MongoDB',
    'hive': 'Apache Hive'
}

# Query definitions
QUERIES = {
    1: {
        'name': 'Daily Traffic Summary',
        'description': 'For each log date and status code, compute total requests and bytes',
        'output_fields': ['log_date', 'status_code', 'request_count', 'total_bytes']
    },
    2: {
        'name': 'Top Requested Resources',
        'description': 'Top 20 requested resource paths by request count',
        'output_fields': ['resource_path', 'request_count', 'total_bytes', 'distinct_host_count'],
        'limit': 20
    },
    3: {
        'name': 'Hourly Error Analysis',
        'description': 'For each date/hour, compute error stats (400-599 status codes)',
        'output_fields': ['log_date', 'log_hour', 'error_request_count', 'total_request_count', 'error_rate', 'distinct_error_hosts']
    }
}

# Pig execution settings
PIG_PATH = '/usr/local/pig/bin/pig'
PIG_SCRIPTS_PATH = 'pig_scripts'

# MapReduce settings
HADOOP_PATH = '/usr/local/hadoop'
MAPREDUCE_JAR_PATH = 'mapreduce'
HADOOP_HOME = '/usr/local/hadoop'

# Hive settings
HIVE_PATH = '/usr/local/hive/bin/hive'
HIVE_SCRIPTS_PATH = 'hive_scripts'

# MongoDB settings
MONGO_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'nasa_logs',
    'collections': {
        'logs': 'http_logs',
        'daily_traffic': 'daily_traffic_summary',
        'top_resources': 'top_resources',
        'error_analysis': 'hourly_error_analysis'
    }
}

# Results and output paths
RESULTS_PATH = 'results'
REPORTS_PATH = 'reports'
LOGS_PATH = 'logs'

# Error status codes range (for Query 3)
ERROR_STATUS_MIN = 400
ERROR_STATUS_MAX = 599
