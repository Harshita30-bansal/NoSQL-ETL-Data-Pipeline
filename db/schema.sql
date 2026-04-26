-- PostgreSQL Schema for ETL Results Reporting

-- Results table storing aggregated query outputs
CREATE TABLE IF NOT EXISTS query_results (
    result_id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) UNIQUE NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    query_id INT NOT NULL,
    query_name VARCHAR(100) NOT NULL,
    batch_id INT NOT NULL,
    batch_size INT NOT NULL,
    avg_batch_size FLOAT NOT NULL,
    execution_time_ms INT NOT NULL,
    execution_timestamp TIMESTAMP NOT NULL,
    malformed_records INT DEFAULT 0,
    total_records_processed INT NOT NULL,
    result_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
);

-- Query 1 Results: Daily Traffic Summary
CREATE TABLE IF NOT EXISTS daily_traffic_summary (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    log_date DATE NOT NULL,
    status_code INT NOT NULL,
    request_count INT NOT NULL,
    total_bytes BIGINT NOT NULL,
    batch_id INT NOT NULL,
    execution_timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (run_id) REFERENCES query_results(run_id)
);

CREATE INDEX idx_dts_run_pipeline ON daily_traffic_summary(run_id, pipeline_name);
CREATE INDEX idx_dts_date ON daily_traffic_summary(log_date);

-- Query 2 Results: Top Requested Resources
CREATE TABLE IF NOT EXISTS top_resources (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    resource_path VARCHAR(500) NOT NULL,
    request_count INT NOT NULL,
    total_bytes BIGINT NOT NULL,
    distinct_host_count INT NOT NULL,
    rank INT NOT NULL,
    batch_id INT NOT NULL,
    execution_timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (run_id) REFERENCES query_results(run_id)
);

CREATE INDEX idx_tr_run_pipeline ON top_resources(run_id, pipeline_name);
CREATE INDEX idx_tr_rank ON top_resources(rank);

-- Query 3 Results: Hourly Error Analysis
CREATE TABLE IF NOT EXISTS hourly_error_analysis (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    log_date DATE NOT NULL,
    log_hour INT NOT NULL,
    error_request_count INT NOT NULL,
    total_request_count INT NOT NULL,
    error_rate FLOAT NOT NULL,
    distinct_error_hosts INT NOT NULL,
    batch_id INT NOT NULL,
    execution_timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (run_id) REFERENCES query_results(run_id)
);

CREATE INDEX idx_hea_run_pipeline ON hourly_error_analysis(run_id, pipeline_name);
CREATE INDEX idx_hea_date_hour ON hourly_error_analysis(log_date, log_hour);

-- Execution metadata table
CREATE TABLE IF NOT EXISTS execution_metadata (
    execution_id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) UNIQUE NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    batch_size INT NOT NULL,
    total_batches INT NOT NULL,
    total_records INT NOT NULL,
    malformed_records INT NOT NULL,
    avg_batch_size FLOAT NOT NULL,
    execution_start TIMESTAMP NOT NULL,
    execution_end TIMESTAMP NOT NULL,
    execution_time_ms INT NOT NULL,
    data_file VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_exec_pipeline ON execution_metadata(pipeline_name);
CREATE INDEX idx_exec_run_id ON execution_metadata(run_id);