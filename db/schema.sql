-- PostgreSQL Schema for ETL Results Reporting
-- PostgreSQL ONLY - no MySQL

-- Execution metadata table (create FIRST - others FK to it)
CREATE TABLE IF NOT EXISTS execution_metadata (
    execution_id  SERIAL PRIMARY KEY,
    run_id        VARCHAR(100) UNIQUE NOT NULL,
    pipeline_name VARCHAR(50)  NOT NULL,
    batch_size    INT          NOT NULL,
    total_batches INT          NOT NULL DEFAULT 0,
    total_records INT          NOT NULL DEFAULT 0,
    malformed_records INT      NOT NULL DEFAULT 0,
    avg_batch_size FLOAT       NOT NULL DEFAULT 0,
    execution_start TIMESTAMP  NOT NULL,
    execution_end   TIMESTAMP  NOT NULL,
    execution_time_ms INT      NOT NULL DEFAULT 0,
    data_file     VARCHAR(255) NOT NULL,
    created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_exec_pipeline ON execution_metadata(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_exec_run_id   ON execution_metadata(run_id);

-- Parent query_results table (one row per pipeline run)
-- run_id is NOT UNIQUE here so multiple pipelines can each have a row
CREATE TABLE IF NOT EXISTS query_results (
    result_id     SERIAL PRIMARY KEY,
    run_id        VARCHAR(100) NOT NULL,
    pipeline_name VARCHAR(50)  NOT NULL,
    query_id      INT          NOT NULL,
    query_name    VARCHAR(100) NOT NULL,
    execution_timestamp TIMESTAMP NOT NULL,
    result_json   JSONB,
    created_at    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES execution_metadata(run_id)
);

CREATE INDEX IF NOT EXISTS idx_qr_run_pipeline ON query_results(run_id, pipeline_name);

-- Query 1 Results: Daily Traffic Summary
CREATE TABLE IF NOT EXISTS daily_traffic_summary (
    id          SERIAL PRIMARY KEY,
    run_id      VARCHAR(100) NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    log_date    DATE         NOT NULL,
    status_code INT          NOT NULL,
    request_count INT        NOT NULL,
    total_bytes BIGINT       NOT NULL,
    FOREIGN KEY (run_id) REFERENCES execution_metadata(run_id)
);

CREATE INDEX IF NOT EXISTS idx_dts_run_pipeline ON daily_traffic_summary(run_id, pipeline_name);
CREATE INDEX IF NOT EXISTS idx_dts_date         ON daily_traffic_summary(log_date);

-- Query 2 Results: Top Requested Resources
CREATE TABLE IF NOT EXISTS top_resources (
    id          SERIAL PRIMARY KEY,
    run_id      VARCHAR(100) NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    resource_path VARCHAR(500) NOT NULL,
    request_count INT         NOT NULL,
    total_bytes BIGINT        NOT NULL,
    distinct_host_count INT   NOT NULL,
    rank        INT           NOT NULL,
    FOREIGN KEY (run_id) REFERENCES execution_metadata(run_id)
);

CREATE INDEX IF NOT EXISTS idx_tr_run_pipeline ON top_resources(run_id, pipeline_name);
CREATE INDEX IF NOT EXISTS idx_tr_rank         ON top_resources(rank);

-- Query 3 Results: Hourly Error Analysis
CREATE TABLE IF NOT EXISTS hourly_error_analysis (
    id          SERIAL PRIMARY KEY,
    run_id      VARCHAR(100) NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    log_date    DATE         NOT NULL,
    log_hour    INT          NOT NULL,
    error_request_count INT  NOT NULL,
    total_request_count INT  NOT NULL,
    error_rate  FLOAT        NOT NULL,
    distinct_error_hosts INT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES execution_metadata(run_id)
);

CREATE INDEX IF NOT EXISTS idx_hea_run_pipeline ON hourly_error_analysis(run_id, pipeline_name);
CREATE INDEX IF NOT EXISTS idx_hea_date_hour    ON hourly_error_analysis(log_date, log_hour);

CREATE TABLE IF NOT EXISTS batch_metadata (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(32),
    pipeline_name VARCHAR(50),
    batch_id INT,
    batch_size INT,
    records_processed INT,
    malformed_records INT,
    execution_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS malformed_record_summary (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(32),
    pipeline_name VARCHAR(50),
    batch_id INT,
    malformed_record_count INT,
    execution_timestamp TIMESTAMP
);
