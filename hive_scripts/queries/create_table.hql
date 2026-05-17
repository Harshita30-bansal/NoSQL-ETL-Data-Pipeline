CREATE DATABASE IF NOT EXISTS nasa_etl;

USE nasa_etl;

DROP TABLE IF EXISTS nasa_logs;

CREATE EXTERNAL TABLE nasa_logs (
    host STRING,
    log_timestamp STRING,
    log_date STRING,
    log_hour INT,
    http_method STRING,
    resource_path STRING,
    protocol_version STRING,
    status_code INT,
    bytes_transferred BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/nasa_etl/logs';