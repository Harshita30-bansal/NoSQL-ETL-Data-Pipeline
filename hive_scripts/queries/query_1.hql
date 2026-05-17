USE nasa_etl;

SELECT
    log_date,
    status_code,
    COUNT(*) AS request_count,
    SUM(bytes_transferred) AS total_bytes
FROM nasa_logs
GROUP BY log_date, status_code
ORDER BY log_date, status_code;