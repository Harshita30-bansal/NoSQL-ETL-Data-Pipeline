USE nasa_etl;
SELECT
    log_date,
    log_hour,
    SUM(CASE WHEN status_code >= 400 AND status_code < 600 THEN 1 ELSE 0 END) AS error_request_count,
    COUNT(*) AS total_request_count,
    CAST(SUM(CASE WHEN status_code >= 400 AND status_code < 600 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code >= 400 AND status_code < 600 THEN host ELSE NULL END) AS distinct_error_hosts
FROM nasa_logs
WHERE log_date IS NOT NULL AND log_hour IS NOT NULL
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour;
