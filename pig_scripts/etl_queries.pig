-- ============================================================
-- pig_scripts/etl_queries.pig
-- Apache Pig ETL script for NASA HTTP Log Analytics.
--
-- Parameters (passed via -param on the CLI):
--   INPUT_DIR  : HDFS directory containing both log files
--   OUTPUT_Q1  : HDFS output path for Query 1
--   OUTPUT_Q2  : HDFS output path for Query 2
--   OUTPUT_Q3  : HDFS output path for Query 3
--   ERROR_MIN  : lower error status bound  (400)
--   ERROR_MAX  : upper error status bound  (599)
--   TOP_N      : top-N resources for Q2    (20)
--
-- Parsing strategy
-- ─────────────────
-- Pig has no built-in Combined Log Format loader, so we use
-- a REGEX_EXTRACT approach that mirrors the Python parser.py
-- regex exactly.  Records that do not match are discarded and
-- counted as malformed (same rule as all other pipelines).
--
-- The regex groups are:
--   $0  host
--   $1  timestamp string  e.g. 01/Jul/1995:00:00:01 -0400
--   $2  raw request string e.g. GET /images/NASA-logosmall.gif HTTP/1.0
--   $3  status code (3-digit string or -)
--   $4  bytes (integer string or -)
-- ============================================================

-- ─── 1. Load raw lines ───────────────────────────────────────
raw_lines = LOAD '$INPUT_DIR' USING TextLoader() AS (line:chararray);

-- ─── 2. Parse with regex (same pattern as parser.py) ────────
--
-- Full CLF regex:
--   ^(\S+)\s+-\s+-\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d{3}|-)\s+(\S+)
--
-- REGEX_EXTRACT returns NULL for the whole tuple when the pattern
-- does not match, which lets us filter malformed records cleanly.

parsed_raw = FOREACH raw_lines GENERATE
    REGEX_EXTRACT(line,
        '^(\\S+)\\s+-\\s+-\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\d{3}|-)\\s+(\\S+)',
        1) AS host:chararray,
    REGEX_EXTRACT(line,
        '^(\\S+)\\s+-\\s+-\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\d{3}|-)\\s+(\\S+)',
        2) AS ts_raw:chararray,
    REGEX_EXTRACT(line,
        '^(\\S+)\\s+-\\s+-\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\d{3}|-)\\s+(\\S+)',
        3) AS request_str:chararray,
    REGEX_EXTRACT(line,
        '^(\\S+)\\s+-\\s+-\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\d{3}|-)\\s+(\\S+)',
        4) AS status_raw:chararray,
    REGEX_EXTRACT(line,
        '^(\\S+)\\s+-\\s+-\\s+\\[([^\\]]+)\\]\\s+"([^"]*)"\\s+(\\d{3}|-)\\s+(\\S+)',
        5) AS bytes_raw:chararray;

-- ─── 3. Discard malformed records ────────────────────────────
--
-- A record is malformed if:
--   • host is NULL (regex did not match)
--   • status_raw is '-' or NULL  (same rule as parser.py)

valid_parsed = FILTER parsed_raw BY
    host        IS NOT NULL
    AND status_raw IS NOT NULL
    AND status_raw != '-';

-- ─── 4. Project structured fields ────────────────────────────
--
-- log_date  : first 11 chars of ts_raw → "01/Jul/1995"
--             Pig's ToString(ToDate(...)) lets us reformat.
--             We use SUBSTRING + REPLACE to convert
--             "01/Jul/1995:00:00:01 -0400" → "1995-07-01"
--
-- log_hour  : chars 12-13 of ts_raw (0-indexed)
--
-- resource_path: first token after HTTP method in request_str
--   e.g. "GET /path HTTP/1.0" → "/path"
--   Handled with a second REGEX_EXTRACT.
--
-- bytes_transferred: cast bytes_raw; treat '-' or '' as 0
-- status_code: cast status_raw to int

structured = FOREACH valid_parsed {
    -- Extract date parts from timestamp "DD/Mon/YYYY:HH:MM:SS zone"
    day_str   = SUBSTRING(ts_raw, 0, 2);
    mon_str   = SUBSTRING(ts_raw, 3, 6);
    year_str  = SUBSTRING(ts_raw, 7, 11);
    hour_str  = SUBSTRING(ts_raw, 12, 14);

    -- Map month abbreviation to number via a nested CASE-like expression
    -- (Pig does not have CASE; we use a chain of nested REPLACE on a
    --  sentinel string — a standard Pig idiom for month mapping.)
    mon_num =
        (mon_str == 'Jan' ? '01' :
        (mon_str == 'Feb' ? '02' :
        (mon_str == 'Mar' ? '03' :
        (mon_str == 'Apr' ? '04' :
        (mon_str == 'May' ? '05' :
        (mon_str == 'Jun' ? '06' :
        (mon_str == 'Jul' ? '07' :
        (mon_str == 'Aug' ? '08' :
        (mon_str == 'Sep' ? '09' :
        (mon_str == 'Oct' ? '10' :
        (mon_str == 'Nov' ? '11' :
        (mon_str == 'Dec' ? '12' : '00'))))))))))));

    log_date_str = CONCAT(year_str, '-', mon_num, '-', day_str);

    -- resource_path from request string
    resource = REGEX_EXTRACT(request_str,
                   '^[A-Z]+\\s+(\\S+)',
                   1);

    -- bytes: treat '-' or empty as 0
    bytes_val = ((bytes_raw IS NULL OR bytes_raw == '-' OR bytes_raw == '')
                  ? 0L
                  : (long) bytes_raw);

    GENERATE
        host                    AS host:chararray,
        log_date_str            AS log_date:chararray,
        (int) hour_str          AS log_hour:int,
        resource                AS resource_path:chararray,
        (int) status_raw        AS status_code:int,
        bytes_val               AS bytes_transferred:long;
};

-- Discard any rows where the date string could not be built
-- (e.g. unrecognised month abbreviation → mon_num = '00')
clean = FILTER structured BY
    log_date IS NOT NULL
    AND log_hour IS NOT NULL
    AND status_code IS NOT NULL;

-- ─── 5. Cache for reuse across all three queries ──────────────
clean_cached = FOREACH clean GENERATE *;


-- ═══════════════════════════════════════════════════════════════
-- QUERY 1 – Daily Traffic Summary
-- For each (log_date, status_code):
--   total requests, total bytes
-- ═══════════════════════════════════════════════════════════════

q1_grouped = GROUP clean_cached BY (log_date, status_code);

q1_results = FOREACH q1_grouped GENERATE
    FLATTEN(group)          AS (log_date:chararray, status_code:int),
    COUNT(clean_cached)     AS request_count:long,
    SUM(clean_cached.bytes_transferred) AS total_bytes:long;

q1_sorted = ORDER q1_results BY log_date ASC, status_code ASC;

STORE q1_sorted INTO '$OUTPUT_Q1' USING PigStorage('\t');


-- ═══════════════════════════════════════════════════════════════
-- QUERY 2 – Top Requested Resources
-- Top $TOP_N resource paths by request count.
-- For each: total requests, total bytes, distinct host count.
-- ═══════════════════════════════════════════════════════════════

-- Exclude records with NULL resource_path (same as MongoDB $match)
q2_input = FILTER clean_cached BY resource_path IS NOT NULL;

q2_grouped = GROUP q2_input BY resource_path;

q2_agg = FOREACH q2_grouped {
    unique_hosts = DISTINCT q2_input.host;
    GENERATE
        group                           AS resource_path:chararray,
        COUNT(q2_input)                 AS request_count:long,
        SUM(q2_input.bytes_transferred) AS total_bytes:long,
        COUNT(unique_hosts)             AS distinct_host_count:long;
};

q2_sorted = ORDER q2_agg BY request_count DESC;

q2_top = LIMIT q2_sorted $TOP_N;

STORE q2_top INTO '$OUTPUT_Q2' USING PigStorage('\t');


-- ═══════════════════════════════════════════════════════════════
-- QUERY 3 – Hourly Error Analysis
-- For each (log_date, log_hour):
--   error requests (status 400–599), total requests,
--   error rate, distinct hosts generating errors.
-- ═══════════════════════════════════════════════════════════════

-- Tag each record: is it an error?
q3_tagged = FOREACH clean_cached GENERATE
    log_date,
    log_hour,
    host,
    (status_code >= $ERROR_MIN AND status_code <= $ERROR_MAX ? 1 : 0)
        AS is_error:int;

q3_grouped = GROUP q3_tagged BY (log_date, log_hour);

q3_results = FOREACH q3_grouped {
    -- All requests in this (date, hour) bucket
    total_reqs  = q3_tagged;

    -- Only the error records
    error_recs  = FILTER q3_tagged BY is_error == 1;

    -- Distinct hosts that generated at least one error
    error_hosts = DISTINCT error_recs.host;

    total_count = (double) COUNT(total_reqs);
    error_count = (double) COUNT(error_recs);

    GENERATE
        FLATTEN(group)              AS (log_date:chararray, log_hour:int),
        (long)  error_count         AS error_request_count:long,
        (long)  total_count         AS total_request_count:long,
        (total_count > 0 ? error_count / total_count : 0.0)
                                    AS error_rate:double,
        COUNT(error_hosts)          AS distinct_error_hosts:long;
};

q3_sorted = ORDER q3_results BY log_date ASC, log_hour ASC;

STORE q3_sorted INTO '$OUTPUT_Q3' USING PigStorage('\t');
