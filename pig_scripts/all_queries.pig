-- ============================================================
-- pig_scripts/etl_queries.pig
-- Apache Pig ETL script for NASA HTTP Log Analytics.
-- Runs in -x local mode (no Hadoop/HDFS required).
--
-- Parameters (passed via -param on the CLI):
--   INPUT_JULY   : local path to July log file  (Batch 1)
--   INPUT_AUGUST : local path to August log file (Batch 2)
--   OUTPUT_Q1    : local output dir for Query 1
--   OUTPUT_Q2    : local output dir for Query 2
--   OUTPUT_Q3    : local output dir for Query 3
--   OUTPUT_META  : local output dir for record counts metadata
--   ERROR_MIN    : lower error status bound  (400)
--   ERROR_MAX    : upper error status bound  (599)
--   TOP_N        : top-N resources for Q2    (20)
--
-- Parsing strategy
-- ─────────────────
-- We use REGEX_EXTRACT to parse the Combined Log Format, mirroring
-- the same rules as parser.py:
--   • Records that do not match the CLF regex are malformed
--   • Records with status '-' or NULL are malformed
--   • bytes '-' or missing is treated as 0
--   • Timestamp parsed to extract log_date (YYYY-MM-DD) and log_hour
--
-- Outputs
-- ───────
-- metadata/    : one row → total_valid_records \t malformed_records
-- q1/          : log_date \t status_code \t request_count \t total_bytes
-- q2/          : resource_path \t request_count \t total_bytes \t distinct_host_count
-- q3/          : log_date \t log_hour \t error_req_count \t total_req_count \t error_rate \t distinct_error_hosts
-- ============================================================


-- ─── 1. Load both files and union them ──────────────────────
-- Each file is one logical batch. We load both and UNION so all
-- three queries run over the combined July + August dataset,
-- matching the behaviour of the MongoDB and MapReduce pipelines.

july_raw   = LOAD '$INPUT_JULY'   USING TextLoader() AS (line:chararray);
august_raw = LOAD '$INPUT_AUGUST' USING TextLoader() AS (line:chararray);
all_raw    = UNION july_raw, august_raw;


-- ─── 2. Apply CLF regex to every line ───────────────────────
--
-- CLF pattern:
--   ^(\S+)\s+-\s+-\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d{3}|-)\s+(\S+)
-- Groups:
--   1 = host
--   2 = timestamp  e.g. "01/Jul/1995:00:00:01 -0400"
--   3 = request    e.g. "GET /path HTTP/1.0"
--   4 = status     e.g. "200" or "-"
--   5 = bytes      e.g. "1234" or "-"

parsed_raw = FOREACH all_raw GENERATE
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


-- ─── 3. Split into valid and malformed ──────────────────────
-- Malformed: regex did not match (host IS NULL)
--            OR status is '-' or NULL  (same rule as parser.py)

valid_raw = FILTER parsed_raw BY
    host       IS NOT NULL
    AND status_raw IS NOT NULL
    AND status_raw != '-';

malformed_raw = FILTER parsed_raw BY
    host IS NULL
    OR status_raw IS NULL
    OR status_raw == '-';


-- ─── 4. Project structured fields from valid records ────────
--
-- Timestamp format: "DD/Mon/YYYY:HH:MM:SS zone"
--   day   = chars 0-1
--   month = chars 3-5  (e.g. "Jul")
--   year  = chars 7-10
--   hour  = chars 12-13
--
-- log_date is reconstructed as YYYY-MM-DD.
-- resource_path is the second token of the request string.
-- bytes_transferred: '-' or empty → 0.

structured = FOREACH valid_raw {
    day_str  = SUBSTRING(ts_raw, 0, 2);
    mon_str  = SUBSTRING(ts_raw, 3, 6);
    year_str = SUBSTRING(ts_raw, 7, 11);
    hour_str = SUBSTRING(ts_raw, 12, 14);

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

    resource = REGEX_EXTRACT(request_str, '^[A-Z]+\\s+(\\S+)', 1);

    bytes_val = (
        (bytes_raw IS NULL OR bytes_raw == '-' OR bytes_raw == '')
        ? 0L
        : (long) bytes_raw
    );

    GENERATE
        host                AS host:chararray,
        log_date_str        AS log_date:chararray,
        (int) hour_str      AS log_hour:int,
        resource            AS resource_path:chararray,
        (int) status_raw    AS status_code:int,
        bytes_val           AS bytes_transferred:long;
};

-- Drop any rows where date parsing failed (unknown month → mon_num='00')
clean = FILTER structured BY
    log_date   IS NOT NULL
    AND log_hour   IS NOT NULL
    AND status_code IS NOT NULL
    AND NOT (SUBSTRING(log_date, 5, 7) == '00');


-- ─── 5. Metadata: count valid and malformed records ─────────
-- These counts replace _count_records_locally() in Python.
-- Output schema: total_valid \t total_malformed

valid_count_grp    = GROUP clean       ALL;
malformed_count_grp = GROUP malformed_raw ALL;

valid_count    = FOREACH valid_count_grp    GENERATE COUNT(clean)         AS cnt:long;
malformed_count = FOREACH malformed_count_grp GENERATE COUNT(malformed_raw) AS cnt:long;

-- Cross to get both counts in one row
counts_cross = CROSS valid_count, malformed_count;
metadata_out = FOREACH counts_cross GENERATE
    valid_count::cnt    AS total_records:long,
    malformed_count::cnt AS malformed_records:long;

STORE metadata_out INTO '$OUTPUT_META' USING PigStorage('\t');


-- ═══════════════════════════════════════════════════════════════
-- QUERY 1 – Daily Traffic Summary
-- For each (log_date, status_code):
--   total request count, total bytes transferred
-- ═══════════════════════════════════════════════════════════════

q1_grouped = GROUP clean BY (log_date, status_code);

q1_results = FOREACH q1_grouped GENERATE
    FLATTEN(group)               AS (log_date:chararray, status_code:int),
    COUNT(clean)                 AS request_count:long,
    SUM(clean.bytes_transferred) AS total_bytes:long;

STORE q1_results
INTO '$OUTPUT_Q1'
USING PigStorage('\t');


-- ═══════════════════════════════════════════════════════════════
-- QUERY 2 – Top Requested Resources
-- Top $TOP_N resource paths by request count.
-- For each: total requests, total bytes, distinct host count.
-- ═══════════════════════════════════════════════════════════════

-- Exclude NULL resource paths
q2_input = FILTER clean BY resource_path IS NOT NULL;

q2_grouped = GROUP q2_input BY resource_path;

q2_agg = FOREACH q2_grouped {

    unique_hosts = DISTINCT q2_input.host;

    GENERATE
        group                             AS resource_path:chararray,
        COUNT(q2_input)                   AS request_count:long,
        SUM(q2_input.bytes_transferred)   AS total_bytes:long,
        COUNT(unique_hosts)               AS distinct_host_count:long;
};

-- Required for TOP-N semantics
q2_sorted = ORDER q2_agg BY request_count DESC;

q2_top = LIMIT q2_sorted $TOP_N;

STORE q2_top
INTO '$OUTPUT_Q2'
USING PigStorage('\t');


-- ═══════════════════════════════════════════════════════════════
-- QUERY 3 – Hourly Error Analysis
-- For each (log_date, log_hour):
--   error requests (status 400-599),
--   total requests,
--   error rate,
--   distinct hosts generating errors
-- ═══════════════════════════════════════════════════════════════

q3_tagged = FOREACH clean GENERATE
    log_date,
    log_hour,
    host,
    (
        status_code >= $ERROR_MIN AND
        status_code <= $ERROR_MAX
            ? 1
            : 0
    ) AS is_error:int;

q3_grouped = GROUP q3_tagged BY (log_date, log_hour);

q3_results = FOREACH q3_grouped {

    error_recs  = FILTER q3_tagged BY is_error == 1;
    error_hosts = DISTINCT error_recs.host;

    total_count = (double) COUNT(q3_tagged);
    error_count = (double) COUNT(error_recs);

    GENERATE
        FLATTEN(group)
            AS (log_date:chararray, log_hour:int),

        (long) error_count
            AS error_request_count:long,

        (long) total_count
            AS total_request_count:long,

        (
            total_count > 0
                ? error_count / total_count
                : 0.0
        ) AS error_rate:double,

        COUNT(error_hosts)
            AS distinct_error_hosts:long;
};

STORE q3_results
INTO '$OUTPUT_Q3'
USING PigStorage('\t');