# Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics

Resume-grade NoSQL systems project based on the NASA HTTP logs, designed to run the same ETL + analytics workload across multiple backends (`Pig`, `MapReduce`, `MongoDB`, `Hive`) with fair comparison and common reporting.

This project implements an end-to-end Big Data ETL pipeline workflow covering ingestion, transformation, distributed processing concepts, and NoSQL analytics.

---

## Current Status (Day 1 Completed)

### Done so far

- Project workspace initialized with clean folder structure.
- NASA July and August log datasets downloaded and decompressed.
- Shared parser (`parser.py`) implemented for normalized field extraction.
- Batch generator implemented with malformed-record counting.
- Full parser tests (`test_parser.py`) added and executed successfully.

### In progress (next phases)

- Pipeline-specific ETL implementations (`Pig`, `MapReduce`, `MongoDB`, `Hive`)
- Relational DB loading (MySQL/PostgreSQL)
- Unified run controller + reporting module
- Runtime and batching comparison reports

---

## Project Structure

```text
etl_project/
├── data/                 # NASA raw logs (.gz + decompressed files)
├── db/                   # Relational schema/load scripts (planned)
├── docs/                 # Documentation artifacts
├── hive_scripts/         # Hive ETL/query scripts (planned)
├── logs/                 # Runtime logs and execution metadata
├── mapreduce/            # MapReduce jobs (planned)
├── pig_scripts/          # Pig scripts (planned)
├── pipelines/            # Orchestration/adapters (planned)
├── reports/              # Reporting outputs (planned)
├── results/              # Intermediate/final aggregations
├── src/                  # Core app modules (planned)
├── tests/                # Test utilities
├── parser.py             # Shared parser used by all pipelines
└── test_parser.py        # Parser unit + batch test suite
```

---

## Dataset

Official source (Internet Traffic Archive):

- [NASA HTTP dataset page](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html)
- [July 1995 trace](https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)
- [August 1995 trace](https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz)

### Local verification (completed)

- `NASA_access_log_Jul95`: `1,891,715` lines
- `NASA_access_log_Aug95`: `1,569,898` lines

---

## Environment Setup

### Python dependencies

- `pymongo`
- `pymysql`
- `tabulate`
- `python-dateutil`

### Recommended setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install pymongo pymysql tabulate python-dateutil
```

---

## Shared Parser Contract (`parser.py`)

The parser is the core consistency layer for all future pipelines.  
It converts each raw log line into a normalized record with this schema:

- `host`
- `log_date` (YYYY-MM-DD)
- `log_hour` (0-23)
- `http_method`
- `resource_path`
- `protocol_version`
- `status_code`
- `bytes_transferred`

### Edge-case handling implemented

- Empty line -> malformed (`None`)
- Broken/unparseable line -> malformed (`None`)
- Bytes value `-` -> `bytes_transferred = 0`
- Invalid bytes token -> fallback to `0`
- Unknown month token -> malformed (`None`)

### Batching behavior implemented

`parse_file_in_batches(filepath, batch_size)` yields:

- `batch_id` (starts at `1`)
- `good_records` (parsed records list)
- `malformed_count` (failed rows in that batch)

Input rows are counted toward batch size regardless of parse success, matching project batching rules.

---

## Testing

Comprehensive parser tests are provided in `test_parser.py`.

### Includes

- Unit checks for:
  - normal valid line
  - bytes dash handling
  - host as DNS name
  - error status parsing
  - empty/garbage malformed lines
  - month/date/hour extraction
- Full-file batch run on July dataset (`batch_size = 1000`)

### Run tests

```powershell
$env:PYTHONPATH='.'
.\.venv\Scripts\python test_parser.py
```

### Latest executed result

- Unit tests: **PASS (8/8)**
- Batch generator: **PASS**
- Summary from run:
  - `Total batches`: `1892`
  - `Total good lines`: `1889796`
  - `Total malformed`: `1919`
  - `Avg batch size`: `999.8`

---

## Why this foundation matters

This project is designed as a comparative systems prototype, so fairness depends on using:

- the same raw dataset,
- the same parsing semantics,
- the same ETL/query logic,
- and the same batching semantics

across all execution backends.

The completed Day 1 work establishes that shared baseline.

---

## Next Milestones

1. Freeze parser contract and expose reusable interface for all pipelines.
2. Implement Query 1/2/3 logic in one baseline reference path.
3. Build backend-specific implementations for Pig, MapReduce, MongoDB, and Hive.
4. Load aggregated outputs into MySQL/PostgreSQL with run metadata.
5. Build reporting module for correctness + runtime + batching comparison.

---

## Author Notes

This repository is being developed in iterative milestones (Day 1, Day 2, ...), with correctness-first implementation and reproducible test evidence at each stage.
