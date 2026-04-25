# test_parser.py
# Run this with: python3 test_parser.py
# All tests must pass before you move to Day 2

from parser import parse_line, parse_file_in_batches


def test(description, line, expected_result):
    result = parse_line(line)
    if expected_result is None:
        assert result is None, f"FAIL [{description}]: expected None, got {result}"
        print(f"  PASS: {description}")
    else:
        assert result is not None, f"FAIL [{description}]: got None, expected {expected_result}"
        for key, val in expected_result.items():
            assert result[key] == val, (
                f"FAIL [{description}]: field '{key}' "
                f"expected {val!r}, got {result[key]!r}"
            )
        print(f"  PASS: {description}")


print("\n=== Parser Unit Tests ===\n")

# Test 1: Normal complete line
test(
    "Normal complete line",
    '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245',
    {
        "host": "199.72.81.55",
        "log_date": "1995-07-01",
        "log_hour": 0,
        "http_method": "GET",
        "resource_path": "/history/apollo/",
        "protocol_version": "HTTP/1.0",
        "status_code": 200,
        "bytes_transferred": 6245,
    },
)

# Test 2: Bytes field is dash
test(
    "Bytes field is dash",
    'burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/ HTTP/1.0" 304 -',
    {"status_code": 304, "bytes_transferred": 0},
)

# Test 3: Hostname instead of IP
test(
    "Hostname as host",
    'unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985',
    {"host": "unicomp6.unicomp.net", "log_date": "1995-07-01", "log_hour": 0},
)

# Test 4: 404 error
test(
    "404 status code",
    '128.159.122.110 - - [01/Jul/1995:00:00:12 -0400] "GET /missing.html HTTP/1.0" 404 -',
    {"status_code": 404, "bytes_transferred": 0},
)

# Test 5: Empty line
test("Empty line", "", None)

# Test 6: Completely garbage line
test("Garbage line", "this is not a log line at all", None)

# Test 7: August log (different date)
test(
    "August date parsing",
    '204.249.114.13 - - [01/Aug/1995:00:00:01 -0400] "GET /index.html HTTP/1.0" 200 1024',
    {"log_date": "1995-08-01"},
)

# Test 8: Hour extraction
test(
    "Hour extraction at midnight",
    '1.2.3.4 - - [15/Jul/1995:23:45:00 -0400] "GET /a HTTP/1.0" 200 100',
    {"log_hour": 23},
)

print("\n=== Batch Generator Test ===\n")

# Test batching with a small batch size on real file
data_file = "data/NASA_access_log_Jul95"
batch_size = 1000

total_good = 0
total_malformed = 0
batch_count = 0

for batch_id, good_records, malformed_count in parse_file_in_batches(data_file, batch_size):
    total_good += len(good_records)
    total_malformed += malformed_count
    batch_count += 1

    # Print every 500th batch so you can see progress
    if batch_id % 500 == 0:
        print(f"  Batch {batch_id}: {len(good_records)} good, {malformed_count} malformed")

print("\nFinal summary:")
print(f"  Total batches    : {batch_count}")
print(f"  Total good lines : {total_good}")
print(f"  Total malformed  : {total_malformed}")
print(f"  Avg batch size   : {(total_good + total_malformed) / batch_count:.1f}")
print("\nAll tests passed!")
