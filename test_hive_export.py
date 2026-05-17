from parser import parse_file_in_batches

OUTPUT_FILE = "hive_scripts/temp/nasa_logs.tsv"

files = [
    "data/NASA_access_log_Jul95",
    "data/NASA_access_log_Aug95",
]

with open(OUTPUT_FILE, "w", encoding="utf-8") as out:

    for file_path in files:

        for _, records, malformed in parse_file_in_batches(
            file_path,
            batch_size=5000
        ):

            for r in records:

                row = [
                    r["host"],
                    r["timestamp"],
                    r["log_date"],
                    str(r["log_hour"]),
                    r["http_method"],
                    r["resource_path"],
                    r["protocol_version"],
                    str(r["status_code"]),
                    str(r["bytes_transferred"]),
                ]

                out.write("\t".join(row) + "\n")

print(f"TSV export completed → {OUTPUT_FILE}")
