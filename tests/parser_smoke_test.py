from parser import parse_file_in_batches, parse_line


def main():
    print(
        parse_line(
            '199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245'
        )
    )
    print(
        parse_line(
            'dd15-062.compuserve.com - - [01/Jul/1995:00:01:12 -0400] "GET /news/sci.space.shuttle/archive/sci-space-shuttle-22-apr-1995-40.txt HTTP/1.0" 404 -'
        )
    )
    print(
        parse_line(
            'drjo014a102.embratel.net.br - - [27/Jul/1995:21:59:14 -0400] "GET  //www.umcc.umich.edu HTTP/1.0" 302 -'
        )
    )
    print(parse_line("alyssa.p"))

    batch_id, good_records, malformed_count = next(
        parse_file_in_batches("data/NASA_access_log_Jul95", 1000)
    )
    print(
        {
            "batch_id": batch_id,
            "good_records": len(good_records),
            "malformed_count": malformed_count,
        }
    )


if __name__ == "__main__":
    main()
