# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
# ]
# ///

import argparse
import subprocess

from timer import Timer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate TPC-H queries for DuckDB.")
    parser.add_argument("-q", "--query", type=int, nargs='*', help="Specific query numbers to generate (1-22). If not specified, all queries will be generated.")
    args = parser.parse_args()

    query_numbers = args.query if args.query else range(1, 23)

    for i in query_numbers:
        EXPERIMENT = f"unnoised-q{i}"
        with open(f"./unnoised/q{i}.sql", "w") as f:
            query = f""".mode csv
PRAGMA tpch({i}); -- https://duckdb.org/docs/stable/extensions/tpch.html
.exit"""
            f.write(query)
        timer = Timer(EXPERIMENT, "total", "./times")
        timer.start("run_sql_file_in_duckdb")
        p = subprocess.run(f"uvx duckdb ./data/tpch/tpch.duckdb < ./unnoised/q{i}.sql > ./unnoised/q{i}.csv", shell=True, check=True)
        timer.end()
        subprocess.run(f"rm ./unnoised/q{i}.sql", shell=True)