# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
# ]
# ///

import argparse
import datetime
import hashlib
import json
import subprocess
import time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate TPC-H queries for DuckDB.")
    parser.add_argument("-q", "--query", type=int, nargs='*', help="Specific query numbers to generate (1-22). If not specified, all queries will be generated.")
    args = parser.parse_args()

    query_numbers = args.query if args.query else range(1, 23)
    times = []

    for i in query_numbers:
        EXPERIMENT = f"unnoised-q{i}"
        with open(f"./unnoised/q{i}.sql", "w") as f:
            query = f""".mode csv
PRAGMA tpch({i}); -- https://duckdb.org/docs/stable/extensions/tpch.html
.exit"""
            f.write(query)

        start = time.time()
        p = subprocess.run(f"duckdb --readonly ./data/tpch/tpch.duckdb < ./unnoised/q{i}.sql > ./unnoised/q{i}.csv && sync", shell=True, check=True)
        stop = time.time()
        elapsed = stop - start
        times.append({
            "query": f"Q{i}",
            "total": elapsed,
        })
        
        subprocess.run(f"rm ./unnoised/q{i}.sql", shell=True)
    
    json.dump(times, open("./unnoised/times.json", "w"), indent=4)

    with open("./unnoised/times.tsv", "w") as f:
        f.write("Query\tTotal\n")
        for entry in times:
            query = entry.get("query", "unknown")
            total = entry.get("total", "NA")
            f.write(f"{query}\t{total}\n")

    # make note of which database file was used
    print("Documenting generation in last_run.txt")
    with open("./unnoised/last_run.txt", "w") as f:
        f.write(f"{datetime.datetime.now().isoformat()}\n")
        # write a sha256 hash of the .duckdb file
        with open("./data/tpch/tpch.duckdb", "rb") as dbfile:
            digest = hashlib.file_digest(dbfile, "sha256")
        f.write(f"hash of ./data/tpch/tpch.duckdb: {digest.hexdigest()}\n")
        f.write(f"query numbers: {', '.join(str(q) for q in query_numbers)}\n")