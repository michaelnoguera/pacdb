# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
# ]
# ///

import subprocess

for i in range(1, 23):
    with open(f"./q{i}.sql", "w") as f:
        query = f""".mode csv
PRAGMA tpch({i}); -- https://duckdb.org/docs/stable/extensions/tpch.html
.exit"""
        f.write(query)
    p = subprocess.run(f"uvx duckdb ../data/tpch/tpch.duckdb < q{i}.sql > q{i}.csv", shell=True, check=True)
    subprocess.run(f"rm q{i}.sql", shell=True)