import subprocess

for i in range(1, 23):
    with open(f"unnoised/q{i}.sql", "w") as f:
        query = f""".mode csv
PRAGMA tpch({i}); -- https://duckdb.org/docs/stable/extensions/tpch.html
.exit"""
        f.write(query)
    subprocess.run(f"duckdb ./data/tpch/tpch.duckdb < unnoised/q{i}.sql > unnoised/q{i}.csv", shell=True)