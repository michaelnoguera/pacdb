#!../../.venv/bin/python3
"""
Generate the TPC-H tables by using the DuckDB tpch plugin.
Export tables to parquet files saved here for use in experiments.
"""

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]

con = duckdb.connect(database='./tpch.duckdb')

tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
for t in tables:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM './{t}.parquet'")

con.close()