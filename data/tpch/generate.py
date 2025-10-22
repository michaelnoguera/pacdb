#!../../.venv/bin/python3
"""
Generate the TPC-H tables by using the DuckDB tpch plugin.
Export tables to parquet files saved here for use in experiments.
"""
import argparse
import logging
import os

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]

# default scale factor is 1gb
parser = argparse.ArgumentParser(description="Generate TPC-H parquet files.")
parser.add_argument('--sf', type=int, default=1, help='Scale factor for TPC-H data generation (default: 1)')
parser.add_argument('--parquet', action='store_true', help='Export tables to parquet files')
args = parser.parse_args()
SCALE_FACTOR = args.sf
PARQUET_EXPORT = args.parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logging.info(f"Starting TPC-H data generation with scale factor {SCALE_FACTOR}")
if PARQUET_EXPORT:
    logging.info("Parquet export enabled")
else:
    logging.info("Parquet export disabled")

# clean up any previous database
if os.path.exists('./tpch.duckdb'):
    logging.info("Removing existing tpch.duckdb database file")
    os.remove('./tpch.duckdb')

logging.info("Connecting to DuckDB database")
con = duckdb.connect(database='./tpch.duckdb')

# generate the data
logging.info("Installing and loading tpch extension")
con.execute("INSTALL tpch; LOAD tpch")
logging.info("Generating TPC-H data")
con.execute(f"CALL dbgen(sf={SCALE_FACTOR})")
tables_list = con.execute("show tables").fetchall()
logging.info(f"Tables generated: {tables_list}")

# export each as parquet
if PARQUET_EXPORT:
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    for t in tables:
        logging.info(f"Exporting table {t} to {t}.parquet")
        res = con.query("SELECT * FROM " + t)
        pq.write_table(res.to_arrow_table(), t + ".parquet")

logging.info("Closing DuckDB connection")
con.close()