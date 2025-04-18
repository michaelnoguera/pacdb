#!/usr/bin/env python
# coding: utf-8

EXPERIMENT = 'pac-duckdb-q1'
OUTPUT_DIR = f'./outputs/{EXPERIMENT}'
GENERATE = False
USE_EVEN_NUMBER_OF_INPUT_ROWS = False

if GENERATE:
    print("GENERATE = True, so we will generate new samples.")
else:
    print("GENERATE = False, so we will load saved output from files rather than recomputing.")

import os
from typing import List
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

import duckdb
import polars as pl
import pyarrow as pa

# duckdb load data/tpch/tpch.duckdb
#con = duckdb.connect(database='data/tpch/tpch.duckdb', read_only=True)
con = duckdb.connect(database=':memory:')
#tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
tables = ["lineitem", "orders"]
for t in tables:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM 'data/tpch/{t}.parquet'")

lineitem_df = con.execute("SELECT * FROM lineitem").fetchdf()
orders_df = con.execute("SELECT * FROM orders").fetchdf()

row_count = lineitem_df.shape[0]

# Construct the table of random samples
# to use, join it with the lineitem table (for specific sample # s) and filter to just the
# rows where random_binary = 1.0
# This will give us a 50% sample of the lineitem table for each sample # s

SAMPLES = 6
TABLE_TO_SAMPLE = 'orders'
assert SAMPLES % 2 == 0, "SAMPLES must be even to create complementary samples."

con.execute(f"""
CREATE TABLE random_samples AS
WITH sample_numbers AS (
    SELECT range AS sample_id FROM range({SAMPLES//2})
),
random_values AS (
    SELECT 
        sample_numbers.sample_id, 
        {TABLE_TO_SAMPLE}.rowid AS row_id,
        FLOOR(RANDOM() * 2) AS random_binary
    FROM sample_numbers
    JOIN {TABLE_TO_SAMPLE} ON TRUE  -- Cross join to duplicate rows for each sample
)
SELECT
    sample_id,
    row_id,
    random_binary
FROM random_values
UNION ALL
SELECT -- select the complementary samples too
    {SAMPLES//2} + sample_id,
    row_id,
    1 - random_binary  -- Inverse the random_binary to get the complementary sample
FROM random_values;
""")


con.execute("""
PREPARE count_orders AS 
SELECT
    l_returnflag,
    l_linestatus,
    2*sum(l_quantity) AS sum_qty,
    2*sum(l_extendedprice) AS sum_base_price,
    2*sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    2*sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    2*count(*) AS count_order
FROM
    lineitem
JOIN orders ON lineitem.l_orderkey = orders.o_orderkey
JOIN random_samples AS rs
    ON rs.row_id = orders.rowid
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
    AND rs.random_binary = 1.0
    AND rs.sample_id = $sample
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
""")

print("number of selected orders")
print(con.execute("""
SELECT SUM(random_samples.random_binary) AS num_orders
FROM random_samples
JOIN orders ON random_samples.row_id = orders.rowid
GROUP BY sample_id;
""").pl().with_row_index())

print("number of selected lineitems")
sample_sizes = con.execute("""
SELECT COUNT(*) AS sample_size
FROM random_samples
JOIN orders ON random_samples.row_id = orders.rowid
JOIN lineitem ON orders.o_orderkey = lineitem.l_orderkey
WHERE random_binary = 1.0
GROUP BY sample_id;
""").pl().with_row_index()

print(sample_sizes)


#dfs: List[pl.DataFrame] = []
for s in range(SAMPLES):
    print(f"con.execute(f'EXECUTE count_orders(sample := {s});")
    print(con.execute(f"EXECUTE count_orders(sample := {s});").pl())
    #dfs.append(con.execute(f"EXECUTE count_orders(sample := {s});").pl())

#dfs[0]




for s in range(SAMPLES):
    con.execute(f"EXECUTE count_orders(sample := {s});")
