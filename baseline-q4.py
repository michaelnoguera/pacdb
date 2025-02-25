#!.venv/bin/python3
"""
Demo of our modified TPC-H query 4.
"""
import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]

con = duckdb.connect(database=':memory:')

#tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
tables = ["orders", "lineitem"]
for t in tables:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM 'data/tpch/{t}.parquet'")

# Original Query 4
print("Original Query 4:")
print(con.execute("""
SELECT o_orderpriority, count(*) AS order_count
FROM orders
WHERE o_orderdate >= CAST('1993-07-01' AS date)
AND o_orderdate < CAST('1993-10-01' AS date)
AND EXISTS (
    SELECT
        *
    FROM
        lineitem
    WHERE
        l_orderkey = o_orderkey
        AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
""").fetchall())

# Modified to only include the '1-URGENT' group
print("Modified Query 4:")
print(con.execute("""
SELECT count(*) AS order_count
FROM orders
WHERE o_orderpriority = '1-URGENT'
AND o_orderdate >= CAST('1993-07-01' AS date)
AND o_orderdate < CAST('1993-10-01' AS date)
AND EXISTS (
    SELECT
        *
    FROM
        lineitem
    WHERE
        l_orderkey = o_orderkey
        AND l_commitdate < l_receiptdate);
""").fetchall())
