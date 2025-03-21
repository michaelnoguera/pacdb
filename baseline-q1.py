#!.venv/bin/python3
"""
Demo of our modified TPC-H query 4.
"""
import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]

# print the original tpch q1
# with duckdb.connect(database=':memory:') as con:
#     con.execute("INSTALL tpch; LOAD tpch;")
#     query = (con.execute("""
#                 SELECT query
#                 FROM tpch_queries()
#                 WHERE query_nr = 1
#                 """).fetchone() or (""))[0]
#     print(query.replace("\\n", "\n"))

con = duckdb.connect(database=':memory:')

#tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
tables = ["lineitem"]
for t in tables:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM 'data/tpch/{t}.parquet'")

# size of each table
print("Table sizes:")
for t in tables:
    print(f"{t}: {con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()}")

# Original Query 4
print("Original Query 1:")
print(con.execute("""
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
""").fetch_df())

# Modified to only include the '1-URGENT' group
# print("Modified Query 1:")
# print(con.execute("""
# SELECT
#     sum(l_quantity) AS sum_qty,
#     sum(l_extendedprice) AS sum_base_price,
#     sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
#     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
#     avg(l_quantity) AS avg_qty,
#     avg(l_extendedprice) AS avg_price,
#     avg(l_discount) AS avg_disc,
#     count(*) AS count_order
# FROM
#     lineitem
# WHERE
#     l_shipdate <= CAST('1998-09-02' AS date)
#     AND l_returnflag = 'A'
#     AND l_linestatus = 'F'
# GROUP BY
#     l_returnflag,
#     l_linestatus
# ORDER BY
#     l_returnflag,
#     l_linestatus;
# """).fetch_df())

# print("Very Modified Query 1:")
# print(con.execute("""
# SELECT
#     count(*) AS count_order
# FROM
#     lineitem
# WHERE
#     l_shipdate <= CAST('1998-09-02' AS date)
#     AND l_returnflag = 'A'
#     AND l_linestatus = 'F';
# """).fetch_df())

# print("Chorus Rewritten Very Modified Query 1:")
# print(con.execute("""
# SELECT
#     COUNT(*) + 20.0 * (CASE WHEN random() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(random() - 0.5))) AS count_order
# FROM 
#     (SELECT l_returnflag, l_linestatus, l_shipdate FROM lineitem) AS t
# WHERE
#     l_shipdate <= CAST('1998-09-02' AS date)
#     AND l_returnflag = 'A'
#     AND l_linestatus = 'F'
# """).fetch_df())

con.close()