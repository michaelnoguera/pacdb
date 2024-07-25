import duckdb
import pyarrow.parquet as pq
con = duckdb.connect(database=':memory:')
con.execute("INSTALL tpch; LOAD tpch")
con.execute("CALL dbgen(sf=1)")
print(con.execute("show tables").fetchall())
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
for t in tables:
    res = con.query("SELECT * FROM " + t)
    pq.write_table(res.to_arrow_table(), t + ".parquet")