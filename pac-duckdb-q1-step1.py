#!/usr/bin/env python
# coding: utf-8

# ## DuckDB Notebook
# 
# This notebook generates a bunch of raw outputs, without applying PAC, to be consumed by a second stage.

# ```
#  group by              output cols                        
#  key cols ┌────────┬────────┬────────┬────────┐           
#         │ │   A    │   B    │   C    │   D    │           
#       ┌─▼─┼────────┼────────┼────────┼────────┤           
#       │ 1 │   2    │        │        │        │           
#       ├───┼───|────┼────────┼────────┼────────┤           
#       │ 2 │   │    │        │        │        │           
#       ├───┼───┼────┼────────┼────────┼────────┤           
#       │ 3 │   │    │        │        │        │           
#       └───┴───┼────┴────────┴────────┴────────┘           
#               ▼                 A_1.json                  
#        Sample 0:   A1=2        ┌─────────────────────────┐
#        Sample 1:   A1=4  ───▶  │{                        │
#              ...               │    col: A               │
#        Sample 999: A1=3        │    row: 1               │
#                                │    value: [2, 4, ... 3] │
#                                │}                        │
#                                └─────────────────────────┘
# ```

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

import os
import pickle
import shutil

import duckdb
import polars as pl

EXPERIMENT = "pac-duckdb-q1"
OUTPUT_DIR = f"./outputs/{EXPERIMENT}-step1"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# In[ ]:


# duckdb load data/tpch/tpch.duckdb into the temporary in-memory database
con = duckdb.connect(database=':memory:')
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
for t in tables:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM 'data/tpch/{t}.parquet'")


# In[3]:


# Construct the table of random samples
# to use, join it with the lineitem table (for specific sample # s) and filter to just the
# rows where random_binary = 1.0
# This will give us a 50% sample of the lineitem table for each sample # s

SAMPLES = 1024
assert SAMPLES % 2 == 0, "SAMPLES must be even to create complementary samples."

random_samples = con.execute(f"""
DROP TABLE IF EXISTS random_samples;

CREATE TABLE random_samples AS
WITH sample_numbers AS MATERIALIZED (
    SELECT range AS sample_id FROM range({SAMPLES//2})
), random_values AS MATERIALIZED (
    SELECT 
        sample_numbers.sample_id,
        customer.rowid AS row_id,
        (RANDOM() > 0.5)::BOOLEAN AS random_binary
    FROM sample_numbers
    JOIN customer ON TRUE  -- Cross join to duplicate rows for each sample
)
SELECT
    sample_id,
    row_id,
    random_binary
FROM random_values
UNION ALL
SELECT -- select the complementary samples too
    ({SAMPLES//2}) + sample_id,
    row_id,
    NOT random_binary  -- Inverse the random_binary to get the complementary sample
FROM random_values
ORDER BY sample_id, row_id;
""")


# The randomness of what rows are chosen is saved to disk in `random_binary.json`. For each sample #, there is an array with one entry per row, where 1 means the row was chosen and 0 means it was not.

# In[ ]:


con.execute("""
SELECT sample_id, array_agg(random_binary::TINYINT) as random_binary
FROM random_samples
GROUP BY sample_id;
""").pl().write_json(f"{OUTPUT_DIR}/random_binary.json")


# Query is specified as a prepared statement. We will then execute it once per sample.

# In[5]:


# Query
con.execute("""
DEALLOCATE PREPARE run_query;

PREPARE run_query AS 
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
JOIN customer ON orders.o_custkey = customer.c_custkey
JOIN random_samples AS rs
    ON rs.row_id = customer.rowid
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
    AND rs.random_binary = TRUE
    AND rs.sample_id = $sample
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;
""")

# Run query to see output
dfs0 = con.execute(f"EXECUTE run_query(sample := {0});").pl()

# Save csv copies of the first 5 samples
os.makedirs(f"{OUTPUT_DIR}/csv", exist_ok=True)
for s in range(5):
    con.execute(f"EXECUTE run_query(sample := {s});").pl().write_csv(f"{OUTPUT_DIR}/csv/sample_{s}.csv")

dfs0


# In[6]:


# Run the query for each sample, but accumulate in a pl.DataFrame instead of a list
dfsdf: pl.DataFrame = pl.concat(
    con.execute(f"EXECUTE run_query(sample := {s});").pl().insert_column(0, pl.lit(s).alias("sample"))
    for s in range(SAMPLES)
)
dfsdf


# In[7]:


# Define which columns are the group-by keys (INDEX_COLS) and which are the output columns (OUTPUT_COLS)
INDEX_COLS = ['l_returnflag', 'l_linestatus']
OUTPUT_COLS = ['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']

# Save these to disk for later use
with open(f"{OUTPUT_DIR}/INDEX_COLS.pkl", 'wb') as f:
    pickle.dump(INDEX_COLS, f)
with open(f"{OUTPUT_DIR}/OUTPUT_COLS.pkl", 'wb') as f:
    pickle.dump(OUTPUT_COLS, f)


# In[8]:


# Combine all the samples into one table, grouped-by the group-by keys. Each cell contains an n <= # of samples length array of values.
listdf = (dfsdf.group_by(INDEX_COLS, maintain_order=True)
      .all()
      .drop("sample"))
listdf


# In[9]:


# What are all the possible group-by key combinations?
allgroups: pl.DataFrame = listdf.select(INDEX_COLS)
allgroups.to_dicts()


# In[10]:


# Template for the final output, including all possible group-by groups
# Obtained by collecting all the samples in a big table and then keeping only the first occurrence of each groupby key.
# Then, fill all OUTPUT_COLS with nulls
templatedf = dfsdf.drop("sample").group_by(INDEX_COLS, maintain_order=True).first()
templatedf = templatedf.clear(n=len(allgroups)).with_columns(allgroups)
templatedf

with open(f"{OUTPUT_DIR}/template.pkl", "wb") as f:
    pickle.dump(templatedf, f)


# In[11]:


# Write all table entries in the output table to their own JSON files. Each file has a number, the information of which file corresponds to which table entry
# is stored in reverse_map.json (as well as in the files themselves)
os.makedirs(f"{OUTPUT_DIR}/json", exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/pkl", exist_ok=True)
i: int = 0
for col in OUTPUT_COLS:
    for group in allgroups.iter_rows(named=True):
        values = listdf.filter(pl.col(k).eq(v) for k, v in group.items()).select(col).to_series()
        j = pl.DataFrame().with_columns([
            pl.lit(col).alias("col"),
            pl.lit(group).alias("row"),
            pl.lit(values.explode().dtype.__repr__()).alias("dtype"),
            values.alias("values"),
        ])
        j.write_json(f"{OUTPUT_DIR}/json/{i}.json")
        i+=1


# In[12]:


# zip the OUTPUT_DIR
shutil.make_archive(OUTPUT_DIR, 'zip', OUTPUT_DIR)

