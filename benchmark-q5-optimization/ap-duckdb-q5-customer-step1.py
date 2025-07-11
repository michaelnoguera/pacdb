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

# In[1]:


#!/usr/bin/env python
# coding: utf-8

import os
import pickle

import duckdb
import polars as pl

from timer import Timer


# In[2]:


EXPERIMENT = "pac-duckdb-q1"
OUTPUT_DIR = f"./outputs/{EXPERIMENT}-step1"
SAMPLES = 1024

SAMPLE_STEP = f"""
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
"""

PREPARE_STEP = """
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
"""

INDEX_COLS = ['l_returnflag', 'l_linestatus']
OUTPUT_COLS = ['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']


# In[3]:


# Parameters
EXPERIMENT = "ap-duckdb-q5-customer"
OUTPUT_DIR = "./outputs/ap-duckdb-q5-customer-step1"
SAMPLES = 1024
SAMPLE_STEP = "DROP TABLE IF EXISTS random_samples;\n\nCREATE TABLE random_samples AS\nWITH sample_numbers AS MATERIALIZED (\n    SELECT range AS sample_id FROM range(1024)\n), random_values AS MATERIALIZED (\n    SELECT \n        sample_numbers.sample_id,\n        customer.rowid AS row_id,\n        (RANDOM() > 0.5)::BOOLEAN AS random_binary\n    FROM sample_numbers\n    JOIN customer ON TRUE  -- Cross join to duplicate rows for each sample\n)\nSELECT\n    sample_id,\n    row_id,\n    random_binary\nFROM random_values\nORDER BY sample_id, row_id;"
PREPARE_STEP = "DEALLOCATE PREPARE run_query;\n\nPREPARE run_query AS \nSELECT\n    n_name,\n    sum(l_extendedprice * (1 - l_discount)) AS revenue\nFROM\n    (SELECT * FROM customer\n              JOIN random_samples AS rs ON rs.row_id = customer.rowid\n              AND rs.random_binary = TRUE\n              AND rs.sample_id = $sample) as customer,\n    orders,\n    lineitem,\n    supplier,\n    nation,\n    region\nWHERE\n    c_custkey = o_custkey\n    AND l_orderkey = o_orderkey\n    AND l_suppkey = s_suppkey\n    AND c_nationkey = s_nationkey\n    AND s_nationkey = n_nationkey\n    AND n_regionkey = r_regionkey\n    AND r_name = 'ASIA'\n    AND o_orderdate >= CAST('1994-01-01' AS date)\n    AND o_orderdate < CAST('1995-01-01' AS date)\nGROUP BY\n    n_name\nORDER BY\n    revenue DESC;"
INDEX_COLS = ["n_name"]
OUTPUT_COLS = ["revenue"]


# In[4]:


# Timer setup
timer = Timer(experiment=EXPERIMENT, step="step1", output_dir="./times")

# Ensure the output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# In[5]:


# duckdb load data/tpch/tpch.duckdb into the temporary in-memory database
timer.start("load_data")
con = duckdb.connect(database=':memory:')
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
for t in tables:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM 'data/tpch/{t}.parquet'")
timer.end()


# In[6]:


# Construct the table of random samples
# to use, join it with the lineitem table (for specific sample # s) and filter to just the
# rows where random_binary = 1.0
# This will give us a 50% sample of the lineitem table for each sample # s

assert SAMPLES % 2 == 0, "SAMPLES must be even to create complementary samples."

timer.start("create_random_samples_table")
random_samples = con.execute(SAMPLE_STEP)
timer.end()


# The randomness of what rows are chosen is saved to disk in `random_binary.json`. For each sample #, there is an array with one entry per row, where 1 means the row was chosen and 0 means it was not.

# In[7]:


try:
    con.execute("""
    SELECT sample_id, array_agg(random_binary::TINYINT) as random_binary
    FROM random_samples
    GROUP BY sample_id;
    """).pl().write_json(f"{OUTPUT_DIR}/random_binary.json")
except duckdb.BinderException as e:
    print(f"No random_binary column present. {e}")
    selected_rows = con.execute("""
    SELECT sample_id, array_agg(row_id) as selected_rows
    FROM random_samples
    GROUP BY sample_id;
    """).pl().write_json(f"{OUTPUT_DIR}/selected_rows.json")


# Query is specified as a prepared statement. We will then execute it once per sample.

# In[8]:


# Query
timer.start("prepare_query")
con.execute(PREPARE_STEP)
timer.end()

# Run query to see output
dfs0 = con.execute(f"EXECUTE run_query(sample := {0});").pl()

# Save csv copies of the first 5 samples
os.makedirs(f"{OUTPUT_DIR}/csv", exist_ok=True)
for s in range(5):
    con.execute(f"EXECUTE run_query(sample := {s});").pl().write_csv(f"{OUTPUT_DIR}/csv/sample_{s}.csv")

dfs0


# In[9]:


# Run the query for each sample, but accumulate in a pl.DataFrame instead of a list
timer.start("run_query_for_all_samples")
dfsdf: pl.DataFrame = pl.concat(
    con.execute(f"EXECUTE run_query(sample := {s});").pl().insert_column(0, pl.lit(s).alias("sample"))
    for s in range(SAMPLES)
)
timer.end()
dfsdf


# In[10]:


timer.start("add_rank_column")
dfsdf = dfsdf.with_columns(
    pl.int_range(pl.len()).over("sample").alias("rank")
)
timer.end()


# In[11]:


# Define which columns are the group-by keys (INDEX_COLS) and which are the output columns (OUTPUT_COLS)
# - moved to parameters cell at top of notebook

# Save these to disk for later use
timer.start("save_index_and_output_cols")
with open(f"{OUTPUT_DIR}/INDEX_COLS.pkl", 'wb') as f:
    pickle.dump(INDEX_COLS, f)
with open(f"{OUTPUT_DIR}/OUTPUT_COLS.pkl", 'wb') as f:
    pickle.dump(OUTPUT_COLS, f)
timer.end()


# In[12]:


# Combine all the samples into one table, grouped-by the group-by keys. Each cell contains an n <= # of samples length array of values.
timer.start("drop_sample_and_group_by")
DEFAULT_INDEX_COLS = ["rank"]
listdf = dfsdf.drop("sample").group_by(INDEX_COLS or DEFAULT_INDEX_COLS, maintain_order=True).all()
timer.end()
listdf


# In[13]:


# What are all the possible group-by key combinations?
timer.start("get_all_groups")
allgroups: pl.DataFrame = listdf.select(INDEX_COLS or DEFAULT_INDEX_COLS)
timer.end()
allgroups.to_dicts()


# In[14]:


# Template for the final output, including all possible group-by groups
# Obtained by collecting all the samples in a big table and then keeping only the first occurrence of each groupby key.
# Then, fill all OUTPUT_COLS with nulls
timer.start("save_template")
templatedf = dfsdf.drop("sample").group_by(INDEX_COLS or DEFAULT_INDEX_COLS, maintain_order=True).first()
templatedf = templatedf.clear(n=len(allgroups)).with_columns(allgroups)
templatedf

with open(f"{OUTPUT_DIR}/template.pkl", "wb") as f:
    pickle.dump(templatedf, f)
timer.end()


# In[15]:


# Write all table entries in the output table to their own JSON files. Each file has a number, the information of which file corresponds to which table entry
# is stored in reverse_map.json (as well as in the files themselves)
timer.start("write_json_files")
os.makedirs(f"{OUTPUT_DIR}/json", exist_ok=True)
i: int = 0
for col in OUTPUT_COLS:
    for group in allgroups.iter_rows(named=True):
        values = listdf.filter(pl.col(k).eq(v) for k, v in group.items()).select(col).to_series()
        j = pl.DataFrame().with_columns([
            pl.lit(col).alias("col"),
            pl.lit(group).alias("row"),
            pl.lit(values.explode().dtype.__repr__()).alias("dtype"),
            pl.lit(SAMPLES).alias("samples"),
            values.alias("values"),
        ])
        j.write_json(f"{OUTPUT_DIR}/json/{i}.json")
        i+=1
os.fsync(os.open(f"{OUTPUT_DIR}/json", os.O_RDONLY))  # Ensure all writes are flushed to disk
timer.end()

