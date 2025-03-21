EXPERIMENT = 'chorus-q1'
OUTPUT_DIR = f'./outputs/{EXPERIMENT}'
GENERATE = False
USE_EVEN_NUMBER_OF_INPUT_ROWS = False
SEED_RANDOM_NUMBER_GENERATOR = True

SAMPLING_METHOD = 'poisson' # 'poisson' or 'half'

if GENERATE:
    print("GENERATE = True, so we will generate new samples.")
else:
    print("GENERATE = False, so we will load saved output from files rather than recomputing.")

import os
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

from typing import Any, Callable, Dict, List, Tuple, Union

import numpy as np
if SEED_RANDOM_NUMBER_GENERATOR:
    np.random.seed(0)

import pandas as pd
from pandas import DataFrame
import concurrent.futures
import pyarrow as pa
import pyarrow.parquet as pq
import pickle
from numpy.random import laplace
from functools import reduce
import operator
from IPython.display import display, HTML
from datetime import date
import duckdb

con = duckdb.connect(database=':memory:')

#tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
tables = ["lineitem"]
for t in tables:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM 'data/tpch/{t}.parquet'")

mi_to_chorus_coef = {0.015625: 5.612887899785308,
 0.03125: 3.936925626444774,
 0.0625: 2.7381067769158585,
 0.25: 1.2175732025240547,
 1.0: 0.1737179436514752,
 2.0: 0.1737179436514752,
 4.0: 0.1737179436514752,
 16.0: 0.1737179436514752}

def query_template(mi):
    return (f"""
SELECT
    COUNT(*) + {mi_to_chorus_coef[mi]} * (CASE WHEN random() - 0.5 < 0 THEN -1.0 ELSE 1.0 END * LN(1 - 2 * ABS(random() - 0.5))) AS count_order
FROM 
    (SELECT l_returnflag, l_linestatus, l_shipdate FROM lineitem) AS t
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
    AND l_returnflag = 'A'
    AND l_linestatus = 'F'
""")


SAMPLES = 8192
assert SAMPLES % 2 == 0, "We need an even number of samples for paired sampling."
number_of_pairs = SAMPLES // 2

OUTPUT_COLS = ['count_order']
INPUT_COLS = ['count_order']

MI_OPTIONS = [1/64, 1/32, 1/16, 1/4, 1., 2., 4., 16.]
EXPERIMENTS = 1000



if GENERATE:
    # df = pd.DataFrame([], columns=['mi', 'count', 'sum', 'mean', 'var'])
    experiment_results = []

    for mi in MI_OPTIONS:
        for e in range(EXPERIMENTS):
            q = query_template(mi)
            release: float = (con.execute(q).fetchone() or tuple([-1.0]))[0]
            experiment_results.append([mi, release])
    
    df = pd.DataFrame(experiment_results, columns=['mi', *OUTPUT_COLS])
    
    # Save the new data to outputs/...
    df.to_parquet(f'{OUTPUT_DIR}/dp_results.parquet')
else:
    # Load the saved data from outputs/...
    df = pq.read_table(f'{OUTPUT_DIR}/dp_results.parquet').to_pandas()

print(df.head())