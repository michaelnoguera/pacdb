import os
import pickle
import subprocess

import polars as pl
import papermill as pm
import parse

QUERYFOLDER = "./queries"

# Execute all of ./queries/{query}.sql
pattern = parse.compile("{q}.sql")
queries_to_run = []
for queryfile in os.listdir(QUERYFOLDER):
    result = pattern.parse(queryfile)
    if result:
        query = result["q"]
        queries_to_run.append(query)

for query in queries_to_run:
    try:
        print(f"Running query: {query}")

        QUERY = query
        
        EXPERIMENT = f"ap-duckdb-{QUERY}"
        OUTPUT_DIR = f"./outputs/{EXPERIMENT}-step3"

        INDEX_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/INDEX_COLS.pkl', 'rb'))
        OUTPUT_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/OUTPUT_COLS.pkl', 'rb'))
        templatedf_path = f'./outputs/{EXPERIMENT}-step1/template.pkl'

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # INDEX_COLS = ['l_returnflag', 'l_linestatus']
        # OUTPUT_COLS = ['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']

        subprocess.run([
            'python3.11', 'pac-duckdb-step2-caller.py', 
            '-e', EXPERIMENT,
            '-mi', '0.25',
        ])

    except Exception as e:
        print(f"Error running query {query}: {e}")
        continue
