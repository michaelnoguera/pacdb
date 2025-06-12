import os
import pickle

import papermill as pm
import parse

from timer import Timer

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

        timer = Timer(experiment=f"{EXPERIMENT}-total", step="step3", output_dir="./times")
        timer.start("s3_run_notebook")
        # run the notebook with these parameters
        pm.execute_notebook(
            "autopac-duckdb-step3.ipynb",
            f"./{EXPERIMENT}-step3.ipynb",
            parameters=dict(
                EXPERIMENT=EXPERIMENT,
                OUTPUT_DIR=OUTPUT_DIR,
                INPUT_ZIP=f"./outputs/{EXPERIMENT}-step2.zip",
                INDEX_COLS=INDEX_COLS,
                OUTPUT_COLS=OUTPUT_COLS,
                templatedf_path=templatedf_path
            ),
        )
        timer.end()
    except Exception as e:
        print(f"Error running query {query}: {e}")
        continue
