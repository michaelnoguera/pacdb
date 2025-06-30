import os
import pickle

import papermill as pm
import parse

from timer import Timer

QUERYFOLDER = "./queries"
STEP_2_RESULT_FOLDER = "./outputs/"

# Execute all of ./queries/{query}.sql
# for all MI values that have a ./outputs/ap-duckdb-{query}-{mi}-step2 folder
pattern = parse.compile("{q}.sql")
query_prefixes = []
for queryfile in os.listdir(QUERYFOLDER):
    result = pattern.parse(queryfile)
    if result:
        query = result["q"]
        query_prefixes.append(query)

queries_to_run = []
for query in query_prefixes:
    for foldername in os.listdir(STEP_2_RESULT_FOLDER):
        if foldername.startswith(f"ap-duckdb-{query}"):
            result = parse.compile("ap-duckdb-"+query+"-{mi}-step2").parse(foldername)
            if result:
                mi = result["mi"]
                queries_to_run.append((query, mi))

for query, mi in queries_to_run:
    print(f"Assembling table for: {query} w/ MI: {mi}")
    
    EXPERIMENT = f"ap-duckdb-{query}"
    OUTPUT_DIR = f"./outputs/{EXPERIMENT}-{mi}-step3"

    INPUT_ZIP = f"./outputs/{EXPERIMENT}-{mi}-step2.zip"

    INDEX_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/INDEX_COLS.pkl', 'rb'))
    OUTPUT_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/OUTPUT_COLS.pkl', 'rb'))
    templatedf_path = f'./outputs/{EXPERIMENT}-step1/template.pkl'

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timer = Timer(experiment=f"{EXPERIMENT}-total", step="step3", output_dir="./times")
    timer.start(f"s3_run_notebook {mi}")
    # run the notebook with these parameters
    pm.execute_notebook(
        "autopac-duckdb-step3.ipynb",
        f"./{EXPERIMENT}-{mi}-step3.ipynb",
        parameters=dict(
            EXPERIMENT=EXPERIMENT,
            OUTPUT_DIR=OUTPUT_DIR,
            INPUT_ZIP=INPUT_ZIP,
            INDEX_COLS=INDEX_COLS,
            OUTPUT_COLS=OUTPUT_COLS,
            templatedf_path=templatedf_path
        ),
    )
    timer.end()
