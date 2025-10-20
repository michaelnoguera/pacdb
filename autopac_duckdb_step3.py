import os
import pickle
from contextlib import redirect_stdout

import parse

from autopac_duckdb_step3_flat import run_step_3
from timer import Timer

QUERYFOLDER = "./queries"
STEP_2_RESULT_FOLDER = "./outputs/"

def main():
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

    print(f"Queries detected: {queries_to_run}")

    for query, mi in queries_to_run:
        print(f"Assembling table for: {query} w/ MI: {mi}")
        
        EXPERIMENT = f"ap-duckdb-{query}"
        OUTPUT_DIR = f"./outputs/{EXPERIMENT}-{mi}-step3"

        INPUT_DIR = f"./outputs/{EXPERIMENT}-{mi}-step2"

        INDEX_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/INDEX_COLS.pkl', 'rb'))
        OUTPUT_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/OUTPUT_COLS.pkl', 'rb'))
        templatedf_path = f'./outputs/{EXPERIMENT}-step1/template.pkl'

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        timer = Timer(experiment=f"{EXPERIMENT}-total", step="step3", output_dir="./times")
        timer.start(f"s3_run_function {mi}")

        # run the function with these parameters
        output_file = os.path.join(OUTPUT_DIR, "step3_stdout.txt")
        with open(output_file, "w") as f, redirect_stdout(f):
            run_step_3(
                experiment=EXPERIMENT,
                input_dir=INPUT_DIR,
                output_dir=OUTPUT_DIR,
                index_cols=INDEX_COLS,
                output_cols=OUTPUT_COLS,
                templatedf_path=templatedf_path
            )

        timer.end()

if __name__ == "__main__":
    main()