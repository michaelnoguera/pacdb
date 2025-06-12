import os

import papermill as pm
import parse
import time
import datetime

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
        QUERY = query
        QUERYFILE = f"./{QUERYFOLDER}/{QUERY}.sql"

        timer = Timer(experiment=f"ap-duckdb-{QUERY}-total", step="step1", output_dir="./times")
        timer.start("s1_parse_query_file")
        # In the queryfile, there are "--begin SAMPLE_STEP--" and "--end SAMPLE_STEP--" comments delimiting the SAMPLE_STEP and the PREPARE_STEP.
        # Extract the SQL code between these comments into variables SAMPLE_STEP and PREPARE_STEP respectively.
        query_strings = {}
        with open(QUERYFILE, "r") as f:
            lines = f.readlines()
            start = lines.index("--begin SAMPLE_STEP--\n") + 1
            end = lines.index("--end SAMPLE_STEP--\n")
            query_strings["sample"] = "".join(lines[start:end]).strip()
            start = lines.index("--begin PREPARE_STEP--\n") + 1
            end = lines.index("--end PREPARE_STEP--\n")
            query_strings["prepare"] = "".join(lines[start:end]).strip()

        # There are also --var:SAMPLES=2048\n style comments in the queryfile. Each of these defines a variable
        pattern = parse.compile("--var:{name:^}={value:^}")
        for line in lines:
            result = pattern.parse(line)
            if result:
                print(f"Found variable: {result['name']} = {result['value'].strip()}")
                query_strings[result["name"]] = eval(result["value"].strip())

        SAMPLES = int(query_strings["SAMPLES"]) or 1024

        # Now, find every occurence of `1024//2` and replace it with f`{SAMPLES//2}` in those two strings
        query_strings["sample"] = query_strings["sample"].replace("1024//2", f"{SAMPLES // 2}")
        query_strings["prepare"] = query_strings["prepare"].replace("1024//2", f"{SAMPLES // 2}")

        timer.end()  # End the timer for parsing the query
        timer.start("s1_run_notebook")
        EXPERIMENT = f"ap-duckdb-{QUERY}"
        OUTPUT_DIR = f"./outputs/{EXPERIMENT}-step1"

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # INDEX_COLS = ['l_returnflag', 'l_linestatus']
        # OUTPUT_COLS = ['sum_qty', 'sum_base_price', 'sum_disc_price', 'sum_charge', 'avg_qty', 'avg_price', 'avg_disc', 'count_order']

        # run the notebook with these parameters
        pm.execute_notebook(
            "autopac-duckdb-step1.ipynb",
            f"./{EXPERIMENT}-step1.ipynb",
            parameters=dict(
                EXPERIMENT=EXPERIMENT,
                OUTPUT_DIR=OUTPUT_DIR,
                SAMPLES=SAMPLES,
                SAMPLE_STEP=query_strings["sample"],
                PREPARE_STEP=query_strings["prepare"],
                INDEX_COLS=query_strings["INDEX_COLS"],
                OUTPUT_COLS=query_strings["OUTPUT_COLS"],
            ),
        )
        timer.end()
    except Exception as e:
        print(f"Error running query {query}: {e}")
        continue
