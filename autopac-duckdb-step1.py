import os
from contextlib import redirect_stdout

import nbconvert
import nbformat
import papermill as pm
import parse

from timer import Timer

QUERYFOLDER = "./queries"

def main(prepare_only=False):
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
            notebook_path = f"./{EXPERIMENT}-step1.ipynb"
            script_path = notebook_path.replace(".ipynb", ".py")

            # Prepare the notebook with papermill
            pm.execute_notebook(
                "autopac-duckdb-step1.ipynb",
                notebook_path,
                prepare_only=True, # faster to run outside of ipython, so we only generate the notebook
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

            # Convert the notebook to a Python script
            exporter = nbconvert.PythonExporter()
            with open(notebook_path, "r", encoding="utf-8") as nb_file:
                notebook_content = nb_file.read()
                nb_node = nbformat.reads(notebook_content, as_version=4)
                script_body, _ = exporter.from_notebook_node(nb_node)
                with open(script_path, "w", encoding="utf-8") as script_file:
                    script_file.write(script_body)

            if prepare_only:
                print(f"Prepared script for {QUERY} but not running it (prepare-only mode).")
            else:
                # Run the script
                output_file = os.path.join(OUTPUT_DIR, "step1_stdout.txt")
                with open(output_file, "w") as f, redirect_stdout(f):
                    exec(script_body, globals())
            timer.end()

        except Exception as e:
            print(f"Error running query {query}: {e}")
            continue

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare-only", action="store_true", help="Do not run the script, only prepare it. Used for external timing.")
    args = parser.parse_args()
    
    main(prepare_only=args.prepare_only)