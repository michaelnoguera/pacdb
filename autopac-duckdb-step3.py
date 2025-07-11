import os
import pickle
from contextlib import redirect_stdout


import nbconvert
import nbformat
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

    INPUT_DIR = f"./outputs/{EXPERIMENT}-{mi}-step2"

    INDEX_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/INDEX_COLS.pkl', 'rb'))
    OUTPUT_COLS = pickle.load(open(f'./outputs/{EXPERIMENT}-step1/OUTPUT_COLS.pkl', 'rb'))
    templatedf_path = f'./outputs/{EXPERIMENT}-step1/template.pkl'

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    timer = Timer(experiment=f"{EXPERIMENT}-total", step="step3", output_dir="./times")
    timer.start(f"s3_run_notebook {mi}")

    # run the notebook with these parameters
    notebook_path = f"./{EXPERIMENT}-{mi}-step3.ipynb"
    script_path = notebook_path.replace(".ipynb", ".py")

    pm.execute_notebook(
        "autopac-duckdb-step3.ipynb",
        f"./{EXPERIMENT}-{mi}-step3.ipynb",
        prepare_only=True,
        parameters=dict(
            EXPERIMENT=EXPERIMENT,
            OUTPUT_DIR=OUTPUT_DIR,
            INPUT_DIR=INPUT_DIR,
            INDEX_COLS=INDEX_COLS,
            OUTPUT_COLS=OUTPUT_COLS,
            templatedf_path=templatedf_path
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

    # Run the script
    output_file = os.path.join(OUTPUT_DIR, "step3_stdout.txt")
    with open(output_file, "w") as f, redirect_stdout(f):
        exec(script_body)
    timer.end()
