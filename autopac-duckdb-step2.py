import argparse
import datetime
import os
import subprocess
import time

import parse

from timer import Timer

QUERYFOLDER = "./queries"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-mi", "--mi", type=float, required=False, help="MI value")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    mi: float = args.mi or 1/2

    queries_to_run = []  # get filenames matching ./queries/{query}.sql
    pattern = parse.compile("{q}.sql")
    for queryfile in os.listdir(QUERYFOLDER):
        result = pattern.parse(queryfile)
        if result:
            query = result["q"]
            queries_to_run.append(query)

    for query in queries_to_run:
        try:
            print(f"Running query: {query}")

            EXPERIMENT = f"ap-duckdb-{query}"

            timer = Timer(experiment=f"{EXPERIMENT}-total", step="step2", output_dir="./times")
            timer.start("s2_run_subprocess")
            
            OUTPUT_DIR = f"./outputs/{EXPERIMENT}-step3"
            os.makedirs(OUTPUT_DIR, exist_ok=True)

            cmd = [
                'python3.11', 'pac-duckdb-step2-caller.py', 
                '-e', EXPERIMENT,
                '-mi', str(mi),
            ]
            if args.verbose:
                cmd.append('-v')
            subprocess.run(cmd)
            timer.end()
        except Exception as e:
            print(f"Error running query {query}: {e}")
            continue
