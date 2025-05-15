import argparse
import logging
import os
import subprocess

import parse

QUERYFOLDER = "./queries"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process input arguments.")
    parser.add_argument("-mi", "--mi", type=float, required=False, help="MI value")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

        # Configure logging level
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
    )

    mi: float = args.mi or 1/4

    queries_to_run = []  # get filenames matching ./queries/{query}.sql
    pattern = parse.compile("{q}.sql")
    for queryfile in os.listdir(QUERYFOLDER):
        result = pattern.parse(queryfile)
        if result:
            query = result["q"]
            queries_to_run.append(query)

    for query in queries_to_run:
        try:
            logging.info(f"Running query: {query}")

            EXPERIMENT = f"ap-duckdb-{query}"
            
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

        except Exception as e:
            print(f"Error running query {query}: {e}")
            continue
