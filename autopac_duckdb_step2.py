import logging
import os

import parse

from pac_duckdb_step2_caller import run_batch_pac_noise_step2
from timer import Timer

QUERYFOLDER = "./queries"

def main(mi: float, verbose: bool):
    # runs pac_duckdb_step2_caller which runs pac_duckdb_step2.py for each output cell of each query in ./queries/{query}.sql
    # if multiple MI values are provided on the command line, this function will be invoked multiple times, once for each MI value
    queries_to_run = []
    pattern = parse.compile("{q}.sql")
    for queryfile in os.listdir(QUERYFOLDER):
        result = pattern.parse(queryfile)
        if result:
            query = result["q"]
            queries_to_run.append(query)
    print(f"Queries detected: {queries_to_run}")

    for query in queries_to_run:
        print(f"Computing PAC noise for: {query} w/ MI: {mi}")

        EXPERIMENT = f"ap-duckdb-{query}"

        timer = Timer(experiment=f"{EXPERIMENT}-total", step="step2", output_dir="./times")
        timer.start(f"s2_run_batch {mi}")

        os.makedirs(f"./outputs/{EXPERIMENT}-{mi}-step2", exist_ok=True)

        run_batch_pac_noise_step2(
            experiment=EXPERIMENT,
            max_mi=mi,
            verbose=verbose
        )

        timer.end()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-mi", "--mi", type=float, action="append", help="MI value(s) (default: 0.5)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
    )
    
    if not args.mi: # default = 0.5
        args.mi = [0.5]

    for mi in args.mi:
        logging.info(f"Running with MI value: {mi}")
        main(mi=mi, verbose=args.verbose)
