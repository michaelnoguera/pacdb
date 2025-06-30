import logging
import os

import parse

from pac_duckdb_step2_caller import run_batch_pac_noise_step2
from timer import Timer

QUERYFOLDER = "./queries"

def main(mi: float, verbose: bool):
    """
    Run PAC noise step2 for all queries in the queries folder.

    Args:
        mi (float): Max mutual information bound.
        verbose (bool): Whether to enable verbose logging.
    """

    queries_to_run = []
    pattern = parse.compile("{q}.sql")
    for queryfile in os.listdir(QUERYFOLDER):
        result = pattern.parse(queryfile)
        if result:
            query = result["q"]
            queries_to_run.append(query)

    for query in queries_to_run:
        print(f"Running query: {query}")

        EXPERIMENT = f"ap-duckdb-{query}"

        timer = Timer(experiment=f"{EXPERIMENT}-total", step="step2", output_dir="./times")
        timer.start("s2_run_batch")

        os.makedirs(f"./outputs/{EXPERIMENT}-step3", exist_ok=True)

        run_batch_pac_noise_step2(
            experiment=EXPERIMENT,
            max_mi=mi,
            verbose=verbose
        )

        timer.end()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-mi", "--mi", type=float, default=0.5, help="MI value (default: 0.5)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
    )
    
    main(mi=args.mi, verbose=args.verbose)
