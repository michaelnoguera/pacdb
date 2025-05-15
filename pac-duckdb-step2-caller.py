# for all files in ./outputs/{EXPERIMENT}-step1/json/{n}.json
# run the script pac-duckdb-step2.py

"""
Usage:
python3.11 pac-duckdb-step2-caller.py -e pac-duckdb-q1 -mi 0.125
"""


import argparse
import logging
import os
import shutil
import subprocess

import parse

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--experiment", type=str, required=True, help="Experiment name")
    parser.add_argument("-mi", "--mi", type=float, required=False, help="MI value")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()

    # Configure logging level
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
    )

    mi: float = args.mi or 1/4

    EXPERIMENT = args.experiment
    INPUT_DIR = f'./outputs/{EXPERIMENT}-step1/json'
    OUTPUT_DIR = f'./outputs/{EXPERIMENT}-step2'

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    pattern = parse.compile("{n}.json")
    for filename in os.listdir(INPUT_DIR):
        result = pattern.parse(filename)
        if result:
            n = result['n']

            cmd = [
                'python3.11', 'pac-duckdb-step2.py',
                '-mi', str(mi),
                '-o', os.path.join(OUTPUT_DIR, f"{n}.json"),
            ]
            if args.verbose:
                cmd.append('-v')
            cmd.append(os.path.join(INPUT_DIR, f"{n}.json"))

            logging.info(f'Running "{" ".join(cmd)}"')
            subprocess.run(cmd)

    # Zip the output directory
    logging.info(f'Zipping {OUTPUT_DIR}.')
    shutil.make_archive(OUTPUT_DIR, 'zip', OUTPUT_DIR)