# for all files in ./outputs/{EXPERIMENT}-step1/json/{n}.json
# run the script pac_duckdb_step2.py

"""
Usage:
python3.11 pac_duckdb_step2_caller.py -e pac-duckdb-q1 -mi 0.125
"""


import argparse
import logging
import os
import shutil

import parse

from pac_duckdb_step2 import add_pac_noise_to_sample
from timer import Timer


def run_batch_pac_noise_step2(
    experiment: str,
    max_mi: float = 0.25,
    verbose: bool = False,
):
    input_dir = f'./outputs/{experiment}-step1/json'
    output_dir = f'./outputs/{experiment}-step2'
    os.makedirs(output_dir, exist_ok=True)

    pattern = parse.compile("{n}.json")
    for filename in os.listdir(input_dir):
        result = pattern.parse(filename)
        if result:
            n = result['n']
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_dir, f"{n}.json")

            logging.info(f"Processing {input_file} -> {output_file}")
            add_pac_noise_to_sample(
                input_path=input_file,
                max_mi=max_mi,
                verbose=verbose,
                output_path=output_file,
                experiment=experiment,
                step="step2"
            )

    # Zip the output directory
    timer = Timer(experiment=experiment, step='step2', output_dir="./times")
    timer.start("zip_output")
    logging.info(f'Zipping {output_dir}.')
    shutil.make_archive(output_dir, 'zip', output_dir)
    timer.end()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--experiment", type=str, required=True, help="Experiment name")
    parser.add_argument("-mi", "--mi", type=float, required=False, default=0.25, help="MI value (default: 0.25)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
    )

    run_batch_pac_noise_step2(
        experiment=args.experiment,
        max_mi=args.mi,
        verbose=args.verbose
    )
