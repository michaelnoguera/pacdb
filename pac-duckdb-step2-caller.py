# for all files in ./outputs/{EXPERIMENT}-step1/json/{n}.json
# run the script pac-duckdb-step2.py

"""
Usage:
python3.11 pac-duckdb-step2-caller.py -e pac-duckdb-q1 -mi 0.125
"""


import os
import subprocess
import parse
import zipfile
import shutil
import argparse

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process input arguments.")
    parser.add_argument("-e", "--experiment", type=str, required=True, help="Experiment name")
    parser.add_argument("-mi", "--mi", type=float, required=False, help="MI value")
    args = parser.parse_args()

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
            input_path = os.path.join(INPUT_DIR, filename)
            output_path = os.path.join(OUTPUT_DIR, filename)

            cmd = [
                'python3.11', 'pac-duckdb-step2.py',
                '-mi', str(mi),
                '-o', output_path,
                input_path
            ]

            print(f'Running: {" ".join(cmd)}')
            subprocess.run(cmd)

    # Zip the output directory
    print(f'Zipping {OUTPUT_DIR}.')
    shutil.make_archive(OUTPUT_DIR, 'zip', OUTPUT_DIR)