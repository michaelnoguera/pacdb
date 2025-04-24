# for all files in ./outputs/pac-duckdb-q1/{n}.json
# do `python3.11 pac_e2e.py -mi 0.125 -o ./outputs/e2e-q1/n.json ./outputs/pac-duckdb-q1/json/{n}.json`

import os
import subprocess
import parse
import zipfile
import shutil

EXPERIMENT = 'pac-duckdb-q1'
INPUT_DIR = f'./outputs/{EXPERIMENT}-step1/json'
OUTPUT_DIR = f'./outputs/{EXPERIMENT}-step2'

pattern = parse.compile("{n}.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

for filename in os.listdir(INPUT_DIR):
    result = pattern.parse(filename)
    if result:
        n = result['n']
        input_path = os.path.join(INPUT_DIR, filename)
        output_path = os.path.join(OUTPUT_DIR, filename)

        cmd = [
            'python3.11', 'pac_e2e.py',
            '-mi', '0.125',
            '-o', output_path,
            input_path
        ]

        print(f'Running: {" ".join(cmd)}')
        subprocess.run(cmd)

# Zip the output directory
print(f'Zipping {OUTPUT_DIR}.')
shutil.make_archive(OUTPUT_DIR, 'zip', OUTPUT_DIR)