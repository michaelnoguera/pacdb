# for all files in ./outputs/pac-duckdb-q1/{n}.json
# do `python3.11 pac_e2e.py -mi 0.125 -o ./outputs/e2e-q1/n.json ./outputs/pac-duckdb-q1/json/{n}.json`

import os
import subprocess
import parse
import zipfile

input_dir = './outputs/pac-duckdb-q1/json/'
output_dir = './outputs/e2e-q1/'
zip_output = './outputs/e2e-q1.zip'

pattern = parse.compile("{n}.json")

os.makedirs(output_dir, exist_ok=True)

for filename in os.listdir(input_dir):
    result = pattern.parse(filename)
    if result:
        n = result['n']
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, filename)

        cmd = [
            'python3.11', 'pac_e2e.py',
            '-mi', '0.125',
            '-o', output_path,
            input_path
        ]

        print(f'Running: {" ".join(cmd)}')
        subprocess.run(cmd)

# Zip the output directory
print(f'Zipping {output_dir} into {zip_output}')
with zipfile.ZipFile(zip_output, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, _, files in os.walk(output_dir):
        for file in files:
            filepath = os.path.join(root, file)
            arcname = os.path.relpath(filepath, start=output_dir)
            zipf.write(filepath, arcname)
