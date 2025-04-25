#!./venv/bin/python
# -*- coding: utf-8 -*-

import argparse
import json
import sys
from pathlib import Path
import numpy as np
from typing import List, Any
import polars as pl

### Compute PAC Noise
def get_pac_noise_scale(out_np_raw: np.ndarray, max_mi: float = 1./4) -> np.ndarray:
    out_np = out_np_raw.copy()
    print(f"out_np.shape: {out_np.shape}")

    out_np_2darr = [np.atleast_1d(o) for o in out_np] # make sure all the DF -> np.ndarray conversions result in 2d arrays
    est_y: np.ndarray = np.stack(out_np_2darr, axis=-1)
    print(f"est_y.shape: {est_y.shape}")
    print(f"est_y: {est_y}")

    # get the scale in each basis direction
    fin_var: np.ndarray = np.var(est_y, axis=1)  # shape (dimensions,)
    print(f"fin_var: {fin_var}")
    # fin_var: np.ndarray = np.array([float(x) for x in fin_var], dtype=np.float64)
    sqrt_total_var: np.floating[Any] = np.sum(np.sqrt(fin_var))
    print(f"sqrt_total_var: {sqrt_total_var}")

    pac_noise: np.ndarray = (1./(2*max_mi)) * fin_var
    print(f"For mi={max_mi}, we should add noise from a normal distribution with scale: pac_noise: {pac_noise}")
    return pac_noise


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process input arguments.")
    parser.add_argument("-mi", type=float, required=False, help="MI value")
    parser.add_argument("json_file", type=str, help="Path to the JSON file")
    parser.add_argument("-o", "--output-file", type=str, help="Output file path")
    args = parser.parse_args()

    # Read JSON file
    path = Path(args.json_file)
    if not path.exists():
        print(f"Error: File '{args.json_file}' does not exist.")
        sys.exit(1)

    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    # Extract single entry
    entry = data[0]
    out_np = np.array(entry["values"], dtype=float)

    print("Extracted Keys and Values:")
    print(f"col: {entry['col']}")
    print(f"row: {entry['row']}")
    print(f"values: {out_np}")

    MI_OPTIONS = [0.001248318631131131, 1/64, 1/32, 1/16, 1/4, 1., 2., 4., 16.]
    mi = args.mi or 1/4
    
    SAMPLES = len(out_np)

    scale = get_pac_noise_scale(out_np, mi) # estimate the stability of the query
    print(f"mi={mi}, scale={scale}")
    
    # for each PAC release at this MI, we will choose a sample from the pre-generated out_np list and add noise to it
    steps = {
        "mi": mi,
        "scale": scale,
    }

    # choose our sample
    chosen_index = np.random.choice(range(SAMPLES))
    chosen_sample = out_np[chosen_index].copy()
    steps["chosen_sample"] = chosen_sample
    
    # add noise to it
    # chosen_noise will also be an array
    
    chosen_noise = np.random.normal(loc=0, scale=np.sqrt(scale))
    steps["chosen_noise"] = chosen_noise
    
    print(f'Chosen Sample {chosen_sample}')
    # chosen_sample = np.array([float(x) for x in chosen_sample], dtype=np.float64)
    release = chosen_sample + chosen_noise # do_pac_and_release(out_np, mi, scale, chosen_index)

    print(f"sample(#{chosen_index}):{chosen_sample} + noise:{chosen_noise} = {release}")
    steps["release"] = release
    #release[0] *= 2   # manually correct count = count * 2

    print(steps)

    # Save a json file containing 'col', 'row', and 'value'
    j = {
        "col": entry["col"],
        "row": entry["row"],
        "dtype": entry["dtype"],
        "value": release.tolist(),
    }

    if args.output_file:
        output_file = Path(args.output_file)
        with output_file.open("w", encoding="utf-8") as file:
            json.dump(j, file, indent=4)
