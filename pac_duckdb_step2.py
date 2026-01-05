import argparse
import json
import logging
from pathlib import Path
from typing import Optional, Union

import numpy as np
import polars as pl

from timer import Timer

# Default max mutual information bound
DEFAULT_MI = 1/2
NUM_TRIALS = 1000

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

def nan_check(values):
    return any(
        isinstance(x, (float, np.floating)) and np.isnan(x)
        for x in values
    )

def add_noise_numeric(values, mi):

    # Compute per-coordinate noise scale: variance / (2 * mi)
    arr_2d = np.stack([np.atleast_1d(v) for v in values], axis=-1)
    variances = np.var(arr_2d, axis=1)
    scale = variances / (2 * mi)
    assert len(scale) == 1
    assert np.nan not in values
    scale = scale[0]

    logging.debug("Stacked array shape: %s", arr_2d.shape)
    logging.debug("Calculated variances: %s", variances)
    logging.debug("Noise scale per coordinate (variance/(2*%s)): %s", mi, scale)
    logging.debug(
        "Numeric type detected. Processing %d numeric samples.", len(values)
    )
    releases = []
    for _ in range(NUM_TRIALS):
        sample = np.random.choice(values)
        # Compute noise for numeric types
        # Ensure scale is a valid float or array of floats
        if scale is None or np.any(np.isnan(scale)):
            raise ValueError("Noise scale is invalid (None or NaN).")
        noise = np.random.normal(loc=0, scale=np.sqrt(scale))
        release = sample + noise
        releases.append(release)

        logging.debug(
            "Selected sample: %s; noise: %s; release: %s",
            sample, noise, release
        )
    return scale, releases

def add_noise_categorical(values, mi):
    null_val = 'null'
    modified = []
    for v in values:
        if v is None or isinstance(v, (float, np.floating)) and np.isnan(v):
            modified.append(null_val)
        else:
            modified.append(v)
    categories, encoded = np.unique(modified, return_inverse=True)
    cat_to_idx = {cat: i for i, cat in enumerate(categories)}
    idx_to_cat = {i: cat for i, cat in enumerate(categories)}
    one_hot_encodings = np.eye(len(categories))[encoded]
    dims = one_hot_encodings.shape[1]
    variances_per_dim = np.var(one_hot_encodings, axis=0)
    sqrt_total_var = sum([variances_per_dim[x]**0.5 for x in range(len(variances_per_dim))])
    per_dim_scale = [1./(2*mi) * variances_per_dim[ind]**0.5 * sqrt_total_var for ind in range(dims)]
    releases = []
    for _ in range(NUM_TRIALS):
        sample = np.random.choice(modified)
        one_hot_rep = np.zeros(len(cat_to_idx))
        one_hot_rep[cat_to_idx[sample]] = 1
        for dim_ind in range(len(one_hot_rep)):
            one_hot_rep[dim_ind] += np.random.normal(loc=0, scale = np.sqrt(per_dim_scale[dim_ind]))
        release = idx_to_cat[np.argmax(one_hot_rep)]
        releases.append(release)
    return per_dim_scale, releases

def add_pac_noise_to_sample(
    input_path: Union[str, Path],
    max_mi: float = DEFAULT_MI,
    verbose: bool = False,
    output_path: Optional[Union[str, Path]] = None,
    experiment: str = "unknown_experiment",
    step: str = "step2",
) -> dict:
    # Configure logging level
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
    )

    mi = max_mi
    input_path = Path(input_path)

    # Validate input file exists
    if not input_path.exists():
        raise FileNotFoundError(f"Input file '{input_path}' does not exist.")

    # Configure timer
    timer = Timer(experiment=experiment, step=step, output_dir="./times")

    # Load and parse JSON entry
    timer.start("load_json")
    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    entry = data[0]

    # Read values and dtype from JSON
    dtype_str = entry.get("dtype", "")
    raw_values = entry.get("values", [])

    sample_size = entry.get("samples", 0)
    add_noise = True
    if len(raw_values) < sample_size or None in raw_values or nan_check(raw_values):
        logging.warning(
            "For %s %s, sample size (%d) is larger than the number of values (%d).", experiment, input_path.name, sample_size, len(raw_values))
        is_numeric = False
    releases = []
    scale = None
    # Determine if numeric type
    try:
        series = pl.Series("v", raw_values)
        try:
            series = series.cast(eval(f"pl.{dtype_str}"))
        except Exception:
            series = series.cast(pl.Float64)

        is_numeric = series.dtype.is_numeric()

        if series.dtype.is_decimal():
            series = series.cast(pl.Float64)

        values = series.to_numpy()
    except Exception:
        logging.warning("Polars cast failed. Attempting numpy conversion.")
        values = np.array(raw_values)
        is_numeric = values.dtype.kind in 'biufc'
    if is_numeric:
        values = [k for k in values if not np.isnan(k)] # only one output col
        assert(len(values) == len(raw_values)) # can't add noise to a NaN so this is an error case

    timer.end()

    timer.start("compute_variance_and_release")
    if is_numeric:
        scale, releases = add_noise_numeric(values, mi)
    else:
        scale, releases = add_noise_categorical(values, mi)

    timer.end()

    timer.start("write_json")
    output = {
        "col": entry.get("col"),
        "row": entry.get("row"),
        "scale": scale,
        "dtype": dtype_str,
        "value": releases,
    }

    if output_path:
        output_path = Path(output_path)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=4, cls=CustomEncoder)
    timer.end()

    return output

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add PAC noise to a sample from input JSON values.")
    parser.add_argument("-mi", "--max-mi", type=float, default=DEFAULT_MI)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("input_file", type=Path)
    parser.add_argument("-o", "--output-file", type=Path)
    parser.add_argument("--experiment", type=str, default="unknown_experiment")
    parser.add_argument("--step", type=str, default="step2")
    args = parser.parse_args()

    result = add_pac_noise_to_sample(
        input_path=args.input_file,
        max_mi=args.max_mi,
        verbose=args.verbose,
        output_path=args.output_file,
        experiment=args.experiment,
        step=args.step
    )

    if not args.output_file:
        print(json.dumps(result, indent=4, cls=CustomEncoder))
