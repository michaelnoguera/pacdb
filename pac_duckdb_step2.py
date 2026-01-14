import argparse
import json
import logging
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd
import polars as pl

from timer import Timer

# Default max mutual information bound
DEFAULT_MI = 1/2
NUM_TRIALS = 1000
NULL_VAL = pl.Null() # Use polars null as sentinel value for null categories


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


def add_noise_numeric(values, mi):

    # Compute per-coordinate noise scale: variance / (2 * mi)
    arr_2d = np.stack([np.atleast_1d(v) for v in values], axis=-1)
    variances = np.var(arr_2d, axis=1)
    scale = variances / (2 * mi)
    assert len(scale) == 1
    assert np.nan not in values
    scale = scale[0]

    # logging.debug("Stacked array shape: %s", arr_2d.shape)
    # logging.debug("Calculated variances: %s", variances)
    # logging.debug("Noise scale per coordinate (variance/(2*%s)): %s", mi, scale)
    # logging.debug(
    #     "Numeric type detected. Processing %d numeric samples.", len(values)
    # )
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

        # logging.debug(
        #     "Selected sample: %s; noise: %s; release: %s",
        #     sample, noise, release
        # )
    return scale, releases

def add_noise_categorical(values, mi):
    # all None, NULL_VAL, and NaN values are treated as a single null category
    mask_null = [
        (v is None) or (v == NULL_VAL) or (isinstance(v, (float, np.floating)) and np.isnan(v))
        for v in values
    ]

    modified = np.where(mask_null, pd.NA, values)  # use np.nan to work with pd.factorize
    encoded, categories  = pd.factorize(modified, use_na_sentinel=True)

    # Convert back from np.nan to NULL_VAL sentinel (because nan is unequal to itself but the sentinel is)
    if any(mask_null):
        modified = np.where(mask_null, pl.Null(), values)
        categories = np.append(categories, pl.Null())  # ensure NULL_VAL is the last category

    one_hot_encodings = np.eye(len(categories))[encoded] # everything is a [1, 0, 0,...] vector now, 2d numpy array

    dims = one_hot_encodings.shape[1]
    variances_per_dim = np.var(one_hot_encodings, axis=0) # 1d numpy array of variances
    assert dims == len(variances_per_dim)

    sqrt_total_var = sum([variances_per_dim[x]**0.5 for x in range(len(variances_per_dim))])
    per_dim_scale = [1./(2*mi) * variances_per_dim[ind]**0.5 * sqrt_total_var for ind in range(dims)]

    """
    This is the unvectorized version, kept for reference
    releases = []
    for _ in range(NUM_TRIALS):
        sample_idx = np.random.choice(encoded) # categories[idx] gives the randomly chosen category

        # create one-hot representation
        one_hot_rep = np.zeros(len(categories))
        one_hot_rep[sample_idx] = 1  # set the hot component to 1
        
        one_hot_rep += np.random.normal(loc=0, scale=np.sqrt(per_dim_scale))  # add noise to each dimension

        # the dimension with the highest value is the category to be released
        release_cat_idx = np.argmax(one_hot_rep)
        release = categories[release_cat_idx]

        releases.append(None if release == NULL_VAL else release)
    """

    ### Vectorized categorical noise addition and release

    # sample_indices contains NUM_TRIALS samples. Each idx in sample_indices corresponds to a chosen category categories[idx]
    sample_indices = np.random.choice(encoded, size=NUM_TRIALS)

    # Instead of adding noise to existing one-hot vectors, we add the one-hot to existing noise vectors. This is
    # faster because it can be done all at once. See reference implementation above for explanation.
    std_devs = np.sqrt(per_dim_scale)
    num_categories = len(categories)
    noise = np.random.normal(loc=0, scale=std_devs, size=(NUM_TRIALS, num_categories)) # noise per sample
    noise[np.arange(NUM_TRIALS), sample_indices] += 1  # the "one-hot" part per sample

    release_cat_indices = np.argmax(noise, axis=1)  # index of max per row
    releases = np.array(categories)[release_cat_indices]
    releases = np.where(releases == NULL_VAL, None, releases)
    releases = releases.tolist()

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
    raw_values: list = entry.get("values", [])
    sample_size = entry.get("samples", 0)
    null_nan_present = entry.get("null_nan_present", False)

    # Begin by assuming the data is numeric, which we might disqualify later
    is_numeric = True

    values = []
    if (null_nan_present or len(raw_values) < sample_size) or (None in raw_values) or any(
        isinstance(x, (float, np.floating)) and np.isnan(x)
        for x in raw_values
    ):
        #logging.info("Detected nulls or insufficient samples; treating data as categorical.")
        #print(raw_values, null_nan_present, len(raw_values), sample_size)
        values = raw_values
        values.extend([NULL_VAL]*(sample_size - len(raw_values)))
        #print(values)
        is_numeric = False  # presence of nulls forces categorical treatment
        assert values is not None
    else:
        try:
            #logging.info("Attempting to cast values to Polars Series with dtype %s", dtype_str)
            series = pl.Series("v", raw_values).cast(dtype=eval(f"pl.{dtype_str}"), strict=True)
            if series.dtype.is_decimal():
                series = series.cast(pl.Float64)
            values = series.to_numpy()
            is_numeric = series.dtype.is_numeric()
            assert values is not None
        except Exception:
            logging.warning("Polars cast failed. Attempting numpy conversion.")
            values = np.array(raw_values)
            is_numeric = values.dtype.kind in 'biufc'
            assert values is not None
        if is_numeric:
            assert all(not np.isnan(k) for k in values) # can't add noise to a NaN so this is an error case

    timer.end()

    timer.start("compute_variance_and_release")
    if is_numeric:
        logging.info("Treating as numeric.")
        scale, releases = add_noise_numeric(values, mi)
    else:
        logging.info("Treating as categorical.")
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
