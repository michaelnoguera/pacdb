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

    sample_size = entry.get("samples", 0)
    add_noise = True 
    if len(values) < sample_size:
        logging.warn("Sample size (%d) is larger than the number of values (%d).", sample_size, len(values))
        # if len(raw_values) < sample_size/2:
        add_noise = False # always return None

    releases = []
    scale = None

    timer.end()

    timer.start("compute_variance_and_release")
    frac_nulls = 0
    if not add_noise:
        frac_nulls = NUM_TRIALS
    elif is_numeric:
        
        # Compute per-coordinate noise scale: variance / (2 * mi)
        arr_2d = np.stack([np.atleast_1d(v) for v in values], axis=-1)
        variances = np.var(arr_2d, axis=1)
        if np.isnan(variances):
            variances = np.nanvar(arr_2d, axis=1)
            logging.warn("Output query is sometimes NaN!")
        scale = variances / (2 * mi)
        assert len(scale) == 1
        scale = scale[0]

        logging.info("Stacked array shape: %s", arr_2d.shape)
        logging.info("Calculated variances: %s", variances)
        logging.info("Noise scale per coordinate (variance/(2*%s)): %s", mi, scale)
        logging.info(
            "Numeric type '%s' detected. Processing %d numeric samples.",
            dtype_str, len(values)
        )

        for _ in range(NUM_TRIALS):


            # Choose a sample at random
            frac_samples = len(values) / sample_size
            if frac_samples != 1:
                assert(False)
            logging.info(f'frac_samples: {frac_samples}')
            if frac_samples > 1:
                assert(False)
            if np.random.rand() < frac_samples:
                sample = np.random.choice(values)
            else:
                sample = np.nan


            if add_noise and not np.isnan(sample):
                # Compute noise for numeric types
                # Ensure scale is a valid float or array of floats
                if scale is None or np.any(np.isnan(scale)):
                    raise ValueError("Noise scale is invalid (None or NaN).")
                noise = np.random.normal(loc=0, scale=np.sqrt(scale))
                release = sample + noise
                releases.append(release)
            else:
                if np.isnan(sample):
                    sample = None
                    frac_nulls += 1
                release = None
                noise = None

            logging.info(
                "Selected sample: %s; noise: %s; release: %s",
                sample, noise, release
            )
    else:
        noise = 'uniform'
        unique_values = list(set(values))
        logging.info("Num unique values: %s", len(unique_values))
        for _ in range(NUM_TRIALS):
            # Choose a sample at random
            frac_samples = len(values) / sample_size
            if frac_samples != 1:
                assert(False)
            logging.info(f'frac_samples: {frac_samples}')
            if np.random.rand() < frac_samples:
                sample = np.random.choice(values)
            else:
                sample = None

            if sample is not None:
                release = np.random.choice(unique_values)
                releases.append(release)
            else:
                frac_nulls += 1
                release = None

            logging.info(
                "Selected sample: %s; noise: %s; release: %s",
                sample, noise, release
            )
    timer.end()

    timer.start("write_json")
    output = {
        "col": entry.get("col"),
        "row": entry.get("row"),
        "scale": scale,
        "dtype": dtype_str,
        "value": releases if len(releases) > 0 else [None],
        "frac_nulls": frac_nulls / NUM_TRIALS
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
