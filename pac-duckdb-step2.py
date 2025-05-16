import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import polars as pl

# Default max mutual information bound
DEFAULT_MI = 1/4

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Add PAC noise to a sample from input JSON values.")
    parser.add_argument("-mi", "--max-mi", type=float, default=DEFAULT_MI)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("input_file", type=Path)
    parser.add_argument("-o", "--output-file", type=Path)
    args = parser.parse_args()
   
    # Configure logging level
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
    )

    mi = args.max_mi
    input_path = args.input_file

    # Validate input file exists
    if not input_path.exists():
        logging.error("Input file '%s' does not exist.", input_path)
        sys.exit(1)

    # Load and parse JSON entry
    try:
        with input_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        entry = data[0]
    except (json.JSONDecodeError, IndexError) as e:
        logging.error("Failed to read or parse '%s': %s", input_path, e)
        sys.exit(1)

    # Read values and dtype from JSON
    dtype_str = entry.get("dtype", "")
    raw_values = entry.get("values", [])

    sample_size = entry.get("samples", 0)
    if len(raw_values) < sample_size:
        logging.info("Sample size (%d) is larger than the number of values (%d).", sample_size, len(raw_values))

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
        logging.warning(
            "Failed to cast values to Polars Series with dtype '%s'. Attempting numpy conversion.",
            dtype_str
        )

        values = np.array(raw_values)

        if values.dtype.kind in 'biufc':  # Check if dtype is numeric (int, float, complex)
            is_numeric = True

    if is_numeric:
        # Compute per-coordinate noise scale: variance / (2 * mi)
        arr_2d = np.stack([np.atleast_1d(v) for v in values], axis=-1)
        variances = np.var(arr_2d, axis=1)
        scale = variances / (2 * mi)

        logging.info("Stacked array shape: %s", arr_2d.shape)
        logging.info("Calculated variances: %s", variances)
        logging.info("Noise scale per coordinate (variance/(2*%s)): %s", mi, scale)
        logging.info(
            "Numeric type '%s' detected. Processing %d numeric samples.",
            dtype_str, len(values)
        )
    else:
        # Nothing to do for non-numeric types
        pass

        logging.info(
            "Non-numeric type '%s' detected. Processing %d categorical values.",
            dtype_str, len(values)
        )

    # Choose a sample at random
    sample = np.random.choice(values)

    # Compute noise for numeric types, none otherwise
    if is_numeric:
        # Ensure scale is a valid float or array of floats
        if scale is None or np.any(np.isnan(scale)):
            logging.error("Noise scale is invalid (None or NaN).")
            sys.exit(1)
        noise = np.random.normal(loc=0, scale=np.sqrt(scale))
        release = sample + noise
    else:
        noise = None
        release = sample

    logging.info(
        "Selected sample: %s; noise: %s; release: %s",
        sample, noise, release
    )

    class CustomEncoder(json.JSONEncoder):
        def default(self, obj):
            # in order to put arbitrary values back into JSON, convert them to strings if needed
            try:
                return super().default(obj)
            except TypeError:
                return str(obj)

    # Prepare output JSON
    output = {
        "col": entry.get("col"),
        "row": entry.get("row"),
        "dtype": dtype_str,
        "value": release.tolist() if hasattr(release, 'tolist') else release,
    }

    if args.output_file:
        with args.output_file.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=4, cls=CustomEncoder)
    else:
        print(json.dumps(output, indent=4, cls=CustomEncoder))

