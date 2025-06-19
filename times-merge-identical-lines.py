"""
Post-processing script for CSV files in the `./times` directory.

Merges identical lines in CSV files by summing numeric columns while
keeping non-numeric columns intact.

Used especially because there are so many outputs for step2 because
the inner script runs many times.
"""

import glob

import polars as pl


def merge_identical_lines(file_path):
    df = pl.read_csv(file_path)
    # Identify numeric columns
    numeric_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype.is_numeric()]
    # Identify non-numeric columns
    non_numeric_cols = [col for col in df.columns if col not in numeric_cols]
    # Group by non-numeric columns and sum numeric columns
    merged = df.group_by(non_numeric_cols, maintain_order=True).agg([pl.col(col).sum() for col in numeric_cols])
    # Save merged file (overwrite original)
    merged.write_csv(file_path)

if __name__ == "__main__":
    csv_files = glob.glob("./times/*.csv")
    for csv_file in csv_files:
        merge_identical_lines(csv_file)