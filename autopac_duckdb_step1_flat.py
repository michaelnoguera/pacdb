#!/usr/bin/env python
# coding: utf-8

import logging
import os
import pickle
import shutil
import tempfile

import duckdb
import polars as pl

from timer import Timer


def main(
    EXPERIMENT,
    OUTPUT_DIR,
    SAMPLES,
    SAMPLE_STEP,
    PREPARE_STEP,
    INDEX_COLS,
    OUTPUT_COLS,
):
    logger = logging.getLogger()
    logger.info(f"Starting main() for experiment: {EXPERIMENT}")

    # Timer setup
    timer = Timer(experiment=EXPERIMENT, step="step1", output_dir="./times")
    logger.debug("Timer initialized.")

    TRUE_OUTPUT_DIR = OUTPUT_DIR
    OUTPUT_DIR = tempfile.mkdtemp()
    logger.info(f"Using temporary output directory: {OUTPUT_DIR}")

    # Ensure the output directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"Created output directory: {OUTPUT_DIR}")
    else:
        logger.debug(f"Output directory already exists: {OUTPUT_DIR}")



    # duckdb load data/tpch/tpch.duckdb into the temporary in-memory database
    timer.start("load_data")
    logger.info("Connecting to DuckDB database at data/tpch/tpch.duckdb")
    con = duckdb.connect(database='data/tpch/tpch.duckdb', read_only=True)
    logger.info("DuckDB connection established.")
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    #for t in tables:
    #    con.execute(f"CREATE TABLE {t} AS SELECT * FROM 'data/tpch/{t}.parquet'")
    timer.end()
    logger.debug("Data load timer ended.")



    # Construct the table of random samples
    # to use, join it with the lineitem table (for specific sample # s) and filter to just the
    # rows where random_binary = 1.0
    # This will give us a 50% sample of the lineitem table for each sample # s

    assert SAMPLES % 2 == 0, "SAMPLES must be even to create complementary samples."

    timer.start("create_random_samples_table")
    logger.info("Creating random_samples table.")
    con.execute(SAMPLE_STEP)
    logger.info("random_samples table created.")
    timer.end()

    # The randomness of what rows are chosen is saved to disk in `random_binary.json`. For each sample #, there is an array with one entry per row, where 1 means the row was chosen and 0 means it was not.



    try:
        logger.info("Aggregating random_binary column and writing to JSON.")
        con.execute("""
        SELECT sample_id, array_agg(random_binary::TINYINT) as random_binary
        FROM random_samples
        GROUP BY sample_id;
        """).pl().write_json(f"{OUTPUT_DIR}/random_binary.json")
        logger.info(f"random_binary.json written to {OUTPUT_DIR}/random_binary.json")
    except duckdb.BinderException as e:
        logger.warning(f"No random_binary column present. {e}")
        con.execute("""
        SELECT sample_id, array_agg(row_id) as selected_rows
        FROM random_samples
        GROUP BY sample_id;
        """).pl().write_json(f"{OUTPUT_DIR}/selected_rows.json")
        logger.info(f"selected_rows.json written to {OUTPUT_DIR}/selected_rows.json")

    # Query is specified as a prepared statement. We will then execute it once per sample.



    # Query
    timer.start("prepare_query")
    logger.info("Preparing query statement.")
    con.execute(PREPARE_STEP)
    logger.info("Query prepared.")
    timer.end()

    # Run query to see output
    dfs0 = con.execute(f"EXECUTE run_query(sample := {0});").pl()
    logger.info("Executed run_query for sample 0.")

    # Save csv copies of the first 5 samples
    os.makedirs(f"{OUTPUT_DIR}/csv", exist_ok=True)
    logger.info(f"Created CSV output directory: {OUTPUT_DIR}/csv")
    for s in range(5):
        logger.debug(f"Writing CSV for sample {s}")
        con.execute(f"EXECUTE run_query(sample := {s});").pl().write_csv(f"{OUTPUT_DIR}/csv/sample_{s}.csv")
    logger.info("First 5 sample CSVs written.")

    dfs0



    # Run the query for each sample, but accumulate in a pl.DataFrame instead of a list
    timer.start("run_query_for_all_samples")
    logger.info("Running query for all samples and concatenating results.")
    dfsdf: pl.DataFrame = pl.concat(
        con.execute(f"EXECUTE run_query(sample := {s});").pl().insert_column(0, pl.lit(s).alias("sample"))
        for s in range(SAMPLES)
    )
    timer.end()
    logger.info("All sample queries executed and concatenated.")
    dfsdf



    timer.start("add_rank_column")
    logger.info("Adding rank column to DataFrame.")
    dfsdf = dfsdf.with_columns(
        pl.int_range(pl.len()).over("sample").alias("rank")
    )
    timer.end()
    logger.info("Rank column added.")



    # Define which columns are the group-by keys (INDEX_COLS) and which are the output columns (OUTPUT_COLS)
    # - moved to parameters cell at top of notebook

    # Save these to disk for later use
    timer.start("save_index_and_output_cols")
    logger.info("Saving INDEX_COLS and OUTPUT_COLS to disk.")
    with open(f"{OUTPUT_DIR}/INDEX_COLS.pkl", 'wb') as f:
        pickle.dump(INDEX_COLS, f)
    with open(f"{OUTPUT_DIR}/OUTPUT_COLS.pkl", 'wb') as f:
        pickle.dump(OUTPUT_COLS, f)
    timer.end()
    logger.info("INDEX_COLS and OUTPUT_COLS saved.")



    # Combine all the samples into one table, grouped-by the group-by keys. Each cell contains an n <= # of samples length array of values.
    timer.start("drop_sample_and_group_by")
    logger.info("Grouping DataFrame by index columns.")
    DEFAULT_INDEX_COLS = ["rank"]
    listdf = dfsdf.drop("sample").group_by(INDEX_COLS or DEFAULT_INDEX_COLS, maintain_order=True).all()
    timer.end()
    logger.info("Group-by operation complete.")
    listdf



    # What are all the possible group-by key combinations?
    timer.start("get_all_groups")
    logger.info("Selecting all group-by key combinations.")
    allgroups: pl.DataFrame = listdf.select(INDEX_COLS or DEFAULT_INDEX_COLS)
    timer.end()
    logger.info("All group-by key combinations selected.")
    allgroups.to_dicts()



    # Template for the final output, including all possible group-by groups
    # Obtained by collecting all the samples in a big table and then keeping only the first occurrence of each groupby key.
    # Then, fill all OUTPUT_COLS with nulls
    timer.start("save_template")
    logger.info("Creating and saving template DataFrame.")
    templatedf = dfsdf.drop("sample").group_by(INDEX_COLS or DEFAULT_INDEX_COLS, maintain_order=True).first()
    templatedf = templatedf.clear(n=len(allgroups)).with_columns(allgroups)
    templatedf

    templatedf.write_csv(f"{OUTPUT_DIR}/template.csv")

    with open(f"{OUTPUT_DIR}/template.pkl", "wb") as f:
        pickle.dump(templatedf, f)
    timer.end()
    logger.info("Template DataFrame saved.")



    # Write all table entries in the output table to their own JSON files. Each file has a number, the information of which file corresponds to which table entry
    # is stored in reverse_map.json (as well as in the files themselves)
    timer.start("write_json_files")
    logger.info("Writing output JSON files for each group and column.")
    os.makedirs(f"{OUTPUT_DIR}/json", exist_ok=True)
    i: int = 0
    for col in OUTPUT_COLS:
        logger.debug(f"Processing output column: {col}")
        for group in allgroups.iter_rows(named=True):
            values = listdf.filter(pl.col(k).eq(v) for k, v in group.items()).select(col).to_series()
            j = pl.DataFrame().with_columns([
                pl.lit(col).alias("col"),
                pl.lit(group).alias("row"),
                pl.lit(values.explode().dtype.__repr__()).alias("dtype"),
                pl.lit(SAMPLES).alias("samples"),
                values.alias("values"),
            ])
            j.write_json(f"{OUTPUT_DIR}/json/{i}.json")
            logger.debug(f"Wrote {OUTPUT_DIR}/json/{i}.json for group {group} and column {col}")
            i+=1
    os.fsync(os.open(f"{OUTPUT_DIR}/json", os.O_RDONLY))  # Ensure all writes are flushed to disk

    shutil.rmtree(TRUE_OUTPUT_DIR, ignore_errors=True) # Move the temp dir to the true output dir, atomic overwrite
    shutil.move(OUTPUT_DIR, TRUE_OUTPUT_DIR)
    OUTPUT_DIR = TRUE_OUTPUT_DIR

    timer.end()
    logger.info("All JSON files written and flushed to disk.")
