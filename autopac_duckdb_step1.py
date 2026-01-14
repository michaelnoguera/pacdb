import logging
import os
import pickle
import shutil
import tempfile

import duckdb
import parse
import polars as pl

QUERYFOLDER = "./queries"

def run_specific_query(query: str, save_extras: bool = False):
    QUERY = query
    QUERYFILE = f"./{QUERYFOLDER}/{QUERY}.sql"

    # In the queryfile, there are "--begin SAMPLE_STEP--" and "--end SAMPLE_STEP--" comments delimiting the SAMPLE_STEP and the PREPARE_STEP.
    # Extract the SQL code between these comments into variables SAMPLE_STEP and PREPARE_STEP respectively.
    query_strings = {}
    lines = []
    with open(QUERYFILE, "r") as f:
        lines = f.readlines()
        start = lines.index("--begin SAMPLE_STEP--\n") + 1
        end = lines.index("--end SAMPLE_STEP--\n")
        query_strings["sample"] = "".join(lines[start:end]).strip()
        start = lines.index("--begin PREPARE_STEP--\n") + 1
        end = lines.index("--end PREPARE_STEP--\n")
        query_strings["prepare"] = "".join(lines[start:end]).strip()

    # There are also --var:SAMPLES=2048\n style comments in the queryfile. Each of these defines a variable
    pattern = parse.compile("--var:{name:^}={value:^}")
    for line in lines:
        result = pattern.parse(line)
        if result:
            print(f"Found variable: {result['name']} = {result['value'].strip()}")
            query_strings[result["name"]] = eval(result["value"].strip())

    SAMPLES = int(query_strings["SAMPLES"]) or 1024

    # Now, find every occurence of `1024//2` and replace it with f`{SAMPLES//2}` in those two strings
    query_strings["sample"] = query_strings["sample"].replace("1024//2", f"{SAMPLES // 2}")
    query_strings["prepare"] = query_strings["prepare"].replace("1024//2", f"{SAMPLES // 2}")

    EXPERIMENT = f"ap-duckdb-{QUERY}"
    OUTPUT_DIR = f"./outputs/{EXPERIMENT}-step1"
    SAMPLE_STEP=query_strings["sample"]
    PREPARE_STEP=query_strings["prepare"]
    INDEX_COLS=query_strings["INDEX_COLS"] # Define which columns are the group-by keys (INDEX_COLS)
    OUTPUT_COLS=query_strings["OUTPUT_COLS"] # and which are the output columns (OUTPUT_COLS)

    logger = logging.getLogger()
    logger.info(f"Starting main() for experiment: {EXPERIMENT}")

    # We need to do everything in a temp directory to avoid conflicts with past runs
    TRUE_OUTPUT_DIR = OUTPUT_DIR
    OUTPUT_DIR = tempfile.mkdtemp()
    logger.info(f"Using temporary output directory: {OUTPUT_DIR}")

    # duckdb load data/tpch/tpch.duckdb into the temporary in-memory database
    logger.info("Connecting to DuckDB database at data/tpch/tpch.duckdb")
    con = duckdb.connect(database='data/tpch/tpch.duckdb', read_only=True)
    logger.info("DuckDB connection established.")
    tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]  # noqa: F841
    #for t in tables:
    #    con.execute(f"CREATE TABLE {t} AS SELECT * FROM 'data/tpch/{t}.parquet'")

    # Construct the table of random samples
    # to use, join it with the lineitem table (for specific sample # s) and filter to just the
    # rows where random_binary = 1.0
    # This will give us a 50% sample of the lineitem table for each sample # s
    assert SAMPLES % 2 == 0, "SAMPLES must be even to create complementary samples."

    logger.info("Creating random_samples table.")
    con.execute(SAMPLE_STEP)
    logger.info("random_samples table created.")

    # The randomness of what rows are chosen is saved to disk in `random_binary.json`.
    # For each sample #, there is an array with one entry per row, where 1 means the row was chosen and 0 means it was not.
    if save_extras:
        con.execute("""
        SELECT sample_id, array_agg(row_id) as selected_rows
        FROM random_samples
        GROUP BY sample_id;
        """).pl().write_json(f"{OUTPUT_DIR}/selected_rows.json")
        logger.info(f"selected_rows.json written to {OUTPUT_DIR}/selected_rows.json")


    # A prepared statement is defined for the query. We will then execute it once per sample.
    logger.info("Preparing query statement.")
    con.execute(PREPARE_STEP)
    logger.info("Query prepared.")


    # Save csv copies of the first 5 samples
    if save_extras:
        os.makedirs(f"{OUTPUT_DIR}/csv", exist_ok=True)
        logger.info(f"Created CSV output directory: {OUTPUT_DIR}/csv")
        for s in range(5):
            logger.debug(f"Writing CSV for sample {s}")
            con.execute(f"EXECUTE run_query(sample := {s});").pl().write_csv(f"{OUTPUT_DIR}/csv/sample_{s}.csv")
        logger.info("First 5 sample CSVs written.")


    # Run the query for each sample, but accumulate in a pl.DataFrame instead of a list
    logger.info("Running query for all samples and concatenating results.")
    dfsdf: pl.DataFrame = pl.concat(
        con.execute(f"EXECUTE run_query(sample := {s});").pl().insert_column(0, pl.lit(s).alias("sample"))
        for s in range(SAMPLES)
    )
    logger.info("All sample queries executed and concatenated.")
    dfsdf


    logger.info("Adding rank column to DataFrame.")
    dfsdf = dfsdf.with_columns(
        pl.int_range(pl.len()).over("sample").alias("rank")
    )
    logger.info("Rank column added.")


    # Save these to disk for later use
    logger.info("Saving INDEX_COLS and OUTPUT_COLS to disk.")
    with open(f"{OUTPUT_DIR}/INDEX_COLS.pkl", 'wb') as f:
        pickle.dump(INDEX_COLS, f)
    with open(f"{OUTPUT_DIR}/OUTPUT_COLS.pkl", 'wb') as f:
        pickle.dump(OUTPUT_COLS, f)
    logger.info("INDEX_COLS and OUTPUT_COLS saved.")


    # Combine all the samples into one table, grouped-by the group-by keys. Each cell contains an n <= # of samples length array of values.
    logger.info("Grouping DataFrame by index columns.")
    DEFAULT_INDEX_COLS = ["rank"]
    listdf = dfsdf.drop("sample").group_by(INDEX_COLS or DEFAULT_INDEX_COLS, maintain_order=True).all()
    logger.info("Group-by operation complete.")
    listdf


    # What are all the possible group-by key combinations?
    logger.info("Selecting all group-by key combinations.")
    allgroups: pl.DataFrame = listdf.select(INDEX_COLS or DEFAULT_INDEX_COLS)
    logger.info("All group-by key combinations selected.")
    allgroups.to_dicts()


    # Template for the final output, including all possible group-by groups
    # Obtained by collecting all the samples in a big table and then keeping only the first occurrence of each groupby key.
    # Then, fill all OUTPUT_COLS with nulls
    logger.info("Creating and saving template DataFrame.")
    templatedf = dfsdf.drop("sample").group_by(INDEX_COLS or DEFAULT_INDEX_COLS, maintain_order=True).first()
    templatedf = templatedf.clear(n=len(allgroups)).with_columns(allgroups)
    templatedf

    templatedf.write_csv(f"{OUTPUT_DIR}/template.csv")

    with open(f"{OUTPUT_DIR}/template.pkl", "wb") as f:
        pickle.dump(templatedf, f)
    logger.info("Template DataFrame saved.")


    # Write all table entries in the output table to their own JSON files.
    # Each file has a number, the information of which file corresponds to which table entry
    # is stored in reverse_map.json (as well as in the files themselves)
    logger.info("Writing output JSON files for each group and column.")
    os.makedirs(f"{OUTPUT_DIR}/json", exist_ok=True)
    i: int = 0
    for col in OUTPUT_COLS:
        logger.debug(f"Processing output column: {col}")
        for group in allgroups.iter_rows(named=True):
            values = listdf.filter(pl.col(k).eq(v) for k, v in group.items()).select(col).to_series()
            null_nan_present = any(1 for v in values.explode().to_list() if v is None or (isinstance(v, float) and (v != v)))
            j = pl.DataFrame().with_columns([
                pl.lit(col).alias("col"),
                pl.lit(group).alias("row"),
                pl.lit(values.explode().dtype.__repr__()).alias("dtype"),
                pl.lit(SAMPLES).alias("samples"),
                pl.lit(null_nan_present).alias("null_nan_present"),
                values.alias("values"),
            ])
            j.write_json(f"{OUTPUT_DIR}/json/{i}.json")
            logger.debug(f"Wrote {OUTPUT_DIR}/json/{i}.json for group {group} and column {col}")
            i+=1
    os.fsync(os.open(f"{OUTPUT_DIR}/json", os.O_RDONLY))  # Ensure all writes are flushed to disk


    # Move the temp dir to the true output dir, atomic overwrite
    shutil.rmtree(TRUE_OUTPUT_DIR, ignore_errors=True)
    shutil.move(OUTPUT_DIR, TRUE_OUTPUT_DIR)
    OUTPUT_DIR = TRUE_OUTPUT_DIR

    logger.info("All JSON files written and flushed to disk.")


def main(prepare_only=False, save_extras=False):
    # Execute all of ./queries/{query}.sql
    pattern = parse.compile("{q}.sql")
    queries_to_run = []
    for queryfile in os.listdir(QUERYFOLDER):
        result = pattern.parse(queryfile)
        if result:
            query = result["q"]
            queries_to_run.append(query)

    for query in queries_to_run:
        if not prepare_only:
            try:
                run_specific_query(query, save_extras=save_extras)
            except Exception as e:
                print(f"Error running query {query}: {e}")
                continue
        

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare-only", action="store_true", help="Do not run the script, only prepare it. Used for external timing.")
    parser.add_argument("--save-extras", action="store_true", 
                        help="Save additional files to show the process (selected rows, first five sample results).")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
    )

    main(prepare_only=args.prepare_only, save_extras=args.save_extras)