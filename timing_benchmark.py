import argparse
import json
import logging
import os
import pathlib
import shutil
import time

import autopac_duckdb_step1 as step1
import autopac_duckdb_step2 as step2
import autopac_duckdb_step3 as step3

ALLQUERIES = [
    # "q3a-customer-naive.sql",
    # "q3b-customer-filter-pushdown.sql",
    # "q3c-customer-sample-pushup.sql",

    "q1-customer.sql",
    "q3-customer.sql",
    "q4-customer.sql",
    "q5-customer.sql",
    "q6-customer.sql",
    "q7-customer.sql",
    "q8-customer.sql",
    "q9-customer.sql",
    "q12-customer.sql",
    "q13-customer.sql",
    "q14-customer.sql",
    "q15-customer.sql",
    "q17-customer.sql",
    #"q19-customer.sql",
    "q20-customer.sql",
    "q21-customer.sql",
    "q22-customer.sql",
]

BENCHMARKS_FOLDER = pathlib.Path("benchmarks")

parser = argparse.ArgumentParser(description="Timing benchmark for PACDB queries.")
group = parser.add_mutually_exclusive_group()
group.add_argument(
    "-v", "--verbose",
    action="store_true",
    help="Set logging level to INFO (same as -l INFO)."
)
group.add_argument(
    "-l", "--log-level",
    choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    default="WARNING",
    help="Set the logging level (default: WARNING)."
)
args = parser.parse_args()

if args.verbose:
    args.log_level = "INFO"

logging.basicConfig(
    level=getattr(logging, args.log_level),
    format="%(asctime)s | %(filename)s:%(lineno)d %(levelname)s %(message)s"
)

if not os.path.exists(BENCHMARKS_FOLDER):
    os.makedirs(BENCHMARKS_FOLDER)

# if queries folder is not empty (except for .gitkeep), warn and ask if we should proceed
queries_folder = pathlib.Path("queries")
existing_files = [f for f in os.listdir(queries_folder) if f != ".gitkeep"]
if existing_files:
    print("Warning: 'queries' folder is not empty. Please remove the files and retry.")
    exit(1)

try:
    for queryfile in ALLQUERIES:
        results: dict = {}
        BENCHMARK_FILE = BENCHMARKS_FOLDER / f"{queryfile}.json"
        MI = 0.125
        results.update({
            "query": queryfile,
            "mi": MI,
        })
        print(f'timing_benchmark.py: Now running {queryfile} with mi={MI}')

        # load the query file into the queries folder
        src = pathlib.Path("queries-notnow") / queryfile
        dst = pathlib.Path("queries") / queryfile
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

        num_trials = 3
        step1_times, step2_times, step3_times = 0., 0., 0.
        for _ in range(num_trials):
            # run step 1 and time it (autopac_duckdb_step1.py)
            # cmd = "uv run autopac_duckdb_step1.py"
            start = time.time()
            step1.main()
            #os.system(cmd)
            end = time.time()
            elapsed = end - start
            step1_times += elapsed
            # print(f"Step 1 for {queryfile} took {elapsed:.3f} seconds.")
            

            # run step 2 and time it (autopac_duckdb_step2.py)
            #cmd = "uv run autopac_duckdb_step2.py -mi 0.125"
            start = time.time()
            #os.system(cmd)
            step2.main(mi=0.125, verbose=False)
            end = time.time()
            elapsed = end - start
            # print(f"Step 2 for {queryfile} took {elapsed:.3f} seconds.")
            step2_times += elapsed

            # run step 3 and time it (autopac_duckdb_step3.py)
            #cmd = "uv run autopac_duckdb_step3.py"
            start = time.time()
            #os.system(cmd)
            step3.main()
            end = time.time()
            elapsed = end - start
            step3_times += elapsed
            # print(f"Step 3 for {queryfile} took {elapsed:.3f} seconds.")

        results.update({"step1": step1_times / num_trials})
        results.update({"step2": step2_times / num_trials})
        results.update({"step3": step3_times / num_trials})

        print(results)

        # Save results to a JSON file
        with open(BENCHMARK_FILE, "w") as f:
            json.dump(results, f, indent=4)

        # remove the query file from the queries folder
        if dst.exists():
            os.remove(dst)
        
except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully.")
