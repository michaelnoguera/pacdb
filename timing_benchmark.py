import json
import os
import pathlib
import shutil
import time

ALLQUERIES = [
    "q1-customer.sql",
    "q3-customer.sql",
    "q4-customer.sql",
    "q5-customer.sql",
    "q5-customer-no-prejoin.sql",
    "q5-customer-new.sql",
    "q6-customer.sql",
    "q7-customer.sql",
    "q8-customer.sql",
    "q9-customer.sql",
    "q12-customer.sql",
    "q13-customer.sql",
    "q14-customer.sql",
    "q15-customer.sql",
    "q17-customer.sql",
    "q19-customer.sql",
    "q20-customer.sql",
    "q21-customer.sql",
    "q22-customer.sql",
]

BENCHMARKS_FOLDER = pathlib.Path("benchmarks")
if not os.path.exists(BENCHMARKS_FOLDER):
    os.makedirs(BENCHMARKS_FOLDER)

try:
    for queryfile in ALLQUERIES:
        results: dict = {}
        BENCHMARK_FILE = BENCHMARKS_FOLDER / f"{queryfile}.json"

        results.update({
            "query": queryfile,
            "mi": 0.125,
        })

        # load the query file into the queries folder
        src = pathlib.Path("queries-notnow") / queryfile
        dst = pathlib.Path("queries") / queryfile
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

        # run step 1 and time it (autopac-duckdb-step1.py)
        cmd = "uv run autopac-duckdb-step1.py"
        start = time.time()
        os.system(cmd)
        end = time.time()
        elapsed = end - start
        print(f"Step 1 for {queryfile} took {elapsed:.3f} seconds.")
        results.update({"step1": elapsed})

        # run step 2 and time it (autopac-duckdb-step2.py)
        cmd = "uv run autopac-duckdb-step2.py -mi 0.125"
        start = time.time()
        os.system(cmd)
        end = time.time()
        elapsed = end - start
        print(f"Step 2 for {queryfile} took {elapsed:.3f} seconds.")
        results.update({"step2": elapsed})

        # run step 3 and time it (autopac-duckdb-step3.py)
        cmd = "uv run autopac-duckdb-step3.py"
        start = time.time()
        os.system(cmd)
        end = time.time()
        elapsed = end - start
        print(f"Step 3 for {queryfile} took {elapsed:.3f} seconds.")
        results.update({"step3": elapsed})

        # Save results to a JSON file
        with open(BENCHMARK_FILE, "w") as f:
            json.dump(results, f, indent=4)

        # remove the query file from the queries folder
        if dst.exists():
            os.remove(dst)
            
except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully.")
