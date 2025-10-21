All input data is in ./data.
Queries in ./queries will be run.
Move the queries from ./queries-notnow into ./queries that you want to run.

All outputs are in ./outputs. Communicaiton between workflow steps takes the form of
json files, one per entry, labeled with the row and column to which the entry belongs.
The folders full of json files in ./outputs are zipped to make tracking them in git manageable. Unzip
the zip files if you want to see the data.

Run in Docker (no setup):
-------------------------
docker build -t pacdb .
docker run -it --rm \
    --mount type=bind,source="$(pwd)",target=/app \
    --tmpfs /app/.venv:uid=1000,gid=1000 \
    -w /app \
    -e UV_PROJECT_ENVIRONMENT=/app/.venv \
    pacdb \
    uv sync --offline && uv run autopac_duckdb_step1.py && uv run autopac_duckdb_step2.py && uv run autopac_duckdb_step3.py

Setup (devcontainer):
---------------------
Open in VSCode, then Ctrl+Shift+P > Dev Containers: Reopen in Container

Setup (uv):
-----------
curl -LsSf https://astral.sh/uv/install.sh | sh  # install uv
uv sync
source .venv/bin/activate

Setup (pip/venv):
-----------------
python3.13 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt

To install the DuckDB CLI:
------------------------------------------
Go to the releases for version 1.3.2, and copy the url for your platform: https://github.com/duckdb/duckdb/releases/tag/v1.3.2
wget https://github.com/duckdb/duckdb/releases/download/v1.3.2/duckdb_cli-osx-universal.zip
unzip duckdb_cli-osx-universal.zip
mv duckdb ~/.local/bin  # add to your path, this directory is on my path

To generate TPC-H data:
-----------------------
The scripts in this repository expect to find a TPC-H dataset located at ./data/tpch/tpch.duckdb.

We provide ./data/tpch/tpch-sf0.1.duckdb (scale factor = 0.1) for convenience and reproducibility.
If you want to use this, then you must copy it to the expected location:
mv ./data/tpch/tpch-sf0.1.duckdb ./data/tpch/tpch.duckdb

The ./data/tpch/generate.py script is used to generate the data for all other scale factors. The
table will be written directly to ./data/tpch/tpch.duckdb, and replace any existing file there.

Usage for generate.py:
cd data/tpch
uv run generate.py --sf 1

Usage:
------
The following steps will work to run any query you put in the ./queries folder. Make sure you don't have anything
precious in the outputs folder.

rm -r ./outputs/* 
uv run autopac_duckdb_step1.py
uv run autopac_duckdb_step2.py
uv run autopac_duckdb_step3.py

There is a timing benchmark script to automating all the queries in a list: uv run timing_benchmark.py
You will have to edit the list of queries in that file if you want to change the queries run.
Make sure that the ./queries folder is empty when you start the script, especially if you are re-running
after it has crashed.

If you are looking at Jupyter notebooks, make sure that you are using the kernel at .venv/bin/jupyter

To clean up before committing, delete everything except the zip files in ./outputs

find ./outputs -type f ! -name "*.zip" -delete
find ./outputs -type d -empty -delete

The Makefile has some useful targets that automate many of these steps.