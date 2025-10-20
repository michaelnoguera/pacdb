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
python3.11 -m venv .venv
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
in the folder data/tpch, run:
uv run generate.py --sf 2

Usage:
------
The following steps will work to run Q1. Make sure you don't have anything precious in the outputs folder.

rm -r ./outputs/* 
python3.11 autopac_duckdb_step1.py
python3.11 autopac_duckdb_step2.py
python3.11 autopac_duckdb_step3.py

If you are looking at Jupyter notebooks, make sure that you are using the kernel at .venv/bin/jupyter

To clean up before committing, delete everything except the zip files in ./outputs

find ./outputs -type f ! -name "*.zip" -delete
find ./outputs -type d -empty -delete