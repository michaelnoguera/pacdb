All input data is in ./data.
Queries in ./queries will be run.
Move the queries from ./queries-notnow into ./queries that you want to run.

All outputs are in ./outputs. Communicaiton between workflow steps takes the form of
json files, one per entry, labeled with the row and column to which the entry belongs.
The folders full of json files in ./outputs are zipped to make tracking them in git manageable. Unzip
the zip files if you want to see the data.

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

Usage:
------
The following steps will work to run Q1. Make sure you don't have anything precious in the outputs folder.

rm -r ./outputs/* 
python3.11 autopac-duckdb-step1.py
python3.11 autopac-duckdb-step2.py
python3.11 autopac-duckdb-step3.py

If you are looking at Jupyter notebooks, make sure that you are using the kernel at .venv/bin/jupyter

To clean up before committing, delete everything except the zip files in ./outputs

find ./outputs -type f ! -name "*.zip" -delete
find ./outputs -type d -empty -delete