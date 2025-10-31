PAC-DB Artifact

All input data is in ./data. Queries in ./queries will be run. Move the queries from 
./queries-notnow into ./queries that you want to run.

All outputs are in ./outputs. Communication between workflow steps takes the form of
json files in the outputs directory.



### If you want to use Docker ###

Run in Docker (without setup):
------------------------------
# Copy the pre-generated tiny dataset into the location where the database file is expected to reside.
mv ./data/tpch/tpch-sf0.1.duckdb ./data/tpch/tpch.duckdb

# Warning: The pre-generated tiny dataset is sf=0.1, smaller than the TPC-H benchmark specifies.
# You might get non-responses from PAC-DB on some queries with sf=0.1, because there are not enough
# selected rows to return non-null results for some samples of the table. This is correct behavior
# from a privacy standpoint, but will prevent you from getting a numeric output. Generate a larger
# dataset of at least sf=1 and retry if this happens repeatedly and you want to see a numeric output.
# The instructions to do that in docker are in the next section of this readme.
# (For Q19 this happens ~16% of the time with sf=0.1 but 0% of the time (99% confidence) with sf=1)

# Build the docker image
docker build -t pacdb .

# Run the benchmark in a docker container
docker run -it --rm \
    --mount type=bind,source="$(pwd)",target=/app \
    --tmpfs /app/.venv:uid=1000,gid=1000 \
    -w /app \
    -e UV_PROJECT_ENVIRONMENT=/app/.venv \
    pacdb \
    uv sync --offline && make unnoised && make benchmark


To generate a larger TPC-H dataset in Docker (without setup):
-------------------------------------------------------------
docker run -it --rm \
    --mount type=bind,source="$(pwd)",target=/app \
    --tmpfs /app/.venv:uid=1000,gid=1000 \
    -w /app \
    -e UV_PROJECT_ENVIRONMENT=/app/.venv \
    pacdb \
    uv sync --offline && cd data/tpch && uv run generate.py --sf 1


To run a specific SQL query in Docker (without setup):
-------------------------------------------------------------
# First, put the corresponding .sql file into the ./queries folder. Then, run all steps:
docker run -it --rm \
    --mount type=bind,source="$(pwd)",target=/app \
    --tmpfs /app/.venv:uid=1000,gid=1000 \
    -w /app \
    -e UV_PROJECT_ENVIRONMENT=/app/.venv \
    pacdb \
    uv sync --offline && uv run autopac_duckdb_step1.py && uv run autopac_duckdb_step2.py && uv run autopac_duckdb_step3.py



### If you do not want to use Docker ###

These are the steps for setting up PAC-DB on your local machine. At the bottom of this readme I have
included the step-by-step commands I ran on a cloudlab machine to gather the data in the paper. The
experiments in the paper were all run without Docker.


Local Setup (uv):
-----------
# Install uv python package manager, if not already installed.
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

# In the repo directory, use uv to fetch all dependencies
uv sync
source .venv/bin/activate


To install the DuckDB CLI:
--------------------------
Go to the releases for version 1.3.2, and copy the url for your platform: https://github.com/duckdb/duckdb/releases/tag/v1.3.2
wget https://github.com/duckdb/duckdb/releases/download/v1.3.2/duckdb_cli-linux-amd64.zip
# wget https://github.com/duckdb/duckdb/releases/download/v1.3.2/duckdb_cli-osx-universal.zip
unzip duckdb_cli-linux-amd64.zip
# unzip duckdb_cli-osx-universal.zip
mv duckdb ~/.local/bin  # add to your path, this directory is on my path


To generate TPC-H data:
-----------------------
The scripts in this repository expect to find a TPC-H dataset located at ./data/tpch/tpch.duckdb.

We provide ./data/tpch/tpch-sf0.1.duckdb (scale factor = 0.1) for convenience.
If you want to use this, then you must copy it to the expected location:
mv ./data/tpch/tpch-sf0.1.duckdb ./data/tpch/tpch.duckdb

The ./data/tpch/generate.py script is used to generate the data for all other scale factors. The
table will be written directly to ./data/tpch/tpch.duckdb, and replace any existing file there.

Usage for generate.py:
cd data/tpch
uv run generate.py --sf 1


To run the comparison benchmark on the dataset in ./data/tpch/tpch.duckdb:
--------------------------------------------------------------------------
make clean
make unnoised
make benchmark


Manual usage to run a specific query
------------------------------------
The following steps will work to run any query you put in the ./queries folder. Make sure you don't have anything
precious in the outputs folder.

rm -r ./outputs/* 
uv run autopac_duckdb_step1.py
uv run autopac_duckdb_step2.py -mi 0.5
uv run autopac_duckdb_step3.py

This is how the data for the privacy benchmark notebooks was obtained. You can run with multiple MIs by specifying
the parameter multiple times in step 2:
uv run autopac_duckdb_step2.py -mi 0.0078125 -mi 0.015625 -mi 0.03125 -mi 0.0625 -mi 0.125 -mi 0.25 -mi 0.5 -mi 1.0 -mi 2.0 -mi 4.0

If you are looking at Jupyter notebooks, make sure that you are using the kernel at .venv/bin/jupyter.



### Steps for performance benchmarking on a new cloudlab.us instance ###
In the paper, we use a r6615 instance from the Clemson cluster with Ubuntu 22.04. To reproduce,
create an instance, SSH, and then run these commands:

# Install uv for python
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

# Install monitoring programs
sudo apt-get update && sudo apt-get install htop btop -y

# Install duckdb 
sudo apt-get update && sudo apt-get install zip unzip -y
wget https://github.com/duckdb/duckdb/releases/download/v1.3.2/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
mv duckdb ~/.local/bin

# Clone repo
git clone git@github.com:michaelnoguera/pacdb.git

# Install dependencies
cd ~/pacdb
uv sync

# Generate data
cd ~/pacdb/data/tpch
uv run generate.py --sf 1

# Run benchmark
cd ~/pacdb
make clean
make benchmark

# Zip results for download
zip -r results.zip benchmarks/ unnoised/ data/tpch/last_generated.txt