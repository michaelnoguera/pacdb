All input data is in ./data.

All outputs are in ./outputs. Communicaiton between workflow steps takes the form of
json files, one per entry, labeled with the row and column to which the entry belongs.
The folders full of json files in ./outputs are zipped to make tracking them in git manageable. Unzip
the zip files if you want to see the data.

Setup:
python3.11 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt

then make sure that you are using jupyter kernel at .venv/bin/jupyter

The following steps will work to run Q1. Make sure you don't have anything precious in the outputs folder.

rm -r ./outputs/* 
python3.11 pac-duckdb-step1.py
python3.11 pac-duckdb-step2-caller.py -e pac-duckdb-q1 -mi 0.125
python3.11 pac-duckdb-q1-step3.py

To clean up before committing, delete everything except the zip files in ./outputs

find ./outputs -type f ! -name "*.zip" -delete
find ./outputs -type d -empty -delete