# run with `sudo -v && bash run.sh` from the directory containing this script

if [ "$(basename "$PWD")" != "benchmark-q5-optimization" ]; then
	echo "Error: Script must be run from the benchmark-q5-optimization directory."
	exit 1
fi

# move python files to the parent directory
cp ap-duckdb-q5-customer-step1.py ..
cp ap-duckdb-q5-customer-no-prejoin-step1.py ..
cp ap-duckdb-q5-customer-new-step1.py ..

cd ..

hyperfine \
--prepare 'sudo purge' 'uv run ap-duckdb-q5-customer-step1.py' \
--prepare 'sudo purge' 'uv run ap-duckdb-q5-customer-no-prejoin-step1.py' \
--prepare 'sudo purge' 'uv run ap-duckdb-q5-customer-new-step1.py' \
--export-markdown -

rm ap-duckdb-q5-customer-step1.py
rm ap-duckdb-q5-customer-no-prejoin-step1.py
rm ap-duckdb-q5-customer-new-step1.py