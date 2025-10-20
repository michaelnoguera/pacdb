.PHONY: run clean

run:
	uv run autopac_duckdb_step1.py && uv run autopac_duckdb_step2.py -mi 0.25 && uv run autopac_duckdb_step3.py

benchmark:
	@set -e
	@bash -c 'comm -12 \
		<(find ./queries -type f ! -name ".*" -exec basename {} \; | sort) \
		<(find ./queries-notnow -type f ! -name ".*" -exec basename {} \; | sort) \
		| grep . && { echo "Conflict detected. Aborting." >&2; exit 1; } || exit 0'
	find ./queries -type f ! -name '.*' -exec mv -n {} ./queries-notnow/ \;
	uv run timing_benchmark.py
	# jq -s ' \
	# 	group_by(.query) \
	# 	| map(max_by(.mi)) \
	# 	| sort_by(.query | capture("q(?<num>\\d+)-").num | tonumber) \
	# 	| map(del(.mi)) \
	# ' benchmarks/*.sql.json > benchmarks/merged.json
	uv run merge_all_json.py
	echo "To plot results use ./notebooks/timing_benchmark_graphs.ipynb"

clean:
	rm -r outputs || true
	mkdir outputs || true
	rm -r times || true
	mkdir times || true
	rm -r benchmarks || true
	mkdir benchmarks || true
	find . -maxdepth 1 -name 'ap-duckdb-*step1.ipynb' -exec rm {} + || true
	find . -maxdepth 1 -name 'ap-duckdb-*step1.py' -exec rm {} + || true
	find . -maxdepth 1 -name 'ap-duckdb-*step3.ipynb' -exec rm {} + || true
	find . -maxdepth 1 -name 'ap-duckdb-*step3.py' -exec rm {} + || true