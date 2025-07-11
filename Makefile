.PHONY: run clean

run:
	uv run autopac-duckdb-step1.py && uv run autopac-duckdb-step2.py -mi 0.25 && uv run autopac-duckdb-step3.py

benchmark:
	@set -e
	@bash -c 'comm -12 \
		<(find ./queries -type f ! -name ".*" -exec basename {} \; | sort) \
		<(find ./queries-notnow -type f ! -name ".*" -exec basename {} \; | sort) \
		| grep . && { echo "Conflict detected. Aborting." >&2; exit 1; } || exit 0'
	find ./queries -type f ! -name '.*' -exec mv -n {} ./queries-notnow/ \;
	uv run timing_benchmark.py
	jq -s ' \
		group_by(.query) \
		| map(max_by(.mi)) \
		| sort_by(.query | capture("q(?<num>\\d+)-").num | tonumber) \
		| map(del(.mi)) \
	' benchmarks/*.sql.json > benchmarks/merged.json

clean:
	rm -r outputs
	mkdir outputs
	rm -r times
	mkdir times
	rm -r benchmarks
	mkdir benchmarks
	rm ap-duckdb-q[0-9]-customer-step[0-9].ipynb ap-duckdb-q[0-9][0-9]-customer-step[0-9].ipynb