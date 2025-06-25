.PHONY: run clean

run:
	uv run autopac-duckdb-step1.py && uv run autopac-duckdb-step2.py && uv run autopac-duckdb-step3.py

clean:
	rm -r outputs
	mkdir outputs
	rm -r times
	mkdir times
	rm ap-duckdb-q[0-9]-customer-step[0-9].ipynb ap-duckdb-q[0-9][0-9]-customer-step[0-9].ipynb