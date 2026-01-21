to generate files used for analysis:

uv run autopac_duckdb_step1.py
uv run autopac_duckdb_step2.py -mi 0.0078125 -mi 0.015625 -mi 0.03125 -mi 0.0625 -mi 0.125 -mi 0.25 -mi 0.5 -mi 1.0 -mi 2.0 -mi 4.0
uv run autopac_duckdb_step3.py

put them in a folder called outputs_1024 (or symlink the outputs folder: `ln -s outputs outputs_1024`)