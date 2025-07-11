NOTE: This benchmark is run generating 1024 samples, as opposed to 128 in the all-queries benchmark
These python files contain the queries as strings, including the SAMPLES=1024. I have created a shortcut
to run them, just use run.sh in this directory.

sudo -v && bash run.sh

---------------------------------------
INSTRUCTIONS TO RECREATE

On commit 5de5b50b or similar, move
1. q5-customer-new.sql
2. q5-customer-no-prejoin.sql
3. q5-customer.sql
into ./queries/, then run `uv run autopac-duckdb-step1.py` once to generate the notebooks for
each of the queries we are testing.

Then let's convert the step 1 notebooks to python scripts so that we can easily time them
directly from the command line.

uv run jupyter nbconvert --to script ap-duckdb-q5-customer-step1.ipynb \
&& uv run jupyter nbconvert --to script ap-duckdb-q5-customer-no-prejoin-step1.ipynb \
&& uv run jupyter nbconvert --to script ap-duckdb-q5-customer-new-step1.ipynb

We should now have one .py file corresponding to each of those .ipynb (notebook) files.

Now we use hyperfine to time them. In this command sudo -v prompts for sudo password and
caches it to avoid interrupting with the prompt while hyperfine is running.
Then we run hyperfine with 'sudo purge' as a prepare step to clear the page cache before each
run. This is the correct command for MacOS, on Linux use
--prepare 'sync; echo 3 | sudo tee /proc/sys/vm/drop_caches'
instead.

sudo -v && \
hyperfine \
--prepare 'sudo purge' 'uv run ap-duckdb-q5-customer-step1.py' \
--prepare 'sudo purge' 'uv run ap-duckdb-q5-customer-no-prejoin-step1.py' \
--prepare 'sudo purge' 'uv run ap-duckdb-q5-customer-new-step1.py' \
--export-markdown -

The output I get is as follows:

Benchmark 1: uv run ap-duckdb-q5-customer-step1.py
  Time (mean ± σ):      7.721 s ±  0.120 s    [User: 15.040 s, System: 5.020 s]
  Range (min … max):    7.569 s …  7.937 s    10 runs
 
Benchmark 2: uv run ap-duckdb-q5-customer-no-prejoin-step1.py
  Time (mean ± σ):      7.256 s ±  0.122 s    [User: 13.942 s, System: 4.097 s]
  Range (min … max):    7.128 s …  7.549 s    10 runs
 
Benchmark 3: uv run ap-duckdb-q5-customer-new-step1.py
  Time (mean ± σ):      3.501 s ±  0.067 s    [User: 6.425 s, System: 3.318 s]
  Range (min … max):    3.381 s …  3.606 s    10 runs
 
Summary
  uv run ap-duckdb-q5-customer-new-step1.py ran
    2.07 ± 0.05 times faster than uv run ap-duckdb-q5-customer-no-prejoin-step1.py
    2.21 ± 0.05 times faster than uv run ap-duckdb-q5-customer-step1.py

| Command | Mean [s] | Min [s] | Max [s] | Relative |
|:---|---:|---:|---:|---:|
| `uv run ap-duckdb-q5-customer-step1.py` | 7.721 ± 0.120 | 7.569 | 7.937 | 2.21 ± 0.05 |
| `uv run ap-duckdb-q5-customer-no-prejoin-step1.py` | 7.256 ± 0.122 | 7.128 | 7.549 | 2.07 ± 0.05 |
| `uv run ap-duckdb-q5-customer-new-step1.py` | 3.501 ± 0.067 | 3.381 | 3.606 | 1.00 |