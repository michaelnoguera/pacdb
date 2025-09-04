# Instructions for using hyperfine to benchmark step 1 directly from the command line
# (as opposed to using the `make benchmark` script)

# this is bash script compatible, so you can run all steps from the repo root directory
# as `sudo -v && bash benchmark-hyperfine/readme.txt`

# 0. warn if queries folder is empty (ignore hidden files)

if [ -z "$(find queries -type f -not -name '.*' -print -quit)" ]; then
  echo "Queries folder is empty, no queries to run!."
  exit
fi

# 1. generate the step 1 scripts

uv run autopac-duckdb-step1.py --prepare-only

# 2. run all the step 1 scripts through hyperfine

sudo -v && \
hyperfine --runs 4 \
-L pythonfile $(find . -type f -maxdepth 1 -name 'ap-duckdb-*.py' -exec basename {} \; | tr '\n' ',' | sed 's/,$//' ) \
--prepare 'sudo purge' 'uv run {pythonfile}' \
--export-json benchmark-hyperfine/hyperfine.json

# 3. sort the results

jq '
  .results |=
    ( sort_by(
        (.command
         | capture("q(?<num>[0-9]+)").num
         | tonumber)
      )
      | map(
          .command |= (
            sub("^uv run ap-duckdb-"; "")
            | sub("-step1\\.py$"; "")
          )
        )
    )
' benchmark-hyperfine/hyperfine.json > benchmark-hyperfine/hyperfine_sorted.json

# 4. plot the output

uv run benchmark-hyperfine/plot_whisker.py benchmark-hyperfine/hyperfine_sorted.json