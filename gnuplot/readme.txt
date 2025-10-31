To plot all graphs, run `gnuplot *.gnuplot`.

To update the data being graphed, copy benchmarks/all.tsv and unnoised/times.tsv manually to this
directory and update the plotting script to read from the correct files. You may have to manually
reformat the TSV for the generated figure to match that in the paper exactly (for example changing
"Q1" to "1" in the first column.)

Figures in the paper used `gnuplot 6.0 patchlevel 3` installed through Homebrew on a Mac.