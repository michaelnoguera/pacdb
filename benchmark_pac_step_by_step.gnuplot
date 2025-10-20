# ================================================================
#  benchmark_pac_step_by_step.gnuplot
#  High-quality, properly aligned stacked bar chart
#  for PAC-DB step-by-step execution times
# ================================================================

reset

# ---------- OUTPUT SETTINGS ----------
set terminal pdfcairo size 4.75in,1.75in font "Times New Roman,10" linewidth 1.1
set output "benchmark_pac_step_by_step.pdf"
#set term pngcairo size 750,250 font "Times New Roman,10" linewidth 1.1
#set output "benchmark_pac_step_by_step.png"

# ---------- GENERAL STYLE ----------
set style fill solid 0.9 border rgb "black"
set boxwidth 0.5 absolute
set style data histograms
set style histogram gap 4
set style histogram rowstacked
set style line 1 lc rgb 'black' lt 1 lw 1
set grid ytics lc rgb "#888888" dt 2 lw 0.8

# Optimized margins for two-column layout
# set lmargin at screen 0.25
# set rmargin at screen 0.25
# set tmargin at screen 0.25
# set bmargin at screen 0.25

set xlabel "Query" offset 0,0.1 font ",11"
set ylabel "Time (seconds)" offset 1,0 font ",11"
#set title "PAC-DB Step-by-Step Execution Time" font ",12" offset 0,-0.5

set tics font ",10"
set tics nomirror
set xtics format ""
#set xrange [-0.4:*]
set xtics scale 0
#set xtics offset 0,0.4

# Move key to top under title
#set key horizontal top left
#set key width 0.3 maxrows 1
set key top left vertical Left reverse enhanced autotitles columnhead nobox

#set key outside right center vertical Left reverse enhanced autotitles columnhead nobox
set key invert samplen 3 spacing 1 width 0 height 0 


set border 3 front lt black linewidth 1.0

# Y-axis range with padding
set yrange [0:4.2]
# set ytics offset 0.8,0

# Reduce offset before first bar
# set offsets 0.15, 0.15, 0, 0

# ---------- COLOR PALETTE ----------
color1 = "#4C78A8"  # blue
color2 = "#72B7B2"  # teal
color3 = "#E45756"  # red

# ---------- DATA PREPARATION ----------
set datafile separator "\t"

# ---------- PLOT ----------
plot 'benchmarks/all.tsv' using 2:xtic(1) title "DuckDB Execution" lc rgb color1, \
     '' using 3 title "Noise Calculation" lc rgb color2, \
     '' using 4 title "Reassembly" lc rgb color3, \
     '' using 0:(column(2)+column(3)+column(4)):(column(2)+column(3)+column(4)==0 ? "" : sprintf("%.2f",column(2)+column(3)+column(4))) with labels font ",10" offset char 0,0.65 center notitle

# ---------- POST ----------
unset output
print "✅ Plot successfully saved to benchmark_pac_step_by_step.pdf"
