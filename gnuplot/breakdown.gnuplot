# ================================================================
#  benchmark_pac_step_by_step.gnuplot
#  High-quality, properly aligned stacked bar chart
#  for PAC-DB step-by-step execution times
# ================================================================

reset

# ---------- OUTPUT SETTINGS ----------
set terminal pdfcairo size 3.35in,1.25in font "Times New Roman,11" linewidth 1.2
set output "breakdown.pdf"
#set term pngcairo size 750,250 font "Times New Roman,10" linewidth 1.1
#set output "benchmark_pac_step_by_step.png"

# ---------- GENERAL STYLE ----------
set style fill solid 1 border rgb "black"
set boxwidth 0.5 absolute
set style data histograms
set style histogram gap 4
set style histogram rowstacked
set style line 1 lc rgb 'black' lt 1 lw 1.2
set grid ytics lc rgb "#888888" dt 2 lw 0.8

# remove all margins
set lmargin at screen 0.11
set rmargin at screen 0.975
set tmargin at screen 0.95
set bmargin at screen 0.2

set xlabel "Query" offset 0,0.85 font ",11"
set ylabel "Runtime (s)" offset 1.25,0 font ",11"
#set title "PAC-DB Step-by-Step Execution Time" font ",12" offset 0,-0.5

set tics font ",11"
set tics nomirror
set xtics format ""
#set xrange [-0.4:*]
set xtics scale 0
set xtics offset 0,0.25

set ytics out
set ytics offset 0.4,0


# Move key to top under title
set key horizontal top right nobox
set key offset 0,0.7
set key samplen 3 spacing 1 width -6 height 0
set key font ",11"

set border 3 front lt black linewidth 1.0

# Y-axis range with padding
#set yrange [0:18]
# set ytics offset 0.8,0

# Reduce offset before first bar
set offsets -1, 0, 0, 0

# ---------- COLOR PALETTE ----------
color1 = "#4C78A8"  # blue
color2 = "#72B7B2"  # teal
color3 = "#E45756"  # red

# ---------- DATA PREPARATION ----------
set datafile separator "\t"

# ---------- PLOT ----------
plot 'breakdown-sf1-r6615.tsv' using 2:xtic(1) title "(1) DuckDB Execution" lc rgb color1, \
     '' using 3 title "(2) Noise Calculation" lc rgb color2, \
     '' using 4 title "(3) Reassembly" lc rgb color3, \
     '' using 0:(column(2)+column(3)+column(4)):(column(2)+column(3)+column(4)==0 ? "" : sprintf("%.1f",column(2)+column(3)+column(4))) with labels font ",11" offset char 0,0.65 center notitle
     #'' using 0:(column(2)+column(3)+column(4)):(column(2)==0 ? "" : sprintf("%.2f",column(2))) with labels font ",10" offset char 0,1.5 center notitle, \

# ---------- POST ----------
unset output
print "✅ Plot success"

