# ================================================================
#  benchmark_comparison.gnuplot
#  Clustered bar chart comparing TPCH runtimes
#  between Scale Factor 1 and Scale Factor 2
# ================================================================

reset

# ---------- OUTPUT SETTINGS ----------
set terminal pdfcairo size 3.35in,1in font "Times New Roman,11" linewidth 1.2
set output "comparison-sf.pdf"
#set term pngcairo size 750,250 font "Times New Roman,10" linewidth 1.1
#set output "benchmark_sf_comparison.png"

# ---------- GENERAL STYLE ----------
set style fill solid 1.2 border rgb "black"
#set boxwidth 0.35
set style data histograms
set style histogram clustered gap 1
set style line 1 lc rgb 'black' lt 1 lw 1
set grid ytics lc rgb "#888888" dt 2 lw 0.8

set xlabel "Query" offset 0,0.75 font ",11"
set ylabel "Runtime (s)" offset 2,-0.4 font ",11"
#set title "Runtime by Scale Factor" font ",12" offset 0,-0.5

set tics font ",11"
set tics nomirror
set xtics format ""
#set xrange [-0.4:*]
set xtics scale 0
set xtics offset 0,0.25

set ytics out
set ytics offset 0.4,0


set key top right horizontal Left reverse enhanced autotitles columnhead nobox
#set key invert samplen 3 spacing 1 width 0 height 0 

#set key horizontal top right nobox
set key samplen 3 spacing 1 width 0 height 0
set key font ",10"
set key offset 0,0.15

set border 3 front lt black linewidth 1.0

# Logarithmic scale for better visualization of data range
set logscale y

#set yrange [0:500]
#set ytics 0,10,500

set lmargin at screen 0.14
set rmargin at screen 0.975
set tmargin at screen 0.95
set bmargin at screen 0.28

# ---------- COLOR PALETTE ----------
color1 = "#4C78A8"  # blue for SF1
color2 = "#E45756"  # red for SF2
color3 = "#72B7B2"  # teal for SF10

# ---------- DATA PREPARATION ----------
set datafile separator "\t"

# ---------- PLOT ----------
plot 'comparison-sf1.tsv' using 5:xtic(1) title "SF=1" lc rgb color1, \
     'comparison-sf10.tsv' using 5 title "SF=10" lc rgb color2, \
     'comparison-sf100.tsv' using 5 title "SF=100" lc rgb color3

# ---------- POST ----------
unset output
print "✅ Plot success"
