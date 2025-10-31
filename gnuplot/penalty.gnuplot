# ================================================================
#  penalty.gnuplot
#  Simple bar plot for penalty.tsv
# ================================================================

reset

# ---------- OUTPUT SETTINGS ----------
set terminal pdfcairo size 3.35in,1in font "Times New Roman,12" linewidth 1.2
set output "penalty.pdf"

# ---------- GENERAL STYLE ----------
set style fill solid 1.2 border rgb "black"
set boxwidth 0.7 absolute
set style data boxes
set style line 1 lc rgb 'black' lt 1 lw 1.2
set grid ytics lc rgb "#888888" dt 2 lw 0.8

# Margins
set lmargin at screen 0.13
set rmargin at screen 0.975
set tmargin at screen 0.97
set bmargin at screen 0.28

set xlabel "Query" offset 0,0.75 font ",11"
set ylabel "Slowdown" offset 1,0 font ",11"
set ytics ("0x" 0, "50x" 50, "100x" 100, "150x" 150, "200x" 200)
set yrange [0:240]

set tics font ",11"
set tics nomirror

set xtics format ""
set xtics scale 0
set xtics offset 0,0.25

set ytics out
set ytics offset 0.5,0

set key top right
set key spacing 1.2
set key font ",11"

set border 3 front lt black linewidth 1.0

set offsets 0.2, 0, 0, 0

# ---------- COLOR PALETTE ----------
#color1 = "#4C78A8"  # blue
# gray
color1 = "#888888"
# ---------- DATA PREPARATION ----------
set datafile separator "\t"

# ---------- PLOT ----------
plot 'penalty.tsv' using 0:2:xtic(1) with boxes lc rgb color1 notitle, \
     '' using 0:2:(stringcolumn(2) eq "0" ? "" : sprintf("%.f",column(2))) with labels font ",11" offset char 0,0.55 center notitle

# ---------- POST ----------
unset output
print "✅ Plot success"