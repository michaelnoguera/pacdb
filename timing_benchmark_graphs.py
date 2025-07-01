#!/usr/bin/env python
# coding: utf-8

# In[3]:


import polars as pl
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib_inline.backend_inline
from matplotlib.rcsetup import cycler

#sns.reset_orig()

# Michael's matplotlib defaults
# set font to Times New Roman
LATEX = False
if LATEX:
    mpl.rcParams['text.usetex'] = True
    mpl.rcParams["font.family"] = "serif"
    mpl.rcParams["font.serif"] = "Times"
else:
    mpl.rcParams['text.usetex'] = False
    mpl.rcParams["font.family"] = "Times New Roman"
    mpl.rcParams["mathtext.fontset"] = "stix"
    
plt.rcParams['svg.fonttype'] = 'none'
mpl.rcParams['savefig.dpi'] = 300

matplotlib_inline.backend_inline.set_matplotlib_formats('svg')

mpl.rcParams['axes.titleweight'] = 'bold'

mpl.rcParams['legend.fancybox'] = False
mpl.rcParams['legend.frameon'] = False


colors = ['#a1c9f4', '#8de5a1', '#ff9f9b', '#d0bbff', '#fffea3', '#b9f2f0']
cmap = mpl.colors.ListedColormap(colors)
bar1 = {'color': colors[0], 'edgecolor': 'k'}
bar2 = {'color': colors[1], 'edgecolor': 'k'}
bar3 = {'color': colors[2], 'edgecolor': 'k'}


# In[16]:


# Load JSON file into Polars DataFrame

df = pl.read_json("benchmarks/merged.json")

# Get the 'query' column as list of labels
queries = df["query"].to_list()

# Get the step columns
step_columns = [col for col in df.columns if col.startswith("step")]

# Get values for each step column
step_values = [df[col].to_list() for col in step_columns]

# Bar positions
x = range(len(queries))

# Create the stacked bar plot
plt.figure(figsize=(8, 3))
bottom = [0] * len(queries)

step_columns = ["1. Run DuckDB Query over Samples", "2. Compute PAC Noise", "3. Reconstruct Table"]
for values, label, style in zip(step_values, step_columns, [bar1, bar2, bar3]):
    plt.bar(x, values, bottom=bottom, label=label, **style)
    bottom = [b + v for b, v in zip(bottom, values)]

# Final plot formatting
plt.xticks(x, queries, ha='right')
plt.xlabel('Query')
plt.ylabel('Time (seconds)')
plt.legend()
plt.tight_layout()
plt.show()


# In[17]:


unnoised = pl.read_json("unnoised/times.json")
# Get the 'query' column as list of labels
queries_unnoised = unnoised["query"].to_list()
# Just has a 'total' column for times
total_unnoised = unnoised["total"].to_list()
x = range(len(queries_unnoised))
# Create the bar plot for unnoised times
plt.figure(figsize=(8, 3))
plt.bar(x, total_unnoised, color=colors[0], edgecolor='k')
plt.xticks(x, queries_unnoised, ha='right')
plt.xlabel('Query')
plt.ylabel('Time (seconds)')
plt.tight_layout()
plt.show()


# In[ ]:




