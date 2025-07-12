#!/usr/bin/env python
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "matplotlib",
#     "pyqt6",
# ]
# ///

# This file is lightly modified from the example script provided by hyperfine
# at https://github.com/sharkdp/hyperfine/blob/master/scripts/plot_whisker.py
# under the MIT license. See the end of this file for the original license text.

"""This program shows `hyperfine` benchmark results as a box and whisker plot.

Quoting from the matplotlib documentation:
    The box extends from the lower to upper quartile values of the data, with
    a line at the median. The whiskers extend from the box to show the range
    of the data. Flier points are those past the end of the whiskers.
"""

import argparse
import json

import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("file", help="JSON file with benchmark results")
parser.add_argument("--title", help="Plot Title")
parser.add_argument("--sort-by", choices=["median"], help="Sort method")
parser.add_argument(
    "--labels", help="Comma-separated list of entries for the plot legend"
)
parser.add_argument("-o", "--output", help="Save image to the given filename.")

args = parser.parse_args()

with open(args.file, encoding="utf-8") as f:
    results = json.load(f)["results"]

if args.labels:
    labels = args.labels.split(",")
else:
    labels = [b["command"] for b in results]
times = [b["times"] for b in results]

if args.sort_by == "median":
    medians = [b["median"] for b in results]
    indices = sorted(range(len(labels)), key=lambda k: medians[k])
    labels = [labels[i] for i in indices]
    times = [times[i] for i in indices]

plt.figure(figsize=(10, 6), constrained_layout=True)
#boxplot = plt.boxplot(times, vert=True, patch_artist=True)
boxplot = plt.boxplot(times, vert=True)

# cmap = plt.get_cmap("rainbow")
# colors = [cmap(val / len(times)) for val in range(len(times))]

# for patch, color in zip(boxplot["boxes"], colors):
#     patch.set_facecolor(color)

if args.title:
    plt.title(args.title)
#plt.legend(handles=boxplot["boxes"], labels=labels, loc="best", fontsize="medium")
plt.ylabel("Time [s]")
plt.ylim(0, None)
plt.xticks(list(range(1, len(labels) + 1)), labels, rotation=90)
if args.output:
    plt.savefig(args.output)
else:
    plt.show()


# MIT License

# Copyright (c) 2018-2022 David Peter, and all hyperfine contributors

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.