import csv
import os
from typing import Dict, Generator, List

import numpy as np
import pandas as pd
from pyspark.ml.linalg import DenseVector, Vectors

from pacdb import PACDataFrame

query_name: str = "count"
budget_list: List[float] = [1/64, 1/32, 1/16, 1/8, 1/4, 1/2, 1., 2., 4.]
sample_size: int = 3
sampling_rate: float = 0.5
m: int = 10
c: float = 1e-6
mi: float = 1./4

# We want to read from raw_samples_count.csv as if it were a generator
INPUT_FILE = 'raw_samples_count.csv'  # has header row, ignore
presampled = csv.reader(open("raw_samples_count.csv", "r"))
next(presampled)  # skip header

def presampled_generator():
    """
    Generator pretending to run queries but actually reading from file. Every
    row is converted to a numpy array before being returned.
    """
    for row in presampled:
        yield np.array(row, dtype=float)

# Hybrid Noise
hybrid_sampler = presampled_generator()

HYBRID_PATH = "noise_comparison/hybrid.csv"
if os.path.exists(HYBRID_PATH):
    os.remove(HYBRID_PATH)

with open(HYBRID_PATH, "a") as f:
    f.write("mi,noise,sqrt_total_var,variances,mean\n")

    try:
        for _ in range(5):
            for mi in budget_list:
                out = PACDataFrame.estimate_hybrid_noise_static(sample_once=hybrid_sampler.__next__, max_mi=mi, anisotropic=False)
                f.write(f"{mi},{out[0]},{out[1][0]},{out[1][1]},{out[1][2]}\n")
    except StopIteration:
        pass

# Anisotropic Noise
anisotropic_sampler = presampled_generator()

ANISOTROPIC_PATH = "noise_comparison/anisotropic.csv"
if os.path.exists(ANISOTROPIC_PATH):
    os.remove(ANISOTROPIC_PATH)

with open(ANISOTROPIC_PATH, "a") as f:
    f.write("mi,noise,sqrt_total_var,variances,mean\n")

    try:
        for _ in range(5):
            for mi in budget_list:
                out = PACDataFrame.estimate_hybrid_noise_static(sample_once=anisotropic_sampler.__next__, max_mi=mi, anisotropic=True)
                f.write(f"{mi},{out[0]},{out[1][0]},{out[1][1]},{out[1][2]}\n")
    except StopIteration:
        pass

# Process evaluated data points to compute average values for plotting

#categories = ['Father Avg', 'Mother Avg', 'Other Avg', 'Father Max', 'Mother Max', 'Other Max']
categories = ['Father Avg', 'Father Max', 'Mother Avg', 'Mother Max', 'Other Avg', 'Other Max']

# Anisotropic

with open(ANISOTROPIC_PATH, 'r') as f:
    f.readline()
    data_anisotropic = f.read().split('\n')[:-1]
    data_anisotropic = [eval(d) for d in data_anisotropic]

# extract to list of dataframes
dfs_anisotropic: Dict[str, List] = {c: [] for c in categories}
for i, c in enumerate(categories):
    cat_data = []
    for d in data_anisotropic:
        cat_data += [d[0], d[1][i], d[2], d[3][i], d[4][i]]
    dfs_anisotropic[c] = pd.DataFrame(np.array(cat_data).reshape(len(data_anisotropic), 5), columns=['mi', 'noise', 'sqrt_total_var', 'variances', 'mean'])

for c in categories:
    if os.path.exists('noise_comparison/anisotropic_{c}.pkl'):
        os.remove('noise_comparison/anisotropic_{c}.pkl')
    dfs_anisotropic[c].to_pickle(f'noise_comparison/anisotropic_{c}.pkl')

# Hybrid

with open(HYBRID_PATH, 'r') as f:
    f.readline()
    data_hybrid = f.read().split('\n')[:-1]
    data_hybrid = [eval(d) for d in data_hybrid]

# extract to list of dataframes
dfs_hybrid = {c: [] for c in categories}
for i, c in enumerate(categories):
    cat_data = []
    for d in data_hybrid:
        cat_data += [d[0], d[1][i], d[2], d[3][i], d[4][i]]
    dfs_hybrid[c] = pd.DataFrame(np.array(cat_data).reshape(len(data_hybrid), 5), columns=['mi', 'noise', 'sqrt_total_var', 'variances', 'mean'])

for c in categories:
    if os.path.exists('noise_comparison/hybrid_{c}.pkl'):
        os.remove('noise_comparison/hybrid_{c}.pkl')
    dfs_hybrid[c].to_pickle(f'noise_comparison/hybrid_{c}.pkl')