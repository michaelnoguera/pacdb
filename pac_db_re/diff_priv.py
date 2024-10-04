"""
Differential Privacy - Sample Laplace noise

We will add noise according to a Laplace Distribution
Y ∼ Lap(1/epsilon)
"""
import numpy as np
from scipy.stats import laplace
import matplotlib.pyplot as plt

# TODO: Confirm how to map epsilon from MI
epsilon = 1.0
delta = 1e-4
sensitivity = 1 # because it is a count query




