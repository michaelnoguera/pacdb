"""
Distance calculation functions for measuring the stability of functions.

Here we consider the three possible cases: Yi is a single value, Yi is a vector,
and Yi is a dataframe with multiple columns.
"""

import itertools
from typing import TypeVar, Union, List
from typing_extensions import Protocol
from pyspark.sql import DataFrame, GroupedData, SparkSession
import numpy as np

def value_distance(a: float | int | bool, b: float | int | bool) -> float | int:
    """Calculate the distance between two single values."""
    if isinstance(a, bool) and isinstance(b, bool):
        return 0 if a == b else 1
    else:
        return abs(a - b)

def minimal_permutation_distance(a, b) -> float:
    """
    $$d_{\pi}(a, b) = \min_{\pi} \sum_{j} \left\|a(j) - b(\pi(j))\right\|^2 / k$$
    """
    #assert len(a) == len(b), "The two vectors must have the same length (tau)"

    # From when type(a) == List[np.ndarray]
    # k = len(a) # = tau
    # min_distance = float('inf')
    # distance = lambda pi: float(sum(np.linalg.norm(a[j] - b[pi[j]]) ** 2 / k for j in range(k)))
    # min_distance = min(distance(pi) for pi in itertools.permutations(range(k)))  # Permutations on the block index [1:k]
    
    return float(np.linalg.norm(a - b))

def vector_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Calculate the minimal permutation distance between two vectors."""
    return float(np.linalg.norm(a - b))

def dataframe_distance(a: DataFrame, b: DataFrame) -> float:
    """Calculate the minimal permutation distance between two dataframes."""
    # TODO
    return 0.0