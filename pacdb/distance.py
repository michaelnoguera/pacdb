"""
Distance calculation functions for measuring the stability of functions.

Here we consider the three possible cases: Yi is a single value, Yi is a vector,
and Yi is a dataframe with multiple columns.
"""

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

def vector_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Calculate the minimal pertubation distance between two vectors."""
    return float(np.linalg.norm(a - b))

def dataframe_distance(a: DataFrame, b: DataFrame) -> float:
    """Calculate the minimal pertubation distance between two dataframes."""
    # TODO
    return 0.0