import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lower, col, count, concat_ws
from pyspark.sql.types import Row
from pyspark import RDD
from typing import List, NamedTuple, Dict

from paclib.util import with_composite_key_column

def sample_proportionately(df: DataFrame, 
                        columns_to_sample_by: List[str],
                        fraction: float,
                        **kwargs) -> DataFrame:
    # kwargs include seed, replacement, and other options for sampleBy

    df_keyed, key_column = with_composite_key_column(df, columns_to_sample_by)

    # create key column
    key_counts: DataFrame = df_keyed.groupBy(key_column).count()
    total_count: int = df_keyed.count()

    _key_fractions: List[Row] = (key_counts.withColumn("fraction", (col("count") / total_count) * fraction)
                                       .select(key_column, col("fraction"))
                                       .collect())
    key_fractions: Dict = {row[key_column]: row["fraction"] for row in _key_fractions}

    print(key_fractions)
    
    return df_keyed.sampleBy(key_column, fractions=key_fractions, **kwargs).drop(key_column)


class GaussianDistribution(NamedTuple):
    mean: float
    variance: float

def noise_to_add_parameters(avg_dist: float, c: float, max_mi: float) -> GaussianDistribution:
    """
    Returns the mean and variance of the Gaussian distribution used by `noise_to_add`
    """
    # noise_to_add_mean = 0  # always 0
    noise_to_add_variance = ((avg_dist + c) / (2*(max_mi / 2.)))  # taken from PAC-ML code

    return GaussianDistribution(0, noise_to_add_variance)

def noise_to_add(avg_dist: float, c: float, max_mi: float) -> float:
    """
    Returns a sample from a Gaussian distribution constructed from parameters
    """
    return np.random.normal(0, noise_to_add_parameters(avg_dist, c, max_mi).variance)