import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lower, col, count, concat_ws
from pyspark.sql.types import Row
from pyspark import RDD
from typing import List, NamedTuple, Tuple, Callable, Dict, Optional, Any
import random

def with_composite_key_column(df: DataFrame, columns: List[str], key_column_name: Optional[str] = None) -> Tuple[DataFrame, str]:
    """
    Add a column to the dataframe that is a concatenation of the values in the specified columns
    Used for sampling proportionately
    """
    if key_column_name is None:
        key_column_name = "_".join(columns)

    if key_column_name in df.columns:
        i: int = 0
        while f"{key_column_name}_{i}" in df.columns:
            i += 1
        key_column_name = f"{key_column_name}_{i}"
    
    return (df.withColumn(key_column_name, concat_ws("_", *[col(c) for c in columns])), key_column_name)