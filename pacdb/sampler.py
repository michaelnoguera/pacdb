from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union, overload
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, count, concat_ws
from pyspark.sql.types import StringType, Row
from pyspark.sql.column import Column
from typeguard import typechecked
from functools import wraps
from abc import ABC, abstractmethod

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

def sample_proportionately(df: DataFrame, 
                        columns_to_sample_by: List[str],
                        fraction: float,
                        **kwargs) -> DataFrame:
    # kwargs include seed, replacement, and other options for sampleBy

    df_keyed, key_column = with_composite_key_column(df, columns_to_sample_by) # create key column

    # establish the fraction of each label in the dataset
    key_counts: DataFrame = df_keyed.groupBy(key_column).count()
    total_count: int = df_keyed.count()

    _key_fractions: List[Row] = (key_counts.withColumn("fraction", (col("count") / total_count) * fraction)
                                       .select(key_column, col("fraction"))
                                       .collect())
    key_fractions: Dict = {row[key_column]: row["fraction"] for row in _key_fractions}

    print(key_fractions)
    
    # attempt sampling to match the given fractions
    return df_keyed.sampleBy(key_column, fractions=key_fractions, **kwargs).drop(key_column)

@dataclass
class SamplerOptions():
    withReplacement: Optional[Union[float, bool]] = False
    fraction: Optional[Union[int, float]] = 0.5
    seed: Optional[int] = None
    columns_to_sample_by: List[str] | None = None

class Sampler(ABC):
    """
    Samplers are used by the PACDataframe to sample data
    You can provide your own sampler by implementing this interface
    """

    @overload
    def __init__(self, df: Optional[DataFrame] = None):
        ...

    def __init__(self, df: Optional[DataFrame] = None, options: SamplerOptions = SamplerOptions()):
        self.df = df
        self.options = options  # defaults to 50% w/o replacement
        pass

    def withOptions(self, options: SamplerOptions) -> "Sampler":
        self.options = options
        return self

    def withOption(self, option: str, value: Any) -> "Sampler":
        if option not in self.options.__dict__:  # check if the option exists
            raise ValueError(f"Option {option} does not exist")
        setattr(self.options, option, value)
        return self
    
    def withDataFrame(self, df: DataFrame) -> "Sampler":
        self.df = df
        return self

    @abstractmethod
    def sample() -> "DataFrame":
        pass
    
    @abstractmethod
    def sampleByColumns(
        self, 
        cols: List[Union[Column, str]], 
    ) -> "DataFrame":
        pass


class DataFrameSampler(Sampler):
    def __init__(self, df: Optional[DataFrame] = None):
        super().__init__(df)

    def sample(self) -> DataFrame:
        return self.df.sample(withReplacement=self.options.withReplacement, fraction=self.options.fraction, seed=self.options.seed)
    
    def sampleByColumns(self, cols: List[Union[Column, str]]) -> DataFrame:
        return sample_proportionately(self.df, cols, self.options.fraction, seed=self.options.seed)
