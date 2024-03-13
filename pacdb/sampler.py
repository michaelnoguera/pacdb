from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union, overload
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, count, concat_ws
from pyspark.sql.types import StringType, Row
from pyspark.sql.column import Column
from typeguard import typechecked
from functools import wraps
from abc import ABC, abstractmethod
import tqdm

def with_composite_key_column(df: DataFrame, columns: List[str], key_column_name: Optional[str] = None) -> Tuple[DataFrame, str]:
    """
    Add a column to the dataframe that is a concatenation of the values in the specified columns
    Used for sampling proportionately
    Args:
        df (DataFrame): The dataset
        columns_to_sample_by (List[str]): Columns from the dataset df that are to be sampled
        key_column_name (str): Composite key

    Returns:
        Tuple: Dataframe with a new concatenated column and the column name
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
    """
    Method to sample the data using specific columns
    Args:
        df (DataFrame): The dataset
        columns_to_sample_by (List[str]): Columns from the dataset df that are to be sampled
        fraction (float)

    Returns:
        DataFrame: sampled dataframe
    """
    # kwargs include seed, replacement, and other options for sampleBy

    # creating a composite key in case of multiple columns
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
    columns_to_sample_by: Optional[List[str]] = None

class Sampler(ABC):
    """
    Samplers are used by the PACDataframe to sample data
    You can provide your own sampler by implementing this interface
    """

    @overload
    def __init__(self, df: Optional[DataFrame] = None):
        ...

    def __init__(self, df: Optional[DataFrame] = None, options: SamplerOptions = SamplerOptions()):
        """
        Sample with default sampling rate, replacement technique (with or without), and seed
        """
        self.df = df
        self.options = options  # defaults to 50% w/o replacement
        pass

    def withOptions(self, options: SamplerOptions) -> "Sampler":
        """
        Sample with provided sampling rate, replacement technique (with or without), and seed
        """
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
    """Implementation of Sampler methods"""

    def __init__(self, df: Optional[DataFrame] = None):
        super().__init__(df)

    def get_samples(self, df: DataFrame, user_defined_sampling_rate: Optional[float] = 0.5, query_filter: Optional[str] = None, aggr: Optional[Any] = None, trials: Optional[int] = 100) -> list:
        samples = []

        for i in tqdm(range(trials)):
            if query_filter is not None and aggr is not None:
                s = (df.sample()
                    .filter(query_filter)  # run query on sample
                    .agg(aggr))
            elif query_filter is None:
                s = (df.sample()
                    .agg(aggr))
            elif aggr is None:
                s = (df.sample()
                    .filter(query_filter))  # run query on sample

            samples.append(s)  # store result of query
        
        # TODO: change based on the aggregation being performed
        samples = [s * (1/user_defined_sampling_rate) for s in samples]  # so that counts are not halved

        return samples

    def sample(self) -> DataFrame:
        """
        Returns self DataFrame with default options
        """
        # Chai's Note: pyspark.sql.sample() does not guarantee Poisson i.i.d. sampling so we need to override or implement a custom function
        return self.df.sample(withReplacement=self.options.withReplacement, fraction=self.options.fraction, seed=self.options.seed)
    
    def sampleByColumns(self, cols: List[Union[Column, str]]) -> DataFrame:
        """
        Returns sampled data
        Args:
            cols (List[Union[Column, str]]): List of columns to be sampled

        Returns:
            DataFrame: the sampled DataFrame
        """
        return sample_proportionately(self.df, cols, self.options.fraction, seed=self.options.seed)
