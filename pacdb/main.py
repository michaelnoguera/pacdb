from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union, overload
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, count, concat_ws
from pyspark.sql.types import StringType, Row
from pyspark.sql.column import Column
from typeguard import typechecked
from functools import wraps
from abc import ABC, abstractmethod

from .sampler import Sampler, SamplerOptions, DataFrameSampler

# Remove additional spaces in name
def remove_extra_spaces(df: DataFrame, column_name: str) -> DataFrame:
    # Remove extra spaces from the specified column
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))

    return df_transformed


def add_attr(cls):
    """
    Decorator to attach a function to an attribute
    https://stackoverflow.com/a/59654133
    """
    def decorator(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            f = func(*args, **kwargs)
            return f

        setattr(cls, func.__name__, _wrapper)
        return func

    return decorator


def pac(self):
    """
    Custom functions to attach
    built on pattern from https://stackoverflow.com/a/59654133
    """
    @add_attr(pac)
    def to_pandasss():
        return self.toPandas()
    
    @add_attr(pac)
    def toPACDataFrame():
        return PACDataFrame(self)

    return pac

# add new property to the Class pyspark.sql.DataFrame
DataFrame.dataframe_extension = property(pac)

# use it
#df.dataframe_extension.add_column3().show()

class PACDataFrame:
    @overload
    def __init__(self, df: DataFrame):
        ...

    def __init__(self, df: DataFrame, sampler: Optional[DataFrameSampler] = None):
        self.df = df
        self.sampler: DataFrameSampler | None = None

    @classmethod
    def fromDataFrame(cls, df: DataFrame) -> "PACDataFrame":
        return cls(df)
    
    def toDataFrame(self) -> DataFrame:
        return self.df
    
    def withSampler(self, sampler: Optional[DataFrameSampler]) -> "PACDataFrame":
        """
        Attach a sampler to the dataframe, or create a new sampler over the dataframe
        """
        if sampler is not None:
            # provided a sampler, make sure it matches the dataframe
            if sampler.df is None:
                self.sampler = sampler.withDataFrame(self.df)
            elif sampler.df != self.df:
                raise ValueError("Sampler dataframe does not match dataframe")
            else: # sampler.df == self.df
                self.sampler = sampler
        else:
            # create a new sampler
            self.sampler = DataFrameSampler(self.df)
        return self

            
        
    
    def withNewSampler(self) -> "PACDataFrame":
        """
        Create a new sampler over this dataframe (with default options)
        """
        if self.sampler is None:
            self.sampler = DataFrameSampler(self.df)
        return self
     
    def withSamplerOptions(self, options: SamplerOptions) -> "PACDataFrame":
        """
        Set the sampling options for the attached sampler
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        self.sampler = self.sampler.withOptions(options)
        return self
    
    def withSamplerOption(self, option: str, value: Any) -> "PACDataFrame":
        """
        Set a single sampling option for the attached sampler
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        self.sampler = self.sampler.withOption(option, value)
        return self
    
    def sample(self) -> DataFrame:
        """
        Sample the dataframe based on the attached sampler. Use `withSampler` to attach a sampler.
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        return self.sampler.sample()
    
    def sampleByColumns(self, cols: List[Union[Column, str]]) -> DataFrame:
        """
        Sample the dataframe, enforcing the restriction that all categories in the specified columns
        must be evenly represented in the sample.

        Example:
        `pacdf.sampleByColumns(["column1"])` where column1 is a categorical column with options ["cats", "dogs"]
        will try to return a DataFrame with an equal number of "cats" and "dogs" in the sample.

        The proportions in the output may not be exact; the behavior of this function will match PySpark's
        sampleBy function.
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        return self.sampler.sampleByColumns(cols)

    def __getattr__(self, name):
        """
        Proxy all unmatched attribute calls to the underlying DataFrame
        """
        return getattr(self.df, name)
    


