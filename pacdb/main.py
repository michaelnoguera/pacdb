'''
Contains logic for custom PACDataFrame object
'''

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Union, overload
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, count, concat_ws
from pyspark.sql.types import StringType, Row
from pyspark.sql.column import Column
from typeguard import typechecked
from functools import wraps
from abc import ABC, abstractmethod

from .sampler import Sampler, SamplerOptions, DataFrameSampler


class PACDataFrame:
    """A custom PACDataFrame class"""

    @overload
    def __init__(self, df: DataFrame):
        """
        Create a PACDataFrame from a PySpark DataFrame to use PAC-private functions.
        A new PACDataFrame will have a new DataFrameSampler attached to it.
        """
        ...

    def __init__(self, df: DataFrame, sampler: Optional[DataFrameSampler] = None):
        self.df = df

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

        self.predicate: Callable[[DataFrame], Any] = None

    @classmethod
    def fromDataFrame(cls, df: DataFrame) -> "PACDataFrame":
        return cls(df)
    
    def toDataFrame(self) -> DataFrame:
        # TODO: add computed noise to one sample and release only that
        return self.df
     
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
        Take a single sample of the dataframe based on the attached sampler.
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        return self.sampler.sample()
    
    def sampleByColumns(self, cols: List[Union[Column, str]]) -> DataFrame:
        """
        Take a single sample of the dataframe, enforcing the restriction that all categories in the specified
        columns must be evenly represented in the sample.

        Example:
        `pacdf.sampleByColumns(["column1"])` where column1 is a categorical column with options ["cats", "dogs"]
        will try to return a DataFrame with an equal number of "cats" and "dogs" in the sample.

        The proportions in the output may not be exact; the behavior of this function will match PySpark's
        sampleBy function.
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        return self.sampler.sampleByColumns(cols)

    def _n(self) -> int:
        """
        Return the exact number of rows in the underlying dataframe, for use in PAC algorithm. This is 
        privacy-sensitive and should not be used to release information about the underlying dataframe!
        """
        return self.df.count()
    
    def withPredicate(self, predicate: Callable[[DataFrame], Any]) -> "PACDataFrame":
        """
        Set the predicate function to be made private.
        """
        self.predicate = predicate
        return self
    
    def _applyPredicate(self, df: DataFrame) -> Any:
        """
        Directly apply the predicate to the given dataframe and return the exact output. This is not private at all!
        """
        if self.predicate is None:
            return df
        return self.predicate(df)
    
    # def count(self, *args, **kwargs):
    #     s = self.sample()
    #     correction_factor = 1. / self.sampler.options.fraction
    #     return s.count() * correction_factor
    
    # def __getattr__(self, name):
    #     """
    #     Proxy all unmatched attribute calls to the underlying DataFrame
    #     """
    #     return getattr(self.df, name)
    


