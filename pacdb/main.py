from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import cache
from typing import (Any, Callable, Dict, List, NamedTuple, Optional, Tuple,
                    Union, overload)

import numpy as np
from pyspark.sql import DataFrame, GroupedData, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import col, concat_ws, count, lower, regexp_replace
from pyspark.sql.types import Row, StringType
from tqdm import tqdm

from pacdb.distance import minimal_permutation_distance, value_distance

from .noise import GaussianDistribution, noise_to_add
from .sampler import DataFrameSampler, Sampler, SamplerOptions


@dataclass
class PACOptions:
    """
    Options for the PAC algorithm.
    """
    trials: int = 1000
    """number of trials for PAC algorithm, will determine length of X and Y"""
    max_mi: float = 1./8
    """maximum mutual information allowed for the query"""
    c: float = 0.001
    """security parameter, lower bound for noise added"""
    tau: int = 3
    """security parameter, number of samples to use for minimal-permutation distance"""



class PACDataFrame:
    """
    Create a PACDataFrame from a PySpark DataFrame to use PAC-private functions.
    A new PACDataFrame will have a new DataFrameSampler attached to it.

    Example:
    ```
    from pacdb import PACDataFrame, Sampler, DataFrameSampler, SamplerOptions

    pac_lung_df = (PACDataFrame(lung_df)
                    .withSamplerOptions(
                        SamplerOptions(
                            withReplacement=False, 
                            fraction=0.5
                        )
                    ))

    pac_lung_df.sample().toPandas().head()
    ```
    """

    def __init__(self, df: DataFrame):
        """
        Construct a new PACDataFrame from a PySpark DataFrame. Same as `fromDataFrame` but with additional optional parameters.
        """
        self.df = df

        self.sampler = DataFrameSampler(self.df)
        self.options = PACOptions()

        self.query: Callable[[DataFrame], Any] | None = None  # set by withQuery
        
        self.X: Optional[List[List[DataFrame]]] = None  # set by _subsample
        """X contains samples of the dataframe, in sets of `tau`. `len(X) = trials`. Set by `_subsample`."""
        self.Y: Optional[List[List[Any]]] = None  # set by _measure

        self.avg_dist: Optional[float] = None  # set by _estimate_noise
        self.noise_distribution: Optional[GaussianDistribution] = None  # set by _estimate_noise


    @classmethod
    def fromDataFrame(cls, df: DataFrame) -> "PACDataFrame":
        """
        Create a PACDataFrame from an existing Spark DataFrame.
        """
        return cls(df)
    
    def toDataFrame(self) -> DataFrame:
        # TODO: add computed noise to one sample and release only that
        return self.df
    

    ### Sampler methods ###

    def withSamplerOptions(self, options: SamplerOptions) -> "PACDataFrame":
        """
        Set the sampling options for the attached sampler
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        self.sampler = self.sampler.withOptions(options)
        return self
    
    @property
    def samplerOptions(self) -> SamplerOptions:
        """
        Return the options of the attached sampler.
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        return self.sampler.options

    @property
    def sampling_rate(self) -> float:
        """
        Return the sampling rate of the attached sampler.
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        return self.sampler.options.fraction
    

    ### PAC inputs ###

    def setNumberOfTrials(self, trials: int) -> "PACDataFrame":
        """
        Set the number of trials to be used by the PAC algorithm. This is used to compute the privacy
        guarantee, and should be set to a large number for accurate results.
        """
        self.trials = trials
        return self
    
    def setMutualInformationBound(self, max_mi: float) -> "PACDataFrame":
        """
        Sets `self.max_mi`, used by `_estimate_noise`.
        """
        self.max_mi = max_mi
        return self


    ### PAC algorithm ###
    @cache
    def _subsample(self) -> None:
        """
        Internal function.
        Calls `sample()` `trials` times to generate X.
        """
        X: List[List[DataFrame]] = []
        tau = self.options.tau

        assert tau >= 1, "tau must be at least 1, otherwise no samples will be taken"
        
        for i in tqdm(range(self.trials * 2), desc="Subsample"):
            # twice as many because we will use them in pairs
            # for each trial, take `tau` samples
            X.append([self.sampler.sample() for _ in range(tau)])

        self.X = X

    @cache
    def _measure_stability(self) -> None:
        """
        Internal function.
        Applies `self.query` to each X to generate Y. Sets the Y (and by extension Y_pairs) instance variables.
        """

        assert self.X is not None, "Must call _subsample() before _measure()"

        Y: List[List[int|float]] = []
        
        for Xi in tqdm(self.X, desc="Measure Stability"):
            Yi = [self._applyQuery(Xit) for Xit in Xi]
            Y.append(Yi)

        self.Y = Y

    def _estimate_noise(self, mi: Optional[float] = None, c: Optional[float] = None) -> None:
        """
        Internal function.
        Estimates the noise needed to privatize the query based on the minimal permutation distance between Y entries.
        Sets `self.avg_dist`.
        """

        assert self.Y is not None, "Must call _measure() before _estimate_noise()"
        mi = self.max_mi if mi is None else mi
        assert mi is not None, "Must set withMutualInformationBound() before _estimate_noise() or provide argument"
        tau = self.options.tau
        
        avg_dist = 0

        for Y1, Y2 in tqdm(self.Y_pairs, desc="Measure Distances"):
            avg_dist += minimal_permutation_distance(Y1, Y2)

        avg_dist /= len(self.Y_pairs)  # \bar\psi=\sum_{k=1}^{m} \psi_{\tau}^{(k)} / {m}

        self.avg_dist = avg_dist

        c = self.options.c if c is None else c
        self.noise_distribution = noise_to_add(avg_dist, c, mi)
        #noise = noise_to_add(avg_dist, c, mi).sample()

    def _noised_release(self, noise_distribution: Optional[GaussianDistribution] = None) -> Any:
        Yj = self._applyQuery(self.sampler.sample())

        nd = self.noise_distribution if noise_distribution is None else noise_distribution
        noise_to_add = nd.sample()
        noised_Yj = Yj + noise_to_add
        return noised_Yj

    def releaseValue(self) -> Any:
        """
        Execute the query with PAC privacy.
        """
        self._subsample()
        self._measure_stability()
        self._estimate_noise()
        return self._noised_release()


    @property
    def Y_pairs(self) -> Optional[List[Tuple[Any, Any]]]:
        """ Generator function so that Y only needs to be stored once but Y_pairs is still accessible. """
        if self.Y is None:
            return None
        return list(zip(self.Y[::2], self.Y[1::2]))

    def _n(self) -> int:
        """
        Return the exact number of rows in the underlying dataframe, for use in PAC algorithm. This is 
        privacy-sensitive and should not be used to release information about the underlying dataframe!
        """
        return self.df.count()
    
    def withQuery(self, query_function: Callable[[DataFrame], Any]) -> "PACDataFrame":
        """
        Set the query function to be made private.

        Example:
        ```
        pac_lung_df: PACDataFrame = PACDataFrame.fromDataFrame(lung_df)
               
        # Define your query as a function
        def A(x: DataFrame) -> int:
            y = (x.filter(lung_df["Smoking"] >= 3)
                    .count())
            return y

        # Attach the query function to the PACDataFrame
        pac_lung_df = pac_lung_df.withQuery(A)
        ```
        """
        self.query = query_function
        return self
    
    def _applyQuery(self, df: DataFrame) -> Any:
        """
        Directly apply the query to the given dataframe and return the exact output. This is not private at all!
        """
        if self.query is None:
            return df
        return self.query(df)
    
    # def __getattr__(self, name):
    #     """
    #     Proxy all unmatched attribute calls to the underlying DataFrame
    #     """
    #     return getattr(self.df, name)
    


class DataFrameWrapper:
    """
    PAC wrapper around PySpark DataFrame.
    """

    def __init__(self, df: DataFrame, ctx: SparkSession) -> None:
        """
        Construct a new DataFrameWrapper wrapping a PySpark DataFrame. Must be used within a PACSession context.

        Args:
        - df: the PySpark DataFrame to wrap
        - ctx: the PACSession context to use

        Do not use this function directly; use `PACSession.createDataFrame` instead.
        """
        self.df = df
        self.ctx = ctx
    
    def toDataFrame(self) -> DataFrame:
        return self.df
    
    def isEmpty(self) -> bool:
        return self.df.isEmpty()
    
    def count(self) -> int:
        return self.df.count()
    
    def show(self, n: int = 20, truncate: bool = True) -> None:
        self.df.show(n, truncate)

    def collect(self) -> List[Row]:
        return self.df.collect()
    
    def limit(self, num: int) -> "DataFrameWrapper":
        return DataFrameWrapper(self.df.limit(num), self.ctx)
    
    def filter(self, condition: Column | str) -> "DataFrameWrapper":
        """Spark wrapper. Filters rows using the given condition."""
        return DataFrameWrapper(self.df.filter(condition), self.ctx)
    
    def select(self, *cols: List[Column] | List[str]) -> "DataFrameWrapper":
        """Spark wrapper. Projects a set of expressions and returns a new DataFrameWrapper."""
        return DataFrameWrapper(self.df.select(*cols), self.ctx)
    
    def withColumnRenamed(self, existing: str, new: str) -> "DataFrameWrapper":
        """Spark wrapper. Renames a column."""
        return DataFrameWrapper(self.df.withColumnRenamed(existing, new), self.ctx)
    
    def groupBy(self, *cols: List[Column] | List[str]) -> "GroupedDataWrapper":
        """Spark wrapper. Groups the DataFrame using the specified columns."""
        return GroupedDataWrapper(self.df.groupBy(*cols), self.ctx)
    
    def drop(self, *cols: List[str]) -> "DataFrameWrapper":
        return DataFrameWrapper(self.df.drop(*cols), self.ctx)


class GroupedDataWrapper:
    """
    PAC wrapper around PySpark GroupedData.
    Implements the same methods as PySpark GroupedData, but returns DataFrameWrappers instead of DataFrames.
    """

    def __init__(self, gd: GroupedData, pacdf: DataFrameWrapper):
        self.gd = gd
        self.pacdf = pacdf
        self._ctx = pacdf.ctx

    def count(self) -> DataFrameWrapper:
        return DataFrameWrapper(self.gd.count(), self._ctx)
    
    def mean(self, *cols: List[str]) -> DataFrameWrapper:
        return DataFrameWrapper(self.gd.mean(*cols), self._ctx)
    
    def avg(self, *cols: List[str]) -> DataFrameWrapper:
        return DataFrameWrapper(self.gd.avg(*cols), self._ctx)
    
    def max(self, *cols: List[str]) -> DataFrameWrapper:
        return DataFrameWrapper(self.gd.max(*cols), self._ctx)
    
    def min(self, *cols: List[str]) -> DataFrameWrapper:
        return DataFrameWrapper(self.gd.min(*cols), self._ctx)
    

    def sum(self, *cols: List[str]) -> DataFrameWrapper:
        return DataFrameWrapper(self.gd.sum(*cols), self._ctx)
    
    def pivot(self, pivot_col: str, values: Optional[List[Union[bool, float, int, str]]]) -> DataFrameWrapper:
        if values is None:
            return GroupedDataWrapper(self.gd.pivot(pivot_col), self.pacdf)
        else:
            return GroupedDataWrapper(self.gd.pivot(pivot_col, values), self.pacdf)