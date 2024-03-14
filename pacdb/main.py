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
    """security parameter? TODO: meaning"""

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
        
        self.X: Optional[List[DataFrame]] = None  # set by _subsample
        self.Y: Optional[List[Any]] = None  # set by _measure

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
        X: List[DataFrame] = []
        
        for i in tqdm(range(self.trials * 2), desc="Subsample"):
            X.append(self.sampler.sample())

        self.X = X

    @cache
    def _measure_stability(self) -> None:
        """
        Internal function.
        Applies `self.query` to each X to generate Y. Sets the Y (and by extension Y_pairs) instance variables.
        """

        assert self.X is not None, "Must call _subsample() before _measure()"

        Y: list[int] = []
        
        for Xi in tqdm(self.X, desc="Measure Stability"):
            Yi = self._applyQuery(Xi)
            # TODO solve correction factor here: Yi = Yi * (1/self.df.sampling_rate)
            Y.append(Yi)

        self.Y = Y

    def _estimate_noise(self) -> None:
        """
        Internal function.
        Estimates the noise needed to privatize the query based on the minimal pertubation distance between Y entries.
        Sets `self.avg_dist`.
        """

        assert self.Y is not None, "Must call _measure() before _estimate_noise()"
        assert self.max_mi is not None, "Must set withMutualInformationBound() before _estimate_noise()"

        avg_dist = 0
        distance_calculator = "all-pairs"

        if distance_calculator == "within-pairs":
            # Seems most consistent with Algorithm 2?
            for Y1, Y2 in tqdm(self.Y_pairs, desc="Measure Distances"):  # iterator is k
                # \psi^{(k)}=d_\pi( y^{(k,1)}, y^{(k,2)})
                # the minimal pertubation distance between scalars is the same as 1D vectors?
                avg_dist += abs(Y1 - Y2)
            avg_dist /= len(self.Y_pairs) # \bar\psi=\frac{\sum_{k=1}^{m} \psi_{\tau}^{(k)}}{m}
        elif distance_calculator == "all-pairs":
            # TODO: Here we assume that Yi is one-dimensional, meaning that distance is defined as abs(Yi - Yj)
            for Y1 in tqdm(self.Y, desc="Measure Distances"):
                min_dist = min([abs(Y1 - Y2) for Y2 in self.Y if Y1 != Y2])  # distance to closest neighbor
                avg_dist += min_dist
            avg_dist /= len(self.Y)

        self.avg_dist = avg_dist

        c, max_mi = self.options.c, self.options.max_mi
        self.noise_distribution = noise_to_add(avg_dist, c, max_mi)
        print(noise_to_add(avg_dist, c, max_mi))
        noise = noise_to_add(avg_dist, c, max_mi).sample()

    def _noised_release(self):
        Yj = self._applyQuery(self.sampler.sample())

        noise_to_add = self.noise_distribution.sample()
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
    


