from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Union, overload
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, count, concat_ws
from pyspark.sql.types import StringType, Row
from pyspark.sql.column import Column
from tqdm import tqdm
from typeguard import typechecked
from functools import wraps
from abc import ABC, abstractmethod

from .sampler import Sampler, SamplerOptions, DataFrameSampler

from paclib import noise_to_add, noise_to_add_parameters, GaussianDistribution

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

    def __init__(self, df: DataFrame, sampler: Optional[DataFrameSampler] = None):
        """
        Construct a new PACDataFrame from a PySpark DataFrame. Same as `fromDataFrame` but with additional optional parameters.
        """
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

        self.query: Callable[[DataFrame], Any] | None = None  # set by withQuery

        self.trials: int = 1000  # number of trials for PAC algorithm, will determine length of X and Y
        self.max_mi: float = 1./8  # maximum mutual information allowed for the query
        
        self.X: Optional[List[DataFrame]] = None  # set by _subsample
        self.Y: Optional[List[Any]] = None  # set by _measure

        self.avg_dist: Optional[float] = None  # set by _estimate_noise


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
    
    def withSamplerOption(self, option: str, value: Any) -> "PACDataFrame":
        """
        Set a single sampling option for the attached sampler
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        self.sampler = self.sampler.withOption(option, value)
        return self
    
    @property
    def samplerOptions(self) -> SamplerOptions:
        """
        Return the options of the attached sampler.
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        return self.sampler.options
    
    def _sample(self) -> DataFrame:
        """
        Take a single sample of the dataframe based on the attached sampler.
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        return self.sampler.sample()
    
    def _sampleByColumns(self, cols: List[str]) -> DataFrame:
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
    
    def withMutualInformationBound(self, max_mi: float) -> "PACDataFrame":
        """
        Sets `self.max_mi`, used by `_estimate_noise`.
        """
        self.max_mi = max_mi
        return self


    ### PAC algorithm ###

    def _subsample(self) -> "PACDataFrame":
        """
        Internal function.
        Calls `sample()` `trials` times to generate X.
        """
        X: List[DataFrame] = []
        
        for i in tqdm(range(self.trials * 2), desc="Subsample"):
            X.append(self._sample())

        self.X = X

    def _measure_stability(self) -> "PACDataFrame":
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

    def _estimate_noise(self) -> "PACDataFrame":
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
            for Y1, Y2 in self.Y_pairs:  # iterator is k
                # \psi^{(k)}=d_\pi( y^{(k,1)}, y^{(k,2)})
                # the minimal pertubation distance between scalars is the same as 1D vectors?
                avg_dist += abs(Y1 - Y2)
            avg_dist /= len(self.Y_pairs) # \bar\psi=\frac{\sum_{k=1}^{m} \psi_{\tau}^{(k)}}{m}
        elif distance_calculator == "all-pairs":
            # TODO: Here we assume that Yi is one-dimensional, meaning that distance is defined as abs(Yi - Yj)
            for Y1 in self.Y:
                min_dist = min([abs(Y1 - Y2) for Y2 in self.Y if Y1 != Y2])  # distance to closest neighbor
                avg_dist += min_dist
            avg_dist /= len(self.Y)

        self.avg_dist = avg_dist

        max_mi = self.max_mi
        c = 1
        print(noise_to_add_parameters(avg_dist, c, max_mi))
        noise = noise_to_add(avg_dist, c, max_mi)

    @property
    def Y_pairs(self) -> Optional[List[Tuple[Any, Any]]]:
        """
        Generator function so that Y only needs to be stored once but Y_pairs is still accessible.
        """
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
        pac_lung_df: PACDataFrame = PACDataFrame(lung_df)
               
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
    


