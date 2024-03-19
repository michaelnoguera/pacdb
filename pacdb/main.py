from dataclasses import dataclass
from typing import (Any, Callable, List, Optional, Tuple)

from pyspark.sql import DataFrame
from tqdm import tqdm

from pacdb.distance import minimal_permutation_distance

from .noise import GaussianDistribution, noise_to_add
from .sampler import DataFrameSampler, SamplerOptions


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
    from pacdb import PACDataFrame, SamplerOptions

    pac_lung_df = (PACDataFrame.fromDataFrame(lung_df)
                    .withSamplerOptions(
                        SamplerOptions(
                            withReplacement=False, 
                            fraction=0.5
                        )
                    ))
    ```
    """

    def __init__(self, df: DataFrame):
        """
        Construct a new PACDataFrame from a PySpark DataFrame. Use `fromDataFrame` instead.
        """
        self.df = df

        self.sampler: DataFrameSampler = DataFrameSampler(self.df)
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
    
    def withOptions(self, options: PACOptions) -> "PACDataFrame":
        """
        Set the PAC options for the dataframe.

        Example:
        ```
        pac_defaulters_df = (PACDataFrame(defaulters)
                .withOptions(
                    PACOptions(
                        trials = 50,
                        max_mi = 1/8,
                        c = 1e-6,
                        tau = 3
                    )
                )
                .withSamplerOptions(
                    SamplerOptions(
                        fraction=0.5
                    )
                ))
        ```
        """
        self.options = options
        return self

    ### Sampler methods ###

    def withSamplerOptions(self, options: SamplerOptions) -> "PACDataFrame":
        """
        Set the sampling options for the attached sampler
        """
        if self.sampler is None:
            raise ValueError("No sampler attached to this dataframe")
        self.sampler = self.sampler.withOptions(options)  # type: ignore  # TODO: fix abstract type error
        return self


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

    def _subsample(self, quiet=False) -> None:
        """
        Internal function.
        Calls `sample()` `trials` times to generate X.
        """
        X: List[List[DataFrame]] = []
        tau = self.options.tau

        assert tau >= 1, "tau must be at least 1, otherwise no samples will be taken"
        
        for i in tqdm(range(self.options.trials * 2), desc="Subsample", disable=quiet):
            # twice as many because we will use them in pairs
            # for each trial, take `tau` samples
            X.append([self.sampler.sample() for _ in range(tau)])

        self.X = X

    def _measure_stability(self, quiet=False) -> None:
        """
        Internal function.
        Applies `self.query` to each X to generate Y. Sets the Y (and by extension Y_pairs) instance variables.
        """

        assert self.X is not None, "Must call _subsample() before _measure()"

        Y: List[List[int|float]] = []
        
        for Xi in tqdm(self.X, desc="Measure Stability", disable=quiet):
            Yi = [self._applyQuery(Xit) for Xit in Xi]
            Y.append(Yi)

        self.Y = Y

    def _estimate_noise(self, mi: Optional[float] = None, c: Optional[float] = None, quiet=False) -> None:
        """
        Internal function.
        Estimates the noise needed to privatize the query based on the minimal permutation distance between Y entries.
        Sets `self.avg_dist`.
        """

        assert self.Y is not None, "Must call _measure() before _estimate_noise()"
        mi = self.options.max_mi if mi is None else mi
        assert mi is not None, "Must set withMutualInformationBound() before _estimate_noise() or provide argument"
        tau = self.options.tau

        avg_dist: float = 0.

        for Y1, Y2 in tqdm(self.Y_pairs, desc="Measure Distances", disable=quiet):
            avg_dist += minimal_permutation_distance(Y1, Y2)

        assert self.Y_pairs is not None
        avg_dist /= float(len(self.Y_pairs))  # \bar\psi=\sum_{k=1}^{m} \psi_{\tau}^{(k)} / {m}

        self.avg_dist = avg_dist

        c = self.options.c if c is None else c
        self.noise_distribution = noise_to_add(avg_dist, c, mi)
        #noise = noise_to_add(avg_dist, c, mi).sample()

    def _noised_release(self, noise_distribution: Optional[GaussianDistribution] = None) -> Any:
        """
        Internal function.
        After _subsample, _measure_stability, and _estimate_noise, this function can be called to release
        the query result with PAC privacy.
        """
        nd = noise_distribution if noise_distribution is not None else self.noise_distribution
        assert nd is not None, "Must call _estimate_noise() before _noised_release() or provide argument"

        Xj: DataFrame = self.sampler.sample()
        Yj = self._applyQuery(Xj)
        delta = nd.sample()

        return Yj + delta

    def releaseValue(self, quiet=False) -> Any:
        """
        Execute the query with PAC privacy.
        """
        self._subsample(quiet=quiet)
        self._measure_stability(quiet=quiet)
        self._estimate_noise(quiet=quiet)
        return self._noised_release()


    ### Utility methods ###

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
    

    ### Query methods ###

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
