from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from typing_extensions import Protocol
from collections.abc import Callable

import numpy as np
from pyspark.sql import DataFrame, Column
import sklearn  # type: ignore
from tqdm import tqdm

from pacdb.distance import minimal_permutation_distance

from .noise import GaussianDistribution, noise_to_add
from .sampler import DataFrameSampler, SamplerOptions

class QueryFunction(Protocol):
    """
    Any function that takes as input a `pyspark.sql.DataFrame` and returns a
    `pyspark.sql.DataFrame`.

    Example:
    ```
    def query(df):
        return df.filter(df["absences"] >= 5).agg(F.count("*"))
    ```
    """
    def __call__(self, df: DataFrame) -> DataFrame: ...

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

        self.query: QueryFunction | None = None  # set by withQuery
        
        self.X: Optional[List[DataFrame]] = None  # set by _subsample
        """X contains samples of the dataframe. `len(X) = trials`. Set by `_subsample`."""
        self.Y: Optional[List[DataFrame]] = None  # set by _measure

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
        Calls `sample()` `options.trials` times to generate X.
        """
        X: List[DataFrame] = []
        
        for _ in tqdm(range(self.options.trials * 2), desc="Subsample", disable=quiet):
            # twice as many because we will use them in pairs
            X.append(self.sampler.sample())

        self.X = X

    def _measure_stability(self, quiet=False) -> None:
        """
        Internal function.
        Applies `self.query` to each X to generate Y. Sets the Y (and by extension Y_pairs) instance variables.
        """

        assert self.X is not None, "Must call _subsample() before _measure()"

        Y: List[DataFrame] = []
        
        # Y = [self._applyQuery(Xi) for Xi in self.X]
        for Xi in tqdm(self.X, desc="Measure Stability", disable=quiet):
            Yi = self._applyQuery(Xi)
            Y.append(Yi)

        self.Y = Y

    def _produce_one_sampled_output(self) -> np.ndarray:
        X: DataFrame = self.sampler.sample()
        Y: DataFrame = self._applyQuery(X)
        output: np.ndarray = self._unwrapDataFrame(Y)
        return output
    
    def _estimate_hybrid_noise(
        self,
        max_mi: Optional[float] = None,
        ) -> Tuple[np.ndarray, List[float]]:

        if max_mi is None:  # optional argument, otherwise use PACDataFrame setting
            max_mi = self.options.max_mi

        eta: float = 0.01  # convergence threshold  # TODO what should eta be?

        # r = max([np.linalg.norm(x) for x in train_x])

        # We do not need to create a new set of ordered keys to 'canonicalize' the data, because
        # the data is already in a consistent column order.

        # Use the identity matrix for our projection matrix
        dimensions = len(self._produce_one_sampled_output())
        proj_matrix: np.ndarray = np.eye(dimensions)

        # projected samples used to estimate variance in each basis direction
        # est_y[i] is a list of magnitudes of the outputs in the i-th basis direction
        est_y: List[List[np.ndarray]] = [[] for _ in range(dimensions)]
        prev_ests: List[np.floating[Any]] | None = None # to measure change per iteration for convergence

        converged = False
        curr_trial = 0
        while not converged:
            output: np.ndarray = self._produce_one_sampled_output()
            assert len(output) == dimensions

            # Compute the magnitude of the output in each of the basis directions, update the estimate lists
            for i in range(len(output)):
                est_y[i].append(np.matmul(proj_matrix[i].T, output.T))

            # Every 10 trials, check for convergence
            if curr_trial % 10 == 0:        
                if prev_ests is None:
                    # During the first iteration, there are no previous estimates, so set and continue
                    prev_ests = [np.var(est_y[i]) for i in range(dimensions)]
                else:
                    # If all dimensions' variance estimates changed by less than eta, we have converged
                    if all(abs(np.var(est_y[i]) - prev_ests[i]) <= eta for i in range(dimensions)):
                        converged = True
                    else:
                        # we have not converged, so update the previous estimates and continue
                        prev_ests = [np.var(est_y[i]) for i in range(dimensions)]
            curr_trial += 1

        # Now that we have converged, get the variance in each basis direction
        fin_var: List[np.floating[Any]] = [np.var(est_y[i]) for i in range(dimensions)]

        sqrt_total_var = sum(fin_var)**0.5
        print(f'sqrt total var is {sqrt_total_var}')

        noise = [np.inf for _ in range(dimensions)]
        for i in range(dimensions):
            noise[i] = 1./max_mi**0.5 * fin_var[i]**0.5 * sqrt_total_var

        return proj_matrix, noise

    def _estimate_noise(self, mi: Optional[float] = None, c: Optional[float] = None, quiet=False) -> None:
        """
        Internal function.
        Estimates the noise needed to privatize the query based on the minimal permutation distance between Y entries.
        Sets `self.avg_dist`.
        """

        assert self.Y is not None, "Must call _measure() before _estimate_noise()"
        mi = self.options.max_mi if mi is None else mi
        assert mi is not None, "Must set withMutualInformationBound() before _estimate_noise() or provide argument"


        avg_dist: float = 0.

        for Y1, Y2 in tqdm(self.Y_pairs, desc="Measure Distances", disable=quiet):
            avg_dist += minimal_permutation_distance(Y1, Y2)

        assert self.Y_pairs is not None
        avg_dist /= float(len(self.Y_pairs))  # \bar\psi=\sum_{k=1}^{m} \psi_{\tau}^{(k)} / {m}

        self.avg_dist = avg_dist

        c = self.options.c if c is None else c
        self.noise_distribution = noise_to_add(avg_dist, c, mi)

    def _noised_release(self, noise_distribution: Optional[GaussianDistribution] = None) -> Any:
        """
        Internal function.
        After _subsample, _measure_stability, and _estimate_noise, this function can be called to release
        the query result with PAC privacy.
        """
        nd = noise_distribution if noise_distribution is not None else self.noise_distribution
        assert nd is not None, "Must call _estimate_noise() before _noised_release() or provide argument"

        Xj: DataFrame = self.sampler.sample()
        Yj: np.ndarray = self._unwrapDataFrame(self._applyQuery(Xj))
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

    def withQuery(self, query_function: QueryFunction) -> "PACDataFrame":
        """
        Set the query function to be made private.
        """
        self.query = query_function
        return self
    
    def _applyQuery(self, df: DataFrame) -> DataFrame:
        """
        Directly apply the query to the given dataframe and return the exact output. This is not private at all!
        """
        if self.query is None:
            raise AttributeError("No query set. Use withQuery() to set the query function.")
        
        query: QueryFunction = self.query

        if not isinstance(df, DataFrame):  # runtime type check
            raise ValueError("Input to query function must be a PySpark DataFrame")
        
        y: DataFrame = query(df)
        
        if not isinstance(y, DataFrame):  # runtime type check
            raise ValueError("Output of query function must be a PySpark DataFrame")
        
        return self.query(df)
    
    @staticmethod
    def _unwrapDataFrame(df: DataFrame, labelColumn: Optional[str | Column] = None) -> np.ndarray:
        """
        Convert a PySpark DataFrame into a numpy vector.
        This is the same as "canonicalizing" the data as described in the PAC-ML paper.
        """
    
        columns: List[str] = [str(c) for c in df.columns]
        if labelColumn is not None:
            columns.remove(str(labelColumn))
        rows = df.collect()

        return np.array(list([row[col] for col in columns for row in rows]))