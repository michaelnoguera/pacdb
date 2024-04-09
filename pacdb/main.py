from dataclasses import dataclass
from typing import Any, List, Optional
from typing_extensions import Protocol

import numpy as np

from pyspark.sql import DataFrame, Column
import pyspark.sql.types as T
import pyspark.pandas as ps

from .sampler import DataFrameSampler, SamplerOptions

from tqdm import tqdm


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

    @classmethod
    def fromDataFrame(cls, df: DataFrame) -> "PACDataFrame":
        """
        Create a PACDataFrame from an existing Spark DataFrame.
        """
        return cls(df)
    
    def withOptions(self, options: PACOptions) -> "PACDataFrame":
        """
        Set the PAC options for the dataframe.
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

    def _produce_one_sampled_output(self) -> np.ndarray:
        X: DataFrame = self.sampler.sample()
        Y: DataFrame = self._applyQuery(X)
        output: np.ndarray = self._unwrapDataFrame(Y)
        return output
        
    def _estimate_hybrid_noise(
        self,
        max_mi: Optional[float] = None,
        quiet: bool = False
        ) -> List[float]:
        """
        Use the hybrid algorithm to determine how much noise to add to each dimension of the query result.

        Parameters:
        max_mi: float, optional
            Maximum mutual information allowed for the query. If not provided, use the PACDataFrame setting.
        
        Returns:
        noise: List[float]
            How much noise to add to each dimension of the query result vector. The value in noise[i] is the
            variance of a Gaussian distribution from which to sample noise to add to the i-th dimension of the
            query result.
        """

        if max_mi is None:  # optional argument, otherwise use PACDataFrame setting
            max_mi = self.options.max_mi

        eta: float = 0.05  # convergence threshold 

        # r = max([np.linalg.norm(x) for x in train_x])

        # We do not need to create a new set of ordered keys to 'canonicalize' the data, because
        # the data is already in a consistent column order.

        # Use the identity matrix for our projection matrix
        dimensions = len(self._produce_one_sampled_output())
        proj_matrix: np.ndarray = np.eye(dimensions)

        # TODO: Use the SVD matrix as the projection matrix

        if not quiet:
            print(f"max_mi: {max_mi}, eta: {eta}, dimensions: {dimensions}")
            print("Hybrid Noise: Using the identity matrix as the projection matrix.")
        else:
            print("Anisotropic Noise: Using SVD to find the projection matrix.")

        # projected samples used to estimate variance in each basis direction
        # est_y[i] is a list of magnitudes of the outputs in the i-th basis direction
        est_y: List[List[np.ndarray]] = [[] for _ in range(dimensions)]
        prev_ests: List[np.floating[Any]] = [np.inf for _ in range(dimensions)] # to measure change per iteration for convergence

        converged = False
        curr_trial = 0

        if not quiet:
            progress = tqdm()

        while not converged:
            output: np.ndarray = self._produce_one_sampled_output()
            assert len(output) == dimensions

            # Compute the magnitude of the output in each of the basis directions, update the estimate lists
            for i in range(len(output)):
                est_y[i].append(np.matmul(proj_matrix[i].T, output.T))

            # Every 10 trials, check for convergence
            if curr_trial % 10 == 0:
                if not quiet:
                    loss = sum(abs(np.var(est_y[i]) - prev_ests[i]) for i in range(dimensions))
                    target = eta * dimensions
                    progress.update(10)
                    progress.set_postfix({"loss": loss, "target": target})

                # If all dimensions' variance estimates changed by less than eta, we have converged
                if all(abs(np.var(est_y[i]) - prev_ests[i]) <= eta for i in range(dimensions)):
                    converged = True
                else:
                    # we have not converged, so update the previous estimates and continue
                    prev_ests = [np.var(est_y[i]) for i in range(dimensions)]
            curr_trial += 1

        # Now that we have converged, get the variance in each basis direction
        fin_var: List[np.floating[Any]] = [np.var(est_y[i]) for i in range(dimensions)]

        if not quiet:
            progress.close()
            print(f"Converged after {curr_trial} trials")
            print(f"Final variance estimates: {fin_var}")

        sqrt_total_var = sum(fin_var)**0.5
        print(f'sqrt total var is {sqrt_total_var}')

        noise: List[float] = [np.inf for _ in range(dimensions)]
        for i in range(dimensions):
            noise[i] = 1./max_mi**0.5 * fin_var[i]**0.5 * sqrt_total_var

        print(f'Computed noise (variances) is {noise}')

        return noise

    @staticmethod
    def _add_noise(result: np.ndarray, noise: List[float], quiet=False) -> np.ndarray:
        # noise is an array of variances, one for each dimension of the query result
        noised = []
        for i in range(len(result)):
            noised.append(result[i] + np.random.normal(0, noise[i]))

        if not quiet: 
            print(f'Sample: {result} + Noise = Noised: {noised}')
        
        return np.array(noised)

    def releaseValue(self, quiet=False) -> DataFrame:
        """
        Execute the query with PAC privacy.
        """
        
        X: DataFrame = self.sampler.sample()
        Y: DataFrame = self._applyQuery(X) 
        output: np.ndarray = self._unwrapDataFrame(Y)

        if not quiet:
            print("Found output format of query: ")
            zeroes = np.zeros(output.shape)
            self._updateDataFrame(zeroes, Y).show()

        noise: List[float] = self._estimate_hybrid_noise()

        noised_output: np.ndarray = self._add_noise(output, noise, quiet=quiet)

        output_df = self._updateDataFrame(noised_output, Y)

        if not quiet:
            print("Inserting to dataframe:")
            output_df.show()
        
        return output_df


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
    def _unwrapDataFrame(df: DataFrame) -> np.ndarray:
        """
        Convert a PySpark DataFrame into a numpy vector.
        This is the same as "canonicalizing" the data as described in the PAC-ML paper.
        """
        
        # Filter to only numeric columns and coerce to numpy array
        numeric_columns: List[str] = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]
        df_numeric: DataFrame = df.select(*numeric_columns)  # select only numeric columns
        np_array: np.ndarray = np.array(df_numeric.collect())

        # Flatten the numpy array column-wise
        flat: np.ndarray = np_array.flatten(order="F")

        return flat

    @staticmethod
    def _updateDataFrame(vec: np.ndarray, df: DataFrame) -> DataFrame:
        """
        Use the values of the numpy vector to update the PySpark DataFrame.
        """

        # Recompute shape and columns
        numeric_columns: List[str] = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]
        df_numeric: DataFrame = df.select(*numeric_columns)  # select only numeric columns
        shape = np.array(df_numeric.collect()).shape

        # -> 2D
        np_array = vec.reshape(shape, order="F")

        # -> Pandas-On-Spark (attach column labels)
        new_pandas: ps.DataFrame = ps.DataFrame(np_array, columns=numeric_columns)

        # Merge the new values with the old DataFrame
        old_pandas: ps.DataFrame = df.pandas_api()
        old_pandas.update(new_pandas)
        updated_df: DataFrame = old_pandas.to_spark()

        return updated_df