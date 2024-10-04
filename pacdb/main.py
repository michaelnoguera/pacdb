from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from typing_extensions import Protocol

import numpy as np
import time

from pyspark.sql import DataFrame, Column
import pyspark.sql.types as T
import pyspark.pandas as ps

from pacdb.threshold.threshold import Threshold

from .sampler import DataFrameSampler, SamplerOptions
from .budget_accountant import BudgetAccountant

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

        self.thresholder = Threshold()
        self.groupByCol = ''
        self.agg_mapping = {'avg': 'avg',
                    'sum': 'sum'    }
        self.agg = None

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
    
    def setGroupBy(self, groupByCol):
        self.groupByCol = groupByCol
    
    def setAgg(self, agg):
        self.agg = self.agg_mapping.get(agg)

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
        X: DataFrame = self.sampler.sample() # samples using pyspark sample
        Y: DataFrame = self._applyQuery(X) # sets query parameter
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

        # Use the identity matrix for our projection matrix
        dimensions = len(self._produce_one_sampled_output())
        proj_matrix: np.ndarray = np.eye(dimensions) # just creates a diagonal matrix

        if not quiet:
            print("Hybrid Noise: Using the identity matrix as the projection matrix.")
        else:
            print("Anisotropic Noise: Using SVD to find the projection matrix.")

        # projected samples used to estimate variance in each basis direction
        # est_y[i] is a list of magnitudes of the outputs in the i-th basis direction
        # est_y: List[List[np.ndarray]] = [[] for _ in range(dimensions)]
        # prev_ests: List[np.floating[Any]] = [np.inf for _ in range(dimensions)] # to measure change per iteration for convergence

        est_y = {}
        prev_ests = None


        # TODO
        converged = False
        curr_trial = 0

        if not quiet:
            progress = tqdm()


        while not converged:
            # Step 1: sample
            # Step 2: get output
            # Step 3: update estimate lists
            # Step 4: Every 10 trials, Check for convergence
            output: np.ndarray = self._produce_one_sampled_output()
            assert len(output) == dimensions

            # Compute the magnitude of the output in each of the basis directions, update the estimate lists
            # for i in range(len(output)):
            #     est_y[i].append(np.matmul(proj_matrix[i].T, output.T))

            for ind in range(len(output)):
                if ind not in est_y:
                    est_y[ind] = []
                est_y[ind].append(np.matmul(proj_matrix[ind].T, np.array(output).T))

            # Every 10 trials, check for convergence
            if curr_trial % 10 == 0:
                if prev_ests is None:
                    prev_ests = {}
                    for ind in est_y:
                        prev_ests[ind] = np.var(est_y[ind])
                else:
                    converged = True
                    for ind in est_y:
                        if abs(np.var(est_y[ind]) - prev_ests[ind]) > eta:
                            converged = False
                    if not converged:
                        for ind in est_y:
                            prev_ests[ind] = np.var(est_y[ind])

            curr_trial += 1

        fin_var = {ind: np.var(est_y[ind]) for ind in est_y}

        noise = {}
        sqrt_total_var = sum(fin_var.values())**0.5

        for ind in fin_var:
            noise[ind] = 1./max_mi**0.5 * fin_var[ind]**0.5 * sqrt_total_var

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
    
    def applyGroupBy(df: DataFrame, groupByCol: str) -> dict:
        """
        Groups the DataFrame by the specified column and returns a dictionary of the group and its count.
        """
        # Group by the specified column and count the number of rows in each group
        grouped_df = df.groupBy(groupByCol).count()
        
        # Collect the results as a list of Row objects
        grouped_list = grouped_df.collect()
        
        # Convert the list of Row objects to a dictionary
        result_dict = {row[groupByCol]: row['count'] for row in grouped_list}
        
        return result_dict

    def releaseValue(self, quiet=False, threshold=False, threshold_value=3) -> DataFrame:
        """
        Execute the query with PAC privacy.
        """
        # sample to determine size of output
        X: DataFrame = self.sampler.sample()
        Y: DataFrame = self._applyQuery(X) 
        output: np.ndarray = self._unwrapDataFrame(Y)

        if not quiet:
            zeroes = np.zeros(output.shape)
            self._updateDataFrame(zeroes, Y)

        noise: List[float] = self._estimate_hybrid_noise(quiet=quiet)

        # take final sample
        final_X: DataFrame = self.sampler.sample()
        Y: DataFrame = self._applyQuery(final_X) 
        output: np.ndarray = self._unwrapDataFrame(Y)




        # threshold
        if threshold:
            self.thresholder.set_threshold(threshold)
            print(final_X)
            print(self.groupByCol)
            dict_of_counts = self.applyGroupBy(final_X, self.groupByCol)
            
            new_dict = {key: value >= threshold for key, value in dict_of_counts.items()}

            final_Y: DataFrame = self._applyQuery(final_X) # DF: col 1: guardian col 2: avg

            group_col = final_Y.columns[0]
            agg_col = final_Y.columns[1]

            final_Y[agg_col] = final_Y[agg_col].where(final_Y[group_col].map(new_dict), 0)
            final_output: np.ndarray = self._unwrapDataFrame(final_Y)
        
        else:
            final_Y: DataFrame = self._applyQuery(final_X) 
            final_output: np.ndarray = self._unwrapDataFrame(final_Y)

        # this stays the same assuming that noise is added on default values if threshold was not met
        noised_output: np.ndarray = self._add_noise(final_output, noise, quiet=quiet)

        output_df = self._updateDataFrame(noised_output, Y)

        if not quiet:
            print("Inserting to dataframe:")
            output_df.show()
        
        return noise

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