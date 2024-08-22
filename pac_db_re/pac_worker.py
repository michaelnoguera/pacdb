"""Class responsible for the methods used to apply PAC"""

from enum import Enum
from typing import Any, Callable, List, Tuple, Union

import numpy as np
from pandas import DataFrame
import pyspark.pandas as ps
from pyspark.ml.feature import VectorAssembler

import pyspark.sql.types as T

from pacdb.main import QueryFunction

# TODO: incorporate ENUMS
class AggregationType(Enum):
    AVG = 'avg'

class FilterTypeEnum(Enum):
    GREATER_THAN = 'greater_than'

"""
Take query input
and the new inputs you defined

estimate_noise() - 
release_value() - 
"""

class PACWorker():

    def __init__(self, 
                 filter_col=None, 
                 filter_value=None, 
                 filter_type=None, 
                 join_db=None, 
                 join_id=None, 
                 group_by_col=None, 
                 agg_type=[],
                 agg_col=None,
                 query_function=QueryFunction) -> None:
        self.filter_col = filter_col
        self.filter_value = filter_value
        self.filter_type = filter_type
        self.join_db = join_db
        self.join_id = join_id
        self.group_by_col = group_by_col
        self.agg_type = agg_type
        self.agg_col = agg_col

        self.threshold = None
        self.threshold_value = 3

        self.query = query_function

    def _updateDataFrame(self, vec: np.ndarray, df: DataFrame) -> DataFrame:
        """
        Use the values of the vector to update the PySpark DataFrame.
        """

        # Recompute shape and columns
        numeric_columns: List[str] = [f.name for f in df.schema.fields 
                                      if isinstance(f.dataType, T.NumericType)]
        shape = (df.count(), len(numeric_columns))
        
        # Convert flat-mapped array to an array of rows
        np_array = np.reshape(vec, shape)

        # -> Pandas-On-Spark (attach column labels)
        new_pandas: ps.DataFrame = ps.DataFrame(np_array, columns=numeric_columns)

        # Merge the new values with the old DataFrame
        old_pandas: ps.DataFrame = df.pandas_api()
        old_pandas.update(new_pandas)
        updated_df: DataFrame = old_pandas.to_spark()

        return updated_df
    

    def _filter_dataframe(self, df: DataFrame) -> DataFrame:
        if self.filter_type == FilterTypeEnum.GREATER_THAN:
            return df.filter(df[self.filter_col] > self.filter_value)
        # TODO: add support for < >= <= == !=
        else:
            raise ValueError("Invalid filter_type. Use 'greater_than' or 'lesser_than'.")

    def _add_noise(result: np.ndarray, noise: List[float]) -> np.ndarray:
        # noise is an array of variances, one for each dimension of the query result
        noised = []
        for i in range(len(result)):
            noised.append(result[i] + np.random.normal(0, noise[i]))

        print(f'Sample: {result} + Noise = Noised: {noised}')
        
        return np.array(noised)

    def _applyQuery(self, df: DataFrame) -> DataFrame:
        """
        Directly apply the query to the given dataframe and return the exact output. This is not private at all!
        """
        return df.transform(self.query)
    
    def _unwrapDataFrame(self, df: DataFrame) -> np.ndarray:
        """
        Convert a PySpark DataFrame into a numpy vector.
        This is the same as "canonicalizing" the data as described in the PAC-ML paper.
        """
        
        numeric_columns: List[str] = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]

        assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features", handleInvalid="error")
        df_vector = assembler.transform(df).select("features").rdd.flatMap(lambda x: x.features)

        return np.array(df_vector.collect())
    

    def _produce_one_sampled_output(self, df) -> np.ndarray:
        X: DataFrame = df.sample(withReplacement=self.options.withReplacement, 
                                                 fraction=self.options.fraction, 
                                                 seed=self.options.seed)
        # Chai Debug: add support for filter here (and later join)
        if self.filter_col:
            X = self._filter_dataframe(X)

        Y: DataFrame = self._applyQuery(X)
        output: np.ndarray = self._unwrapDataFrame(Y)
        return output


    # to be used as it is
    def _estimate_hybrid_noise(
            self,
            sample_once: Callable[[], np.ndarray],
            max_mi: float = 1./4,
            eta: float = 0.05,
            dimensions=None
            ) -> Tuple[List[float], List[Any]]:
        
        # Use the identity matrix for our projection matrix
        proj_matrix: np.ndarray = np.eye(dimensions)

        # projected samples used to estimate variance in each basis direction
        # est_y[i] is a list of magnitudes of the outputs in the i-th basis direction
        est_y: List[List[np.ndarray]] = [[] for _ in range(dimensions)]
        prev_ests: List[Union[float, np.floating[Any]]] = [np.inf for _ in range(dimensions)] # only to measure change per iteration for convergence

        converged = False
        curr_trial = 0

        while not converged:
            output: np.ndarray = sample_once()
            if len(output) == dimensions:
                print("All groups accounted for!")
            else: 
                account_for_missing_groups = True
                # for every group that is missing, insert a 0

            
            # Compute the magnitude of the output in each of the basis directions, update the estimate lists
            for i in range(len(output)):
                est_y[i].append(output[i])

                # Every 10 trials, check for convergence
                if curr_trial % 10 == 0:
                    # If all dimensions' variance estimates changed by less than eta, we have converged
                    if all(abs(np.var(est_y[i]) - prev_ests[i]) <= eta for i in range(dimensions)):
                        converged = True
                    else:
                        # we have not converged, so update the previous estimates and continue
                        prev_ests = [np.var(est_y[i]) for i in range(dimensions)]
                curr_trial += 1

        # Now that we have converged, get the variance in each basis direction
        fin_var: List[np.floating[Any]] = [np.var(est_y[i]) for i in range(dimensions)]
        # and the mean in each basis direction
        fin_mean: List[np.floating[Any]] = [np.mean(est_y[i]) for i in range(dimensions)]

        sqrt_total_var = sum([fin_var[x]**0.5 for x in range(len(fin_var))])

        noise: np.ndarray = np.array([np.inf for _ in range(dimensions)])
        for i in range(dimensions):
            noise[i] = float(1./(2*max_mi) * fin_var[i]**0.5 * sqrt_total_var)

        return list(noise), [sqrt_total_var, fin_var, fin_mean]
    
    # TODO: what do we want happens in this case
    # before filter if we group by we get 2 true groups
    # after filter, we get 1 true group

    def estimate_noise(self, df, sampling_rate=None): 
        # TODO: add support for join

        # get true result to get correct dimensions
        if self.filter_col:
            df = self._filter_dataframe(df)

        Y: DataFrame = self._applyQuery(df)
        output: np.ndarray = self._unwrapDataFrame(Y)

        zeroes: np.ndarray = np.zeros(output.shape)
        self._updateDataFrame(zeroes, Y).show()

        noise: List[float] = self._estimate_hybrid_noise(sample_once=self._produce_one_sampled_output(df), 
                                                         dimensions=len(zeroes))[0]

        return noise
    
    def release_pac_value(self, df, threshold, threshold_value, sampling_rate, noise):
        X: DataFrame = df.sample(withReplacement=False, fraction=sampling_rate, seed=None)

        # apply query -
        # first filter
        # then join
        # then group by count
        Y: DataFrame
        output: np.ndarray

        # add noise to count
        intermediate_noised_output: np.ndarray = self._add_noise(output, noise)
        # here check if any group counts are below threshold
        # if yes,
            # flag the group
        
        # apply query again -
        # this time group by actual agg type
        Y: DataFrame
        output: np.ndarray
        # if the group is flagged, make its result 0

        noised_output: np.ndarray = self._add_noise(output, noise)

        output_df = self._updateDataFrame(noised_output, Y)
        output_df.show()
















    """may not use"""
    
        
    def _apply_group_by(self, df: DataFrame) -> DataFrame:
        return df.groupBy(self.group_by_col)


    def _aggregate_col(self, df) -> DataFrame:
        # Apply each aggregation type
        aggregations = []
        for agg_type in self.agg_type:
            if agg_type == "sum":
                aggregations.append(df.sum(self.agg_col).alias(f"sum_{self.agg_col}"))
            elif agg_type == "avg":
                aggregations.append(df.avg(self.agg_col).alias(f"avg_{self.agg_col}"))
            elif agg_type == "max":
                aggregations.append(df.max(self.agg_col).alias(f"max_{self.agg_col}"))
            elif agg_type == "min":
                aggregations.append(df.min(self.agg_col).alias(f"min_{self.agg_col}"))
            elif agg_type == "count":
                aggregations.append(df.count().alias("count"))
            else:
                raise ValueError(f"Invalid agg_type '{agg_type}'. Use 'sum', 'avg', 'max', 'min', or 'count'.")
        
        # Apply the aggregations
        result_df = df.agg(*aggregations)
        
        return result_df





