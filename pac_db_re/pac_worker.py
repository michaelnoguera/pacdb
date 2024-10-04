"""Class responsible for the methods used to apply PAC"""

from enum import Enum
from typing import Any, Callable, List, Tuple, Union
from pyspark.sql.functions import lit, when, col
from pyspark.sql import DataFrame, functions as F

import numpy as np
from pandas import DataFrame
import pyspark.pandas as ps
from pyspark.ml.feature import VectorAssembler
from typing_extensions import Protocol

import pyspark.sql.types as T
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import lit

# TODO: incorporate ENUMS
class AggregationType(Enum):
    AVG = 'avg'

class FilterTypeEnum(Enum):
    GREATER_THAN = 'greater_than'

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

    def _add_noise(self, result: np.ndarray, noise: List[float]) -> np.ndarray:
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
    
    def _insert_row_at_index(self, df: DataFrame, row: Row, index: int) -> DataFrame:
        # Split the DataFrame into two parts: before the insertion point and after
        df_before = df.limit(index)
        df_after = df.subtract(df_before)
        
        new_row_df = df.sql_ctx.createDataFrame([row])
        
        result_df = df_before.union(new_row_df).union(df_after)

        return result_df
    
    def _account_for_missing_groups(self, sample_output, true_values):
        for idx in range(len(true_values)):
            true_value = true_values[idx]
            # Check if the value exists in the sample DataFrame
            if sample_output.filter(sample_output[self.group_by_col] == true_value).count() == 0:
                # Create a new row with the col_name value and all other columns set to 0
                new_row = {self.group_by_col: true_value}
                for column in sample_output.columns:
                    if column != self.group_by_col:
                        new_row[column] = 0
                
                # Insert the new row at the correct index in the sample DataFrame
                sample_output = self._insert_row_at_index(sample_output, Row(**new_row), idx)

        return sample_output
    

    def _produce_one_sampled_output(self, 
                                    dimensions, 
                                    df=None, 
                                    true_values=None, 
                                    sampling_rate=0.5,
                                    v1=False,
                                    v2=False) -> np.ndarray:
        X: DataFrame = df.sample(withReplacement=False, fraction=sampling_rate, seed=None)

        if v1: 
            # if v1, then we filter and join every time we get a sample before group by agg
            if self.filter_col:
                X = self._filter_dataframe(X)

        # if v2, X is the filtered and joined df - use as is
        Y: DataFrame = self._applyQuery(X)

        Y = self._account_for_missing_groups(Y, true_values)

        output: np.ndarray = self._unwrapDataFrame(Y)
        return output


    # to be used as it is
    def _estimate_hybrid_noise(
            self,
            df,
            max_mi: float = 2,
            eta: float = 0.1,
            dimensions=None,
            true_values=None,
            v1=False,
            v2=False
            ) -> Tuple[List[float], List[Any]]:
        
        # Use the identity matrix for our projection matrix
        proj_matrix: np.ndarray = np.eye(dimensions)

        # projected samples used to estimate variance in each basis direction
        # est_y[i] is a list of magnitudes of the outputs in the i-th basis direction
        est_y: List[List[np.ndarray]] = [[] for _ in range(dimensions)]
        prev_ests: List[Union[float, np.floating[Any]]] = [np.inf for _ in range(dimensions)] # only to measure change per iteration for convergence

        converged = False
        curr_trial = 0

        account_for_missing_groups = False

        while not converged:
            output: np.ndarray = self._produce_one_sampled_output(df=df, dimensions=dimensions, true_values=true_values, v1=v1, v2=v2)
            if len(output) != dimensions:
                print("All groups not accounted for!")
                account_for_missing_groups = True
            
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
    
    def _group_by_count(self, df) -> DataFrame:
        return df.groupBy(self.group_by_col).count()
    
    def _check_group_by_count_threshold(self, X, groups_list, noise, threshold_value) -> dict:
        # get final output, accounting for any missing groups in the final sample
        intermediate_Y = self._group_by_count(X)
        intermediate_Y = self._account_for_missing_groups(intermediate_Y, groups_list)
        output: np.ndarray = self._unwrapDataFrame(intermediate_Y)

        # add noise to count
        intermediate_noised_output: np.ndarray = self._add_noise(output, noise)

        intermediate_output_df = self._updateDataFrame(intermediate_noised_output, intermediate_Y)
        intermediate_output_df.show()

        flag_dict = {}
        # here check if any group counts are below threshold
        for ind in range(len(intermediate_noised_output)):
            noisy_group_count = intermediate_noised_output[ind]
            if abs(noisy_group_count) < threshold_value:
                flag_dict[groups_list[ind]] = True
            else:
                flag_dict[groups_list[ind]] = False

        return flag_dict
    
    def _apply_threshold(self, df, flag_dict):
        for key, should_update in flag_dict.items():
            if should_update:
                # Apply the condition to each column
                for column in df.columns:
                    if column != self.group_by_col:
                        df = df.withColumn(
                            column,
                            F.when(F.col(self.group_by_col) == key, 
                                   F.lit(0)).otherwise(F.col(column))
                        )
        return df
    
    def estimate_noise(self, 
                       df, 
                       sampling_rate=None, 
                       v1=True, 
                       v2=False): 
        # get true result to get correct dimensions
        if self.filter_col:
            filtered_df = self._filter_dataframe(df)

        # TODO: add support for join
        filtered_and_joined_df = filtered_df

        Y: DataFrame = self._applyQuery(filtered_and_joined_df)
        groups_list = [row[self.group_by_col] for row in Y.select(self.group_by_col).collect()]
        output: np.ndarray = self._unwrapDataFrame(Y)

        zeroes: np.ndarray = np.zeros(output.shape)
        self._updateDataFrame(zeroes, Y).show()

        # whether v1 or v2, only changes how the sampling efficiency is improved
        if v1:
            noise: List[float] = self._estimate_hybrid_noise(df=df, dimensions=len(zeroes), true_values=groups_list, v1=True)[0]
        if v2:
            noise: List[float] = self._estimate_hybrid_noise(df=filtered_and_joined_df, dimensions=len(zeroes), true_values=groups_list, v2=True)[0]

        return noise, groups_list
    
    def release_pac_value(self, 
                          df, 
                          groups_list, 
                          threshold_value, 
                          noise, 
                          sampling_rate=0.5):
        # get final sample
        X: DataFrame = df.sample(withReplacement=False, fraction=sampling_rate, seed=None)
        if self.filter_col:
            X = self._filter_dataframe(X)

        flag_dict = self._check_group_by_count_threshold(X, groups_list, noise, threshold_value)
        
        # apply query again - this time group by actual agg type
        Y: DataFrame = self._applyQuery(X)
        Y = self._account_for_missing_groups(Y, groups_list)
        output: np.ndarray = self._unwrapDataFrame(Y)
        noised_output: np.ndarray = self._add_noise(output, noise)

        output_df = self._updateDataFrame(noised_output, Y)
        output_df.show()

        output_df = self._apply_threshold(df=output_df, flag_dict=flag_dict)
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





"""
Y_dict = {row[self.group_by_col]: row['count'] for row in intermediate_Y.collect()}

        for i, group in true_groups:
            if group in Y_dict:
                print('Group present in sample!')
            else:
                items = list(Y_dict.items())
                # Insert the new item at the specified index
                items.insert(i, (group, 0))
                Y_dict = dict(items)

            i += 1
"""