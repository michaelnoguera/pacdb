"""Class responsible for implementing v1 of PAC"""

"""
v1
PAC Steps:

- for a fixed n
    - sample df
    - apply the query on df: first filter, then join, then group by, then aggregation
- deploy algorithm to calculate noise
- final sample
    - final query
    - add noise
"""

import numpy as np
from pandas import DataFrame
from pac_db_re.pac_worker import PACWorker


class PACV1(PACWorker):

    def __init__():
        super.__init__()

    # use your new params only for the final sample
        

    def _apply_query(self, df: DataFrame, dimensions: int):
        account_for_missing_groups = False

        if self.filter_col:
            super._filter_dataframe(df)

        # TODO: add join support
            
        if self.group_by_col:
            grouped_df = super._apply_group_by(df)
            sample_output_dimensions = grouped_df.count().count()

            # if a sample results in 0 count for any group, account for the group anyway
            if sample_output_dimensions < dimensions:
                account_for_missing_groups = True

        if self.agg_type:
            result_df = super._aggregate_col(df)

            if account_for_missing_groups:
                # fill the rest of the result
                result_df.show()


    def sample_df(self, df, m=5000, sampling_rate=0.5):
        sample_df_list_df_i = []
        query_output_qdf_i = []

        dimensions = None
        distinct_group_values = None

        if self.group_by_col:
            grouped_df = super._apply_group_by(df)
            result_df = grouped_df.count()
            dimensions = result_df.count()
            print(f"Number of groups: {dimensions}")

            distinct_group_values = grouped_df.select(self.group_by_col).rdd.flatMap(lambda x: x).collect()
            print(f"Distinct groups: {distinct_group_values}")

        for _ in m:
            sample_df = df.sample(withReplacement=self.options.withReplacement, 
                                                 fraction=self.options.fraction, 
                                                 seed=self.options.seed)
            sample_df_list_df_i.append(sample_df)
            output = self._apply_query(sample_df, dimensions, distinct_group_values)

            assert len(output) == dimensions
        

    def calculate_noise():
        super.hybrid_noise_alg()

    def release_pac_value(threshold=False, threshold_value=3):
        # if threshold,
            # apply group by, and check 

        pass