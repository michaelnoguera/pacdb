"""Class responsible for implementing v1 of PAC"""

"""
v1
PAC Steps:

- modify df to filter and join df first 
- for a fixed n
    - sample df
    - apply the query on df: group by, then aggregation
- deploy algorithm to calculate noise
- final sample
    - final query
    - add noise
"""

from pac_db_re.pac_worker import PACWorker


class PACV2(PACWorker):
    def __init__():
        super.__init__()

    def filter_and_join_df(df) -> DataFrame:
        # modify the df first
        pass

    def sample_df(m=5000):
        # m times
            # sample
            # apply query params
        pass
    
    def calculate_noise():
        super.hybrid_noise_alg()