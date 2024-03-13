"""
Contains logic for encapsulating query rewriting/query execution on samples
"""
from typing import Optional

class QueryRewriter:

    def __init__(self) -> None:
        self.aggregator_map = {
            "count": self.spark.functions.count,
            "sum"  : self.spark.functions.sum
        }

    def add_filter(self, filter: Optional[str]):
        # maybe use for multiple filters?
        return filter
    
    def map_to_function(self, aggregation: Optional[str]):
        if aggregation in self.aggregator_map:
            return self.aggregator_map[aggregation]
        else:
            raise ValueError(f"Function '{aggregation}' is not supported.")
        