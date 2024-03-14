from dataclasses import dataclass
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union, overload
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, count, concat_ws
from pyspark.sql.types import StringType, Row
from pyspark.sql.column import Column
from typeguard import typechecked
from functools import wraps
from abc import ABC, abstractmethod
from tqdm import tqdm

@dataclass
class SamplerOptions():
    withReplacement: Optional[bool] = False
    fraction: Union[int, float] = 0.5
    seed: Optional[int] = None
    columns_to_sample_by: List[str] | None = None

class Sampler(ABC):
    """
    Samplers are used by the PACDataframe to sample data
    You can provide your own sampler by implementing this interface
    """

    def __init__(self, df: Optional[DataFrame] = None, options: SamplerOptions = SamplerOptions()):
        self.df = df
        self.options: SamplerOptions = options  # defaults to 50% w/o replacement
        ...

    def withOptions(self, options: SamplerOptions) -> "Sampler":
        self.options = options
        return self

    def withOption(self, option: str, value: Any) -> "Sampler":
        if option not in self.options.__dict__:  # check if the option exists
            raise ValueError(f"Option {option} does not exist")
        setattr(self.options, option, value)
        return self
    
    def withDataFrame(self, df: DataFrame) -> "Sampler":
        self.df = df
        return self

    @abstractmethod
    def sample(self) -> "DataFrame":
        pass
    
    @abstractmethod
    def sampleByColumns(
        self, 
        cols: List[str], 
    ) -> "DataFrame":
        pass


class DataFrameSampler(Sampler):
    """Implementation of Sampler methods"""

    def __init__(self, df: Optional[DataFrame] = None):
        super().__init__(df)


    def get_samples(self, query_filter: Optional[str] = None, aggr: Optional[Any] = None, trials: int = 100) -> list:
        samples = []

        for i in tqdm(range(trials)):
            if query_filter is not None and aggr is not None:
                s = (self.sample()
                    .filter(query_filter)  # run query on sample
                    .agg(aggr))
            elif query_filter is None:
                s = (self.sample()
                    .agg(aggr))
            elif aggr is None:
                s = (self.sample()
                    .filter(query_filter))  # run query on sample

            samples.append(s)  # store result of query
        
        # TODO: change based on the aggregation being performed
        samples = [s * (1/self.options.fraction) for s in samples]  # so that counts are not halved

        return samples

    def sample(self) -> DataFrame:
        """
        Returns self DataFrame with default options
        """
        # Chai's Note: pyspark.sql.sample() does not guarantee Poisson i.i.d. sampling so we need to override or implement a custom function
        if self.df is None:
            raise ValueError("No dataframe attached to this sampler")
        return self.df.sample(withReplacement=self.options.withReplacement, fraction=self.options.fraction, seed=self.options.seed)
    