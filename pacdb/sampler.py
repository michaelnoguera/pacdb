from dataclasses import dataclass
from typing import Optional, Union
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod

@dataclass
class SamplerOptions():
    withReplacement: Optional[bool] = False
    fraction: Union[int, float] = 0.5
    seed: Optional[int] = None

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
    
    def withDataFrame(self, df: DataFrame) -> "Sampler":
        self.df = df
        return self

    @abstractmethod
    def sample(self) -> "DataFrame":
        pass

class DataFrameSampler(Sampler):
    def __init__(self, df: Optional[DataFrame] = None):
        super().__init__(df)

    def sample(self) -> DataFrame:
        if self.df is None:
            raise ValueError("No dataframe attached to this sampler")
        return self.df.sample(withReplacement=self.options.withReplacement, fraction=self.options.fraction, seed=self.options.seed)