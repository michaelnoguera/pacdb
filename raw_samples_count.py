"""
Generate raw samples for the student absences by guardian count query. Outputs are saved
to raw_samples_count.csv.
"""

from typing import List
import numpy as np

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.ml.linalg import DenseVector

from pyspark.conf import SparkConf

from tqdm import tqdm

spark: SparkSession = (SparkSession.builder.appName("pacdb")
                        .master("local[8]")
                        .config("spark.executor.memory", "1G")
                        .config("spark.sql.warehouse.dir", ".spark")
                        .getOrCreate())

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Supress warnings about pyspark --> numpy conversion and back
import warnings
from pyspark.pandas.utils import PandasAPIOnSparkAdviceWarning
warnings.filterwarnings("ignore", category=PandasAPIOnSparkAdviceWarning)

math_df: pyspark.sql.DataFrame = spark.read.csv("./data/student_performance/student-mat.csv", header=True, inferSchema=True, sep=";")
math_df.cache()

from pacdb import PACDataFrame
df = math_df

query_name: str = "count"
budget_list: List[float] = [1/64, 1/32, 1/16, 1/8, 1/4, 1/2, 1., 2., 4.]
sample_size: int = 3
sampling_rate: float = 0.5
m: int = 10
c: float = 1e-6
mi: float = 1./4

def subsample(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return df.sample(withReplacement=False, fraction=sampling_rate)

def query(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return df.groupBy(F.col("guardian")).agg(F.avg("absences"), F.max("absences"))

def sample_once() -> np.ndarray:
    """ Generate one query output for use in hybrid algorithm """
    return PACDataFrame._unwrapDataFrame(df.transform(subsample).transform(query))


# create csv file of np.ndarray from calling sample_once

with open("raw_samples_count.csv", "a") as f:
    for i in tqdm(range(10000000)):
        s = sample_once().tolist()
        f.write(f"{s}\n")
        if i % 100 == 0:
            f.flush()
        
        
