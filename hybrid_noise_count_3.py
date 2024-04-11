from typing import List
import numpy as np

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark: SparkSession = (SparkSession.builder.appName("pacdb")
         .config("spark.executor.memory", "1024M")
         .config("spark.sql.warehouse.dir", ".spark")
         .enableHiveSupport()
         .getOrCreate())

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Supress warnings about pyspark --> numpy conversion and back
import warnings
from pyspark.pandas.utils import PandasAPIOnSparkAdviceWarning
warnings.filterwarnings("ignore", category=PandasAPIOnSparkAdviceWarning)


math_df: pyspark.sql.DataFrame = spark.read.csv("./data/student_performance/student-mat.csv", header=True, inferSchema=True, sep=";")


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

for _ in range(50):
    out = PACDataFrame.estimate_hybrid_noise_static(sample_once=sample_once, max_mi=mi)
    print(f'noise: {out[0]}, sqrt_total_var: {out[1][0]}, variances: {out[1][1]}, mean: {out[1][2]}')
    with open("hybrid_noise_count_3.csv", "a") as f:
        f.write(f"{out[0]},{out[1][0]},{out[1][1]},{out[1][2]}\n")