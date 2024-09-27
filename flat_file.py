"""
PAC all in one file
"""

from typing import Any, Callable, Dict, List, Tuple, Union

import numpy as np
import pyspark.pandas as ps
import pyspark.sql
import pyspark.sql.types as T
from pyspark.conf import SparkConf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import DenseVector
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from tqdm import tqdm

### Spark Setup

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

### Data Setup

math_df: pyspark.sql.DataFrame = spark.read.csv("./data/student_performance/student-mat.csv", header=True, inferSchema=True, sep=";")
math_df.cache()

# load tpch tables from data/tpch/*.parquet
# TPCH_TABLE_NAMES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
TPCH_TABLE_NAMES = ["lineitem"]  # for q1 we only need this one table

tables: Dict[str, DataFrame] = {
    t: spark.read.parquet(f"./data/tpch/{t}.parquet") for t in TPCH_TABLE_NAMES
}

### Query Setup

df = tables["lineitem"]

# Push down filter all the way
df2 = df.filter(F.col("l_shipdate") <= "1998-09-02")

# We have to sample before we aggregate
# We need many samples, though, so we'll put them all in an array

SAMPLES = 10
out: List[DataFrame] = []
group_by_counts: List[int] = []

while len(out) < SAMPLES:
    df3 = df2.sample(withReplacement=False, fraction=0.5)  # sampling step

    group_by_count = df3.groupBy("l_returnflag", "l_linestatus").count()

    df4 = (df3.groupBy("l_returnflag", "l_linestatus")
            .agg(
                F.col("l_returnflag"),
                F.col("l_linestatus"),
                F.sum("l_quantity").alias("sum_qty"),
                F.sum("l_extendedprice").alias("sum_base_price"),
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("sum_disc_price"),
                F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount") * (1 + F.col("l_tax")))).alias("sum_charge"),
                F.avg("l_quantity").alias("avg_qty"),
                F.avg("l_extendedprice").alias("avg_price"),
                F.avg("l_discount").alias("avg_disc"),
                F.count("*").alias("count_order"),
            )
            .sort("l_returnflag", "l_linestatus")
        )
    
    out.append(df4)

### Account for Missing Groups
# For all outputs, if any groups are missing from the output, add them in with 0 values

# First we run the query once without any sampling to get the true output format with all possible groups present
GROUP_BY_KEYS = ["l_returnflag", "l_linestatus"]
df_unsampled_output = (df2
                        .groupBy(*GROUP_BY_KEYS)
                        .agg(
                            F.col("l_returnflag"),
                            F.col("l_linestatus"),
                            F.sum("l_quantity").alias("sum_qty"),
                            F.sum("l_extendedprice").alias("sum_base_price"),
                            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("sum_disc_price"),
                            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount") * (1 + F.col("l_tax")))).alias("sum_charge"),
                            F.avg("l_quantity").alias("avg_qty"),
                            F.avg("l_extendedprice").alias("avg_price"),
                            F.avg("l_discount").alias("avg_disc"),
                            F.count("*").alias("count_order"),
                        )
                        .sort("l_returnflag", "l_linestatus")
                    )

# Build a template: Zero out everything but the group-by columns
df_unsampled_zeroed = df_unsampled_output.select(
    *GROUP_BY_KEYS, # retain only values of the group-by columns
    *[F.lit(0).alias(col) for col in df_unsampled_output.columns if col not in GROUP_BY_KEYS] # set all other columns to zeroes
)

# Here we assume that all output DataFrames have the same schema. If this assumption is not valid, then this line should
# move inside the loop below (and replace out[0] with o)
df_unsampled_zeroed = df_unsampled_zeroed.select(out[0].columns)

# Now apply the template to all of the output DataFrames. If a group is missing from the output, it will be added in with 0 values
for o in out:
    o = (o.union(df_unsampled_zeroed)  # append the zeroed-out rows to the output
         .groupBy(*GROUP_BY_KEYS)  # deduplicate the output, preferring the original values
         .agg(*[F.first(col).alias(col) for col in o.columns if col not in GROUP_BY_KEYS]))
    
    # We'll re-sort so that the order of the groups doesn't give anything away.
    o = o.sort("l_returnflag", "l_linestatus")  # TODO generalize

    o.show()

### Count thresholding
# If the number of rows from the original table contributing to any of the groups is smaller than the threshold, we omit the group
COUNT_THRESHOLD = 10

# *********************Hybrid-DP Noise******************************

# Step 1: Check group
group_by_count = df3.groupBy("l_returnflag", "l_linestatus").count()

# Step 2: Add DP-noise to each group
# TODO: How to convert MI to epsilon? 
epsilon = 0.5
sensitivity = 1 # since we are calculating count, sensitivity is 1
scale = sensitivity / epsilon
noise = np.random.laplace(0, scale, 1)[0]

noisy_group_by_count = group_by_count + noise

# Step 3: Check if noisy_group_by_count is less than threshold -- this is the algorithm
# flags = []
# i = 0
# for noisy_group in noisy_group_by_count:
#     if noisy_group < COUNT_THRESHOLD:
#         flags[i] = 1
#     i += 1

# Step 4: Continue with adding PAC noise - at the very end, check if the PAC-noised-group was flagged using index i -- if yes, make the value 0
    
# *********************Hybrid-DP Noise******************************

### Convert to Numpy Array for PAC logic

def unwrapDataFrame(df: DataFrame) -> np.ndarray:
    """
    Convert a PySpark DataFrame into a numpy vector.
    This is the same as "canonicalizing" the data as described in the PAC-ML paper.
    """
    
    numeric_columns: List[str] = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]

    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features", handleInvalid="error")
    df_vector = assembler.transform(df).select("features").rdd.flatMap(lambda x: x.features)

    return np.array(df_vector.collect())

out_np: List[np.ndarray] = [unwrapDataFrame(o) for o in out]

### Compute PAC Noise

max_mi: float = 1./4

# Use the identity matrix for our projection matrix
dimensions = len(out_np[0])
proj_matrix: np.ndarray = np.eye(dimensions)

# projected samples used to estimate variance in each basis direction
# est_y[i] is a list of magnitudes of the outputs in the i-th basis direction
est_y: List[List[np.ndarray]] = [[] for _ in range(dimensions)]

for i in range(SAMPLES):
    est_y[i].append(out_np[i])

# get the variance in each basis direction
fin_var: List[np.floating[Any]] = [np.var(est_y[i]) for i in range(dimensions)]
# and the mean in each basis direction
fin_mean: List[np.floating[Any]] = [np.mean(est_y[i]) for i in range(dimensions)]

sqrt_total_var = sum([fin_var[x]**0.5 for x in range(len(fin_var))])

noise: np.ndarray = np.array([np.inf for _ in range(dimensions)])
for i in range(dimensions):
    noise[i] = float(1./(2*max_mi) * fin_var[i]**0.5 * sqrt_total_var)

noises_to_add = list(noise)
noises_other_outputs = [sqrt_total_var, fin_var, fin_mean]

print(noises_to_add)
print(noises_other_outputs)

# TODO not yet working correctly