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

# load tpch tables from data/tpch/*.parquet
# TPCH_TABLE_NAMES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
TPCH_TABLE_NAMES = ["lineitem"]  # for q1 we only need this one table

tables: Dict[str, DataFrame] = {
    t: spark.read.parquet(f"./data/tpch/{t}.parquet") for t in TPCH_TABLE_NAMES
}

### Query Setup

# Input to query step:
# 1. tables: Dict[str, DataFrame] - the tables to query
#  . the query to run (expressed here inline)

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
                #F.col("l_returnflag"),  # implicitly output because this is a group by key
                #F.col("l_linestatus"),  # implicitly output because this is a group by key
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
    group_by_counts.append(group_by_count)

# Output of query step:
# 1. out: List[DataFrame] - results of running the query $SAMPLES times
# 2. group_by_counts: List[DataFrame] - number of rows contributing to each group in each sample

### Account for Missing Groups
# For all outputs, if any groups are missing from the output, add them in with 0 values
"""                                                         
 Output of this sample is               Missing groups are added with 
 missing groups present when            zeroes                        
 query run on other samples                                           
┌────────────┬──────┬──────┐            ┌────────────┬──────┬──────┐  
│group_by_key│ col1 │ col2 │            │group_by_key│ col1 │ col2 │  
├────────────┼──────┼──────┤            ├────────────┼──────┼──────┤  
│A           │  •   │  •   │            │A           │  •   │  •   │  
└────────────┴──────┴──────┘            ├────────────┼──────┼──────┤  
                              ───────▶  │B           │  0   │  0   │  
┌────────────┬──────┬──────┐            ├────────────┼──────┼──────┤  
│C           │  •   │  •   │            │C           │  •   │  •   │  
└────────────┴──────┴──────┘            └────────────┴──────┴──────┘  
"""

# First we run the query once without any sampling to get the true output format with all possible groups present
GROUP_BY_KEYS = ["l_returnflag", "l_linestatus"]
df_unsampled_output = (df2
                        .groupBy(*GROUP_BY_KEYS)
                        .agg(
                            #F.col("l_returnflag"),  # implicitly output because this is a group by key
                            #F.col("l_linestatus"),  # implicitly output because this is a group by key
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

# Build a template: Zero out everything but the group-by keys
template = df_unsampled_output.select(
    *GROUP_BY_KEYS, # leave grouped-by columns unchanged
    *[F.lit(0).alias(col) for col in df_unsampled_output.columns if col not in GROUP_BY_KEYS] # set all other columns to zeroes
)

# Now apply the template to all of the output DataFrames. If a group is missing from the output, it will be added in with 0 values
for o in out:
    o = (o.union(template)  # append the zeroed-out rows to the output
         .groupBy(*GROUP_BY_KEYS)  # deduplicate the output, preferring the original values
         .agg(*[F.first(col).alias(col) for col in o.columns if col not in GROUP_BY_KEYS]))
    
    # We'll re-sort so that the order of the groups doesn't give anything away.
    o = o.sort("l_returnflag", "l_linestatus")  # TODO generalize

# TODO: account for missing groups for group_by_counts too
for group_by_count in group_by_counts:
#     group_by_count = (group_by_count.union(template)  # append the zeroed-out rows to the output
#          .groupBy(*GROUP_BY_KEYS)  # deduplicate the output, preferring the original values
#          .agg(*[F.first(col).alias(col) for col in group_by_count.columns if col not in GROUP_BY_KEYS]))
    
    # We'll re-sort so that the order of the groups doesn't give anything away.
    group_by_count = group_by_count.sort("l_returnflag", "l_linestatus")  # TODO generalize

# Output of this step:
# 1. out: List[DataFrame] - results of running the query $SAMPLES times, with all groups present in each output

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

# Input to PAC noise step:
# 1. out_np: List[np.ndarray] - the outputs of the query as numpy arrays
# 2. SAMPLES: int - the number of samples taken

max_mi: float = 1./4

dimensions: int = len(out_np[0])
proj_matrix: np.ndarray = np.eye(dimensions)
out_np_2darr = [np.atleast_2d(o) for o in out_np] # make sure all the DF -> np.ndarray conversions result in 2d arrays

# est_y[i] is a numpy array of magnitudes of the outputs in the i-th basis direction
est_y: np.ndarray = np.stack(out_np_2darr, axis=-1).reshape(dimensions, SAMPLES)

# get the variance in each basis direction
fin_var: np.ndarray = np.var(est_y, axis=1)  # shape (dimensions,)
sqrt_total_var: np.floating[Any] = np.sum(np.sqrt(fin_var))

pac_noise: np.ndarray = (1./(2*max_mi)) * sqrt_total_var * np.sqrt(fin_var)  # variance of the PAC noise

pac_noises_to_add: np.ndarray = np.random.normal(loc=0, scale=pac_noise)

#print("pac_noises_to_add", pac_noises_to_add)

# Add noise element-wise to the outputs
pac_release = out_np[0] + pac_noises_to_add
print("pac_release", pac_release)

# Output of PAC Noise step:
# 1. pac_release: np.ndarray - the PAC release of the query output

def updateDataFrame(vec: np.ndarray, df: DataFrame) -> DataFrame:
    """
    Use the values of the vector to update the PySpark DataFrame.
    """

    # Recompute shape and columns
    numeric_columns: List[str] = [f.name for f in df.schema.fields if isinstance(f.dataType, T.NumericType)]
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

# Update the DataFrame with the noisy output
noisy_output_df = updateDataFrame(pac_release, template)
noisy_output_df.show()

### Count thresholding

# If the number of rows from the original table contributing to any of the groups is smaller than the threshold, we omit the group
COUNT_THRESHOLD = 2000

DEFAULT_PAC_VALUE = 0

MI_EPS_MAPPING = {
    1./4: 1.64,
    1./16: 0.73,
    1./64: 0.36
}

# *********************Hybrid-DP Noise******************************
# Step 1: Check group
# Since 0th sample was taken for PAC release
print("Chai Debug: Add DP Noise to:")
true_group_by_count_df = group_by_counts[0]

true_group_by_count_np = unwrapDataFrame(true_group_by_count_df)
print(true_group_by_count_np)

# Step 2: Add DP-noise to each group
epsilon = MI_EPS_MAPPING[max_mi]
sensitivity = 1 # since we are calculating count, sensitivity is 1
scale = sensitivity / epsilon
dp_noise = np.random.laplace(0, scale, 1)[0]

dp_noisy_group_by_count_np = true_group_by_count_np + dp_noise
print("Chai Debug: Noisy DP group by counts are", dp_noisy_group_by_count_np)

# Step 3: Check if noisy_group_by_count is less than threshold -- this is the algorithm
flags = []
for noisy_group in dp_noisy_group_by_count_np:
    if noisy_group < COUNT_THRESHOLD:
        flags.append(1)
    else:
        flags.append(0)
print("Chai Debug: Flags are", flags)

noisy_output_df_with_index = noisy_output_df.withColumn("id", F.monotonically_increasing_id())

idx_ = 0
for flag in flags:
    if flag == 1:
        # Set 'value' to 0 for the specified row
        noisy_output_df_with_index = noisy_output_df_with_index.withColumn(
            "sum_qty", 
            F.when(noisy_output_df_with_index["id"] == idx_, 0).otherwise(noisy_output_df["sum_qty"])
        ).withColumn(
            "sum_base_price", 
            F.when(noisy_output_df_with_index["id"] == idx_, 0).otherwise(noisy_output_df["sum_base_price"])
        ).withColumn(
            "sum_disc_price", 
            F.when(noisy_output_df_with_index["id"] == idx_, 0).otherwise(noisy_output_df["sum_disc_price"])
        ).withColumn(
            "sum_charge", 
            F.when(noisy_output_df_with_index["id"] == idx_, 0).otherwise(noisy_output_df["sum_charge"])
        ).withColumn(
            "avg_qty", 
            F.when(noisy_output_df_with_index["id"] == idx_, 0).otherwise(noisy_output_df["avg_qty"])
        ).withColumn(
            "avg_price", 
            F.when(noisy_output_df_with_index["id"] == idx_, 0).otherwise(noisy_output_df["avg_price"])
        ).withColumn(
            "avg_disc", 
            F.when(noisy_output_df_with_index["id"] == idx_, 0).otherwise(noisy_output_df["avg_disc"])
        ).withColumn(
            "count_order", 
            F.when(noisy_output_df_with_index["id"] == idx_, 0).otherwise(noisy_output_df["count_order"])
        )

    idx_ += 1
        
# *********************Hybrid-DP Noise******************************
# noisy_output_df_with_index.show()
release_df = noisy_output_df_with_index.drop("id")
release_df.show()
