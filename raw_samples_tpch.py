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

from pacdb import PACDataFrame

query_name: str = "TPCH-Q1"
budget_list: List[float] = [1/64, 1/32, 1/16, 1/8, 1/4, 1/2, 1., 2., 4.]
sample_size: int = 3
sampling_rate: float = 0.5
m: int = 10
c: float = 1e-6
mi: float = 1./4

# load tpch tables from data/tpch/*.parquet
# tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
tables = ["lineitem"]
dfs = {}
for table in tables:
    dfs[table] = spark.read.parquet(f"./data/tpch/{table}.parquet")

def subsample(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return df.sample(withReplacement=False, fraction=sampling_rate)

# pre-filter before subsampling
# try to optimize this query, probably 10 optimizations possible
df2 = dfs["lineitem"].filter(F.col("l_shipdate") <= "1998-09-02")

def query(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return spark.sql("""select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
        from
            {df}
        where
            l_shipdate <= date('1998-09-02')
        group by
            l_returnflag,
            l_linestatus
        order by
            l_returnflag,
            l_linestatus
    """, df=df)


print("Query output:")
query(dfs["lineitem"]).show()

def sample_once() -> np.ndarray:
    """ Generate one query output for use in hybrid algorithm """
    return PACDataFrame._unwrapDataFrame(dfs["lineitem"].transform(subsample).transform(query))

#print(sample_once())
#print(sample_once())

#PACDataFrame.fromDataFrame(dfs["lineitem"]).withQuery(query).releaseValue().show()
# create csv file of np.ndarray from calling sample_once

# with open("tpch_q1_raw_results.csv", "a") as f:
#    for i in tqdm(range(10000000)):
#        s = sample_once().tolist()
#        f.write(f"{s}\n")
#        if i % 10 == 0:
#            f.flush()
        
        
