import numpy as np
import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from pacdb import PACDataFrame, PACOptions, SamplerOptions

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

def test_unwrap_df_scalar(spark_fixture):
    spark = spark_fixture

    df = spark.read.csv("./data/student_performance/student-mat.csv", header=True, inferSchema=True, sep=";")

    def query(df:DataFrame) -> DataFrame:
        return df.filter(df["absences"] >= 5).agg(F.count("*"))
    
    assert query(df).toJSON().collect() == ['{"count(1)":151}']

    assert np.array_equiv(PACDataFrame._unwrapDataFrame(query(df)), np.array([151]))

def test_unwrap_df_vector(spark_fixture):
    spark = spark_fixture

    df = spark.read.csv("./data/student_performance/student-mat.csv", header=True, inferSchema=True, sep=";")

    def query(df:DataFrame) -> DataFrame:
        return df.groupBy(F.col("guardian")).agg(F.count("*"))
    
    assert query(df).toJSON().collect() == ['{"guardian":"father","count(1)":90}',
        '{"guardian":"mother","count(1)":273}', '{"guardian":"other","count(1)":32}']
    
    assert np.array_equiv(PACDataFrame._unwrapDataFrame(query(df)), np.array([90, 273, 32]))

def test_unwrap_df_matrix(spark_fixture):
    spark = spark_fixture

    df = spark.read.csv("./data/student_performance/student-mat.csv", header=True, inferSchema=True, sep=";")

    def query(df:DataFrame) -> DataFrame:
        return df.groupBy(F.col("guardian")).agg(F.avg("absences"), F.max("absences"))
    
    assert query(df).toJSON().collect() == ['{"guardian":"father","avg(absences)":3.977777777777778,"max(absences)":21}',
        '{"guardian":"mother","avg(absences)":5.835164835164835,"max(absences)":75}',
        '{"guardian":"other","avg(absences)":9.5,"max(absences)":40}']
    
    answer = PACDataFrame._unwrapDataFrame(query(df))
    correct = np.array([3.97777778, 5.83516484, 9.5, 21., 75., 40.])
    assert np.allclose(answer, correct)
    

    
