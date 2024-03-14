import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession, DataFrame

from pacdb import PACDataFrame

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

def test_create_pacdf(spark_fixture):
    spark = spark_fixture

    lung_df = spark.read.parquet("./data/lung.parquet")
    print(lung_df.show(5))

    pac_lung_df = PACDataFrame(lung_df)

    assert pac_lung_df.df == lung_df