import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession, DataFrame

import pacdb

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

def test_single_space(spark_fixture):
    spark = spark_fixture

    sample_data = [{"name": "John    D.", "age": 30},
                   {"name": "Alice   G.", "age": 25},
                   {"name": "Bob  T.", "age": 35},
                   {"name": "Eve   A.", "age": 28}]

    # Create a Spark DataFrame
    original_df = spark.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = pacdb.remove_extra_spaces(original_df, "name")

    expected_data = [{"name": "John D.", "age": 30},
    {"name": "Alice G.", "age": 25},
    {"name": "Bob T.", "age": 35},
    {"name": "Eve A.", "age": 28}]

    expected_df = spark.createDataFrame(expected_data)

    assertDataFrameEqual(transformed_df, expected_df)

@pytest.mark.skip(reason="From a previous version")
def test_add_attr(spark_fixture):
    spark = spark_fixture

    sample_data = [{"name": "John    D.", "age": 30},
                   {"name": "Alice   G.", "age": 25},
                   {"name": "Bob  T.", "age": 35},
                   {"name": "Eve   A.", "age": 28}]

    # Create a Spark DataFrame
    original_df = spark.createDataFrame(sample_data)
    original_df = original_df.dataframe_extension.toPACDataframe()

    # Apply the transformation function from before
    transformed_df = original_df.dataframe_extension.to_pandasss()

    expected_df = original_df.toPandas()

    assertDataFrameEqual(transformed_df, expected_df)