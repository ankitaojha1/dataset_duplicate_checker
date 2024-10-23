import pytest
import pandas as pd
from pyspark.sql import SparkSession
import findspark
findspark.init()
findspark.find()
import sys
sys.path.append('.') 
from src.duplicate_checker import DuplicateChecker


# Fixture to create and tear down a SparkSession for testing
@pytest.fixture(scope="module")
def spark():
    # Set up Spark session for testing
    spark = SparkSession.builder.master("local[2]").appName("DuplicateCheckerTest").getOrCreate()
    yield spark
    spark.stop()


# Test case for no duplicates in PySpark DataFrame
def test_no_duplicates_spark(spark):
    data = [
        ('A', 'x'),
        ('B', 'y'),
        ('C', 'z')
    ]
    df = spark.createDataFrame(data, ['col_1', 'col_2'])
    result = DuplicateChecker.check_duplicates(df, ['col_1'])
    assert result['count'] == 0
    assert result['samples'].rdd.isEmpty()#, "There should be no duplicates."


# Test case for duplicates in a PySpark DataFrame
def test_with_duplicates_spark(spark):
    data = [
        ('A', 'x'),
        ('A', 'x'),
        ('B', 'y'),
        ('B', 'y'),
        ('C', 'z')
    ]
    df = spark.createDataFrame(data, ['col_1', 'col_2'])
    result = DuplicateChecker.check_duplicates(df, ['col_1', 'col_2'])
    assert result['count'] == 2
    assert result['samples'].count() == 2#, "There should be two duplicate groups."


# Test case for duplicates in a Pandas DataFrame
def test_with_duplicates_pandas(spark):
    data = {
        'col_1': ['A', 'A', 'B', 'B', 'C'],
        'col_2': ['x', 'x', 'y', 'y', 'z']
    }
    df = pd.DataFrame(data)
    result = DuplicateChecker.check_duplicates(df, ['col_1', 'col_2'])
    assert result['count'] == 2
    assert result['samples'].count() == 2#, "There should be two duplicate groups."


# Test case for an empty PySpark DataFrame
def test_empty_dataframe_spark(spark):
    df = spark.createDataFrame([], schema="col_1 STRING, col_2 STRING")
    with pytest.raises(ValueError, match="The input dataframe is empty."):
        DuplicateChecker.check_duplicates(df, ['col_1'])
    # result = DuplicateChecker.check_duplicates(df, ['col_1'])
    # assert result['count'] == 0
    # assert result['samples'].rdd.isEmpty()#, "Empty DataFrame should return 0 duplicates."


# Test case for an empty Pandas DataFrame
def test_empty_dataframe_pandas(spark):
    df = pd.DataFrame(columns=["col_1", "col_2"])
    with pytest.raises(ValueError, match="The input dataframe is empty."):
        DuplicateChecker.check_duplicates(df, ['col_1'])
#     result = DuplicateChecker.check_duplicates(df, ['col_1'])
#     assert result['count'] == 0
#     assert result['samples'].rdd.isEmpty(), "Empty DataFrame should return 0 duplicates."


# Test case for invalid column names in PySpark DataFrame
def test_invalid_columns_spark(spark):
    data = [
        ('A', 'x'),
        ('B', 'y')
    ]
    df = spark.createDataFrame(data, ['col_1', 'col_2'])
    with pytest.raises(ValueError, match="Some columns specified are not present in the DataFrame."):
        DuplicateChecker.check_duplicates(df, ['non_existent_col'])


# Test case for empty column list
def test_empty_column_list(spark):
    data = [
        ('A', 'x'),
        ('B', 'y')
    ]
    df = spark.createDataFrame(data, ['col_1', 'col_2'])
    with pytest.raises(ValueError, match="The `columns` list should not be empty."):
        DuplicateChecker.check_duplicates(df, [])
