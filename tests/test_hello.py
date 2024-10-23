import pytest
from pyspark.sql import SparkSession
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