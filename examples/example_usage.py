import pandas as pd
from pyspark.sql import SparkSession
import sys
sys.path.append('.') 
from src.duplicate_checker import DuplicateChecker

# Initialize PySpark session
spark = SparkSession.builder.master("local").appName("DuplicateCheckerExample").getOrCreate()

# Example with PySpark DataFrame
data_spark = [
    ('A', 'x'),
    ('A', 'x'),
    ('B', 'y'),
    ('C', 'z')
]
df_spark = spark.createDataFrame(data_spark, ['col_1', 'col_2'])

# Use DuplicateChecker on PySpark DataFrame
print("PySpark DataFrame - Checking for duplicates:")
result_spark = DuplicateChecker.check_duplicates(df_spark, ['col_1', 'col_2'])
result_spark['samples'].show()
print("results.....")

#Example with Pandas DataFrame
data_pandas = {
    'col_1': ['A', 'A', 'B', 'B', 'C'],
    'col_2': ['x', 'x', 'y', 'y', 'z']
}
df_pandas = pd.DataFrame(data_pandas)

# Use DuplicateChecker on Pandas DataFrame
print("Pandas DataFrame - Checking for duplicates:")
result_pandas = DuplicateChecker.check_duplicates(df_pandas, ['col_1', 'col_2'])
result_pandas['samples'].show()
print("results pandas.....")

#Stop Spark session
spark.stop()
