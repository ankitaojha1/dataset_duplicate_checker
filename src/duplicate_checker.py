import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Any


class DuplicateChecker:
    """
    DuplicateChecker can check for duplicates in either a PySpark or Pandas DataFrame.
    If a Pandas DataFrame is provided, it is automatically converted to a PySpark DataFrame.
    """

    @staticmethod
    def spark_session_builder() -> SparkSession:
        """
        Convert a Pandas DataFrame to a PySpark DataFrame.

        Args:
            None.

        Returns:
            SparkSession: A PySpark Session.
        """
        spark = SparkSession.builder.appName("DuplicateChecker").getOrCreate()
        return spark

    @staticmethod
    def convert_to_spark(df: pd.DataFrame) -> DataFrame:
        """
        Convert a Pandas DataFrame to a PySpark DataFrame.

        Args:
            df (pd.DataFrame): Pandas DataFrame to convert.

        Returns:
            DataFrame: A PySpark DataFrame.
        """
        #spark = SparkSession.builder.appName("DuplicateChecker").getOrCreate()
        return DuplicateChecker.spark_session_builder().createDataFrame(df)

    @staticmethod
    def check_duplicates(df: Any, columns: List[str]) -> Dict[str, Any]:
        """
        Check for duplicate rows in a PySpark or Pandas DataFrame based on the specified columns.

        Args:
            df (Any): The input DataFrame, which can be a Pandas DataFrame or PySpark DataFrame.
            columns (List[str]): The list of columns to check duplicates for.

        Returns:
            Dict[str, Any]: A dictionary with:
                - 'count': Number of duplicate occurrences.
                - 'samples': DataFrame with grouped duplicate rows and their counts.

        Raises:
            ValueError: If the input DataFrame or columns are invalid.
        """

        # If it's a Pandas DataFrame, convert it to PySpark DataFrame
        if isinstance(df, pd.DataFrame):
            if df.empty:
                raise ValueError("The input dataframe is empty.")
            else:
                df = DuplicateChecker.convert_to_spark(df)

        # Ensure it's now a PySpark DataFrame
        if not isinstance(df, DataFrame):
            raise ValueError("The input `df` must be a PySpark DataFrame or a Pandas DataFrame.")

        if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
            raise ValueError("The `columns` argument must be a list of strings.")

        if df.count() == 0:
            raise ValueError("The input dataframe is empty.")
            #return {"count": 0, "samples": df.limit(0)}

        if not all(col in df.columns for col in columns):
            raise ValueError("Some columns specified are not present in the DataFrame.")

        if len(columns) == 0:
            raise ValueError("The `columns` list should not be empty.")

        # Finding duplicates based on the specified columns
        duplicates_df = df.groupBy(columns).count().filter("count > 1")

        if duplicates_df.count() == 0:
            return {"count": 0, "samples": df.limit(0)}

        return {
            "count": duplicates_df.count(),
            "samples": duplicates_df
        }
