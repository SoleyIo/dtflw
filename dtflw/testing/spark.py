import unittest
from dtflw.databricks import get_dbutils, get_spark_session


class SparkTestCase(unittest.TestCase):
    """
    Inherit from this class to implement a test case requiring a Spark session.
    """

    def setUp(self):
        self.spark = get_spark_session()
        self.dbutils = get_dbutils()

    def assert_dataframes_same(self, expected_df, actual_df):
        """
        Asserts an expected and an actual dataframes being same.
        Disregards columns and rows order.
        """

        self.assertIsNotNone(expected_df)
        self.assertIsNotNone(actual_df)

        # Assert columns same
        self.assertCountEqual(expected_df.dtypes, actual_df.dtypes)

        # Assert rows count equal
        expected_df.coalesce(1)
        expected_rows = [r.asDict() for r in expected_df.collect()]

        actual_df.coalesce(1)
        actual_rows = [r.asDict() for r in actual_df.collect()]

        self.assertEqual(len(expected_rows), len(actual_rows))

        # Assert content same
        self.assertCountEqual(expected_rows, actual_rows)
