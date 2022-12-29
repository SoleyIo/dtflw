import unittest
from unittest.mock import patch
from ddt import ddt, data, unpack
from dtflw.io.azure import AzureStorage
from dtflw.logger import DefaultLogger
from dtflw.flow_context import FlowContext
from dtflw.output_table import OutputTable


@ddt
class OutputTableTestCase(unittest.TestCase):

    def __mock_dbutils(self, fs_ls_raises_error=False):
        # Custom mock, since pyspark.dbutils.DBUtils is not available.

        def dbutils_fs_ls(path):
            if fs_ls_raises_error:
                raise Exception("java.io.FileNotFoundException")

        class MockObj:
            pass
        dbutils_mock = MockObj()
        dbutils_mock.fs = MockObj()
        dbutils_mock.fs.ls = dbutils_fs_ls

        return dbutils_mock

    def __mock_spark(self, spark_class_mock):
        spark_mock = spark_class_mock.return_value()

        def spark_read_parquet(path, df_dtypes=[("id", "bigint")]):
            # Mock the real behavior.

            with patch("pyspark.sql.dataframe.DataFrame") as DataFrameMock:
                df_mock = DataFrameMock.return_value()
                df_mock.dtypes = df_dtypes
                return df_mock

        spark_mock.read.parquet = spark_read_parquet
        return spark_mock

    @patch("pyspark.sql.session.SparkSession")
    def test_validate_succeeds(self, spark_class_mock):

        # Arrange

        dbutils_mock = self.__mock_dbutils()
        spark_mock = self.__mock_spark(spark_class_mock)

        o = OutputTable(
            name="foo",
            abs_file_path="foo.parquet",
            cols=[("id", "bigint")],
            ctx=FlowContext(
                storage=AzureStorage("account", "container",
                                     "", spark_mock, dbutils_mock),
                spark=spark_mock,
                dbutils=dbutils_mock,
                logger=DefaultLogger()
            )
        )

        # Act/Assert

        # Output is valid means that the file exists and columns are compatible.
        o.validate()

    @patch("pyspark.sql.session.SparkSession")
    def test_validate_fails_file_does_not_exist(self, spark_class_mock):

        # Arrange

        dbutils_mock = self.__mock_dbutils(fs_ls_raises_error=True)
        spark_mock = self.__mock_spark(spark_class_mock)

        o = OutputTable(
            name="foo",
            abs_file_path="foo.parquet",
            cols=[("id", "bigint")],
            ctx=FlowContext(
                storage=AzureStorage("account", "container",
                                     "", spark_mock, dbutils_mock),
                spark=spark_mock,
                dbutils=dbutils_mock,
                logger=DefaultLogger()
            )
        )

        # Act/Assert

        with self.assertRaises(Exception):
            o.validate()

    @patch("pyspark.sql.session.SparkSession")
    def test_validate_fails_columns_are_incompatible(self, spark_class_mock):

        # Arrange

        dbutils_mock = self.__mock_dbutils(fs_ls_raises_error=False)
        spark_mock = self.__mock_spark(spark_class_mock)

        o = OutputTable(
            name="foo",
            abs_file_path="foo.parquet",
            # Expected columns
            cols=[("name", "string")],
            ctx=FlowContext(
                storage=AzureStorage("account", "container",
                                     "", spark_mock, dbutils_mock),
                spark=spark_mock,
                dbutils=dbutils_mock,
                logger=DefaultLogger()
            )
        )

        # Act/Assert

        with self.assertRaises(AssertionError):
            o.validate()

    @data(True, False)
    def test_needs_eval(self, expected_needs_eval):

        # Arrange

        dbutils_mock = self.__mock_dbutils(
            fs_ls_raises_error=expected_needs_eval)

        o = OutputTable(
            name="foo",
            abs_file_path="foo.parquet",
            cols=None,
            ctx=FlowContext(
                storage=AzureStorage("account", "container",
                                     "", None, dbutils_mock),
                spark=None,
                dbutils=dbutils_mock,
                logger=DefaultLogger()
            )
        )

        self.assertEqual(o.needs_eval(), expected_needs_eval)
