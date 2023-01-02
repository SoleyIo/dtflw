import unittest
from unittest.mock import patch
from ddt import ddt, data
from dtflw.logger import DefaultLogger
from dtflw.flow_context import FlowContext
from dtflw.output_table import OutputTable
import tests.utils as utils


@ddt
class OutputTableTestCase(unittest.TestCase):

    @patch("pyspark.sql.session.SparkSession")
    def test_validate_succeeds(self, spark_class_mock):

        # Arrange

        dbutils_mock = utils.mock_dbutils()
        spark_mock = utils.mock_spark(spark_class_mock)

        o = OutputTable(
            name="foo",
            abs_file_path="foo.parquet",
            cols=[("id", "bigint")],
            ctx=FlowContext(
                storage=utils.StorageMock("account", "container",
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

        dbutils_mock = utils.mock_dbutils(fs_ls_raises_error=True)
        spark_mock = utils.mock_spark(spark_class_mock)

        o = OutputTable(
            name="foo",
            abs_file_path="foo.parquet",
            cols=[("id", "bigint")],
            ctx=FlowContext(
                storage=utils.StorageMock("account", "container",
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

        dbutils_mock = utils.mock_dbutils(fs_ls_raises_error=False)
        spark_mock = utils.mock_spark(spark_class_mock)

        o = OutputTable(
            name="foo",
            abs_file_path="foo.parquet",
            # Expected columns
            cols=[("name", "string")],
            ctx=FlowContext(
                storage=utils.StorageMock("account", "container",
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

        dbutils_mock = utils.mock_dbutils(
            fs_ls_raises_error=expected_needs_eval)

        o = OutputTable(
            name="foo",
            abs_file_path="foo.parquet",
            cols=None,
            ctx=FlowContext(
                storage=utils.StorageMock("account", "container",
                                     "", None, dbutils_mock),
                spark=None,
                dbutils=dbutils_mock,
                logger=DefaultLogger()
            )
        )

        self.assertEqual(o.needs_eval(), expected_needs_eval)
