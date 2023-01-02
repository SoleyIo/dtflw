import unittest
from dtflw.logger import DefaultLogger
from dtflw.flow_context import FlowContext
from dtflw.input_table import InputTable
import tests.utils as utils


class InputTableTestCase(unittest.TestCase):

    def test_validate_succeeds(self):

        input_name = "foo"
        input_file_name = f"{input_name}.parquet"
        input_file_path = f"wasbs://container@account.blob.core.windows.net/{input_file_name}"

        dbutils_mock = utils.mock_dbutils(
            fs_ls_raises_error=False,
            ls_return=[(input_file_name, input_file_path)]
        )
        storage = utils.StorageMock("account", "container", "", None, dbutils_mock)

        i = InputTable(
            name=input_name,
            abs_file_path=input_file_path,
            ctx=FlowContext(
                storage=storage,
                spark=None,
                dbutils=dbutils_mock,
                logger=DefaultLogger()
            )
        )

        i.validate()

    def test_validate_fails_file_path_not_given(self):

        i = InputTable(name="foo", abs_file_path=None, ctx=None)

        with self.assertRaises(Exception):
            i.validate()

    def test_validate_fails_file_does_not_exist(self):

        dbutils_mock = utils.mock_dbutils(fs_ls_raises_error=True)
        storage = utils.StorageMock("account", "container", "", None, dbutils_mock)

        i = InputTable(
            name="foo",
            abs_file_path=storage.get_abs_path("foo.parquet"),
            ctx=FlowContext(
                storage=storage,
                spark=None,
                dbutils=dbutils_mock,
                logger=DefaultLogger()
            )
        )

        with self.assertRaises(Exception):
            i.validate()
