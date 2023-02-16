import unittest
from dtflw import init_args, init_inputs, init_outputs
from unittest.mock import patch
import tests.utils as utils
from ddt import ddt, data, unpack
import dtflw.com as com
import dtflw.arguments as a


@ddt
class ArgumentsTestCase(unittest.TestCase):

    @data(
        (a.Argument, True),
        (a.Input, False),
        (a.Output, False)
    )
    @unpack
    @patch("dtflw.databricks.get_dbutils")
    def test_initialize_arguments_no_values(self, argument_type, expected_has_value, get_dbutils_mock):

        get_dbutils_mock.return_value = utils.DButilsMock()

        actual = argument_type.create("foo")

        self.assertDictEqual(
            {a.name: a.value for name, a in actual.items()},
            {"foo": ""}
        )

        self.assertEqual(actual["foo"].has_value, expected_has_value)

    @data(a.Argument, a.Input, a.Output)
    @patch("dtflw.databricks.get_dbutils")
    def test_initialize_arguments_non_empty_values(self, argument_type, get_dbutils_mock):

        get_dbutils_mock.return_value = utils.DButilsMock()

        actual = argument_type.create(
            {"a": "data.parquet", "b": 42, "c": None, "d": True}
        )

        self.assertDictEqual(
            {a.name: a.value for name, a in actual.items()},
            {"a": "data.parquet", "b": "42", "c": "None", "d": "True"}
        )

        self.assertTrue(
            all([a.has_value for a in actual.values()])
        )

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.databricks.get_spark_session")
    @patch("dtflw.databricks.get_dbutils")
    def test_init_arguments_from_shared_values(self, get_dbutils_mock, get_session_mock, get_this_notebook_abs_path_mock):

        # Arrange
        get_dbutils_mock.return_value = utils.DButilsMock()
        get_session_mock.return_value = utils.SparkSessionMock()

        notebook_abs_path = "/Repos/user@a.b/project/nb"
        get_this_notebook_abs_path_mock.return_value = notebook_abs_path

        ch = com.NotebooksChannel()
        ch.share_args(notebook_abs_path, {"foo": "1"})
        ch.share_inputs(notebook_abs_path, {"bar": "2"})
        ch.share_outputs(notebook_abs_path, {"baz": "3"})

        # Act
        args = init_args("foo")
        inputs = init_inputs("bar")
        outputs = init_outputs("baz")

        # Assert
        self.assertEqual("1", args["foo"].value)
        self.assertEqual("2", inputs["bar"].value)
        self.assertEqual("3", outputs["baz"].value)
