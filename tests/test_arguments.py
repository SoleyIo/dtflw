import unittest
from dtflw import init_args, init_inputs, init_outputs
from unittest.mock import patch
import tests.utils as utils
from ddt import ddt, data, unpack
import dtflw.com as com
import dtflw.arguments as a


@ddt
class ArgumentTestCase(unittest.TestCase):

    @data(
        (a.Argument, True),
        (a.Input, False),
        (a.Output, False)
    )
    @unpack
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.databricks.get_spark_session")
    @patch("dtflw.databricks.get_dbutils")
    def test_create_with_names_no_values(self, argument_type, has_value, get_dbutils_mock, get_spark_session_mock, get_this_notebook_abs_path_mock):
        get_dbutils_mock.return_value = utils.DButilsMock()
        get_spark_session_mock.return_value = utils.SparkSessionMock()

        notebook_abs_path = "/Repos/user@a.b/project/nb"
        get_this_notebook_abs_path_mock.return_value = notebook_abs_path

        actual = argument_type.create("foo", "bar")

        self.assertEqual(2, len(actual))

        self.assertEqual(actual["foo"].value, "")
        self.assertEqual(actual["foo"].has_value, has_value)

        self.assertEqual(actual["bar"].value, "")
        self.assertEqual(actual["bar"].has_value, has_value)

    @data(a.Argument, a.Input, a.Output)
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.databricks.get_dbutils")
    def test_create_with_names_and_default_values(self, argument_type, get_dbutils_mock, get_this_notebook_abs_path_mock):
        get_dbutils_mock.return_value = utils.DButilsMock()

        notebook_abs_path = "/Repos/user@a.b/project/nb"
        get_this_notebook_abs_path_mock.return_value = notebook_abs_path

        actual = argument_type.create({"foo": "a", "bar": "b"})

        self.assertEqual(2, len(actual))

        self.assertEqual(actual["foo"].value, "a")
        self.assertTrue(actual["foo"].has_value)

        self.assertEqual(actual["bar"].value, "b")
        self.assertTrue(actual["bar"].has_value)

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.databricks.get_spark_session")
    @patch("dtflw.databricks.get_dbutils")
    def test_create_with_names_and_shared_values(self, get_dbutils_mock, get_spark_session_mock, get_this_notebook_abs_path_mock):
        get_dbutils_mock.return_value = utils.DButilsMock()
        get_spark_session_mock.return_value = utils.SparkSessionMock()

        notebook_abs_path = "/Repos/user@a.b/project/nb"
        get_this_notebook_abs_path_mock.return_value = notebook_abs_path

        ch = com.NotebooksChannel()
        ch.share_args(
            notebook_abs_path,
            {"a": "1"}
        )
        ch.share_inputs(
            notebook_abs_path,
            {"c": "3"}
        )
        ch.share_outputs(
            notebook_abs_path,
            {"e": "5"}
        )

        args = init_args("a", "b")
        inputs = init_inputs("c", "d")
        outputs = init_outputs("e", "f")

        self.assertEqual(args["a"].value, "1")
        self.assertTrue(args["a"].has_value)
        # There is no value for "b" shared.
        self.assertEqual(args["b"].value, "")
        self.assertTrue(args["b"].has_value)

        self.assertEqual(inputs["c"].value, "3")
        self.assertTrue(inputs["c"].has_value)
        # There is no value for "d" shared.
        self.assertEqual(inputs["d"].value, "")
        self.assertFalse(inputs["d"].has_value)

        self.assertEqual(outputs["e"].value, "5")
        self.assertTrue(outputs["e"].has_value)
        # There is no value for "f" shared.
        self.assertEqual(outputs["f"].value, "")
        self.assertFalse(outputs["f"].has_value)
