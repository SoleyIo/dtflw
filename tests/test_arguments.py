import unittest
from dtflw.arguments import initialize_arguments, Argument, Input, Output
from unittest.mock import patch
import tests.utils as utils
from ddt import ddt, data, unpack


@ddt
class ArgumentsTestCase(unittest.TestCase):

    @data(
        (Argument, True),
        (Input, False),
        (Output, False)
    )
    @unpack
    @patch("dtflw.databricks.get_dbutils")
    def test_initialize_arguments_no_values(self, clazz, expected_has_value, get_dbutils_mock):

        get_dbutils_mock.return_value = utils.DButilsMock()

        actual = initialize_arguments(clazz, "foo")

        self.assertDictEqual(
            {a.name: a.value for name, a in actual.items()},
            {"foo": ""}
        )

        self.assertEqual(actual["foo"].has_value, expected_has_value)

    @data(Argument, Input, Output)
    @patch("dtflw.databricks.get_dbutils")
    def test_initialize_arguments_non_empty_values(self, clazz, get_dbutils_mock):

        get_dbutils_mock.return_value = utils.DButilsMock()

        actual = initialize_arguments(
            clazz,
            {"a": "data.parquet", "b": 42, "c": None, "d": True}
        )

        self.assertDictEqual(
            {a.name: a.value for name, a in actual.items()},
            {"a": "data.parquet", "b": "42", "c": "None", "d": "True"}
        )

        self.assertTrue(
            all([a.has_value for a in actual.values()])
        )
