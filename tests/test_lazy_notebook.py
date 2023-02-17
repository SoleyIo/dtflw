import unittest
from unittest.mock import MagicMock, patch
from dtflw.logger import DefaultLogger
from ddt import ddt, data, unpack
from dtflw.lazy_notebook import LazyNotebook
from dtflw.flow_context import FlowContext
from tests import utils
from tests.utils import StorageMock


@ddt
class LazyNotebookTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self._storage = StorageMock(
            "account", "container", "", None, None)

        self._ctx = FlowContext(
            storage=self._storage,
            spark=None,
            dbutils=None,
            logger=DefaultLogger()
        )

    @data(
        # Input's abs path is None.
        (None, None),
        # Input's abs path gets resolved from earlier outputs.
        (None, "wasbs://container@account.blob.core.windows.net/foo.txt"),
        # Input's abs path gets built from the given relative path.
        ("bar/foo.txt", "wasbs://container@account.blob.core.windows.net/bar/foo.txt"),
        # Input's abs path is given explicitly.
        ("wasbs://container@account.blob.core.windows.net/foo.txt",
         "wasbs://container@account.blob.core.windows.net/foo.txt"),
        # Input's abs path gets resolved from the earlier output given by source_table.
        (None, "wasbs://container@account.blob.core.windows.net/baz.txt", "baz")
    )
    @unpack
    def test_input(self, file_path, expected_input_path, source_table=None):

        # Arrange

        if file_path is None and expected_input_path is not None:
            # The input table is expected to be resolved as an output of an earlier notebook.
            if source_table is None:
                # Case 1: search for a table with input's name.
                self._ctx.publish_tables(
                    {"foo": expected_input_path}, "some_earlier_notebook")
            else:
                # Case 2: search for a table with provided name.
                self._ctx.publish_tables(
                    {source_table: expected_input_path}, "some_earlier_notebook")

        # Act
        nb = LazyNotebook("nb", self._ctx).input(
            name="foo",
            file_path=file_path,
            source_table=source_table
        )

        # Assert

        inputs = nb.get_inputs()

        self.assertEqual(len(inputs), 1)
        self.assertIn("foo", inputs)
        self.assertEqual(inputs["foo"].name, "foo")
        self.assertEqual(inputs["foo"].abs_file_path, expected_input_path)

    def test_input_name_none_fails(self):

        with self.assertRaises(ValueError):
            LazyNotebook("nb", self._ctx).input(name=None)

    @data(
        # Default. Output's abs file path gets resolved from a path of the current notebook.
        (None,
         "wasbs://container@account.blob.core.windows.net/project/nb/foo.parquet"),
        # Outputs's path is given as a relative path.
        ("project/foo.parquet",
         "wasbs://container@account.blob.core.windows.net/project/foo.parquet"),
        # Outputs's path is given as an abs file path.
        ("wasbs://container@account.blob.core.windows.net/foo.parquet",
         "wasbs://container@account.blob.core.windows.net/foo.parquet")
    )
    @unpack
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_output_abs_file_path(self, file_path, expected_output_path, get_this_notebook_abs_path_mock):

        # Arrange
        get_this_notebook_abs_path_mock.return_value = "/Repos/a@b.c/project/main"

        # Act
        nb = LazyNotebook("nb", self._ctx).output(
            name="foo",
            cols=None,
            file_path=file_path,
            alias=None
        )

        # Assert
        outputs = nb.get_outputs()

        self.assertEqual(len(outputs), 1)
        self.assertIn("foo", outputs)
        self.assertEqual(outputs["foo"].name, "foo")
        self.assertEqual(outputs["foo"].abs_file_path, expected_output_path)
        self.assertEqual(outputs["foo"].alias, "foo")

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_output_alias(self, get_this_notebook_abs_path_mock):

        # Arrange
        get_this_notebook_abs_path_mock.return_value = "/Repos/a@b.c/project/main"

        # Act
        nb = LazyNotebook("nb", self._ctx).output(
            name="foo",
            cols=None,
            file_path=None,
            alias="FooAliased"
        )

        # Assert
        outputs = nb.get_outputs()

        # Assert
        self.assertIn("foo", outputs)
        self.assertEqual(outputs["foo"].alias, "FooAliased")

    def test_output_name_none_fails(self):

        with self.assertRaises(ValueError):
            LazyNotebook("nb", self._ctx).output(name=None)

    @data(
        (True, False, False),
        (True, True, True),
        (False, False, True),
        (False, True, True),
    )
    @unpack
    @patch("dtflw.databricks.get_spark_session")
    @patch("dtflw.databricks.is_job_interactive")
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.output_table.OutputTable.needs_eval")
    @patch("dtflw.output_table.OutputTable.validate")
    @patch("dtflw.databricks.run_notebook")
    def test_run(self, is_lazy, outputs_need_eval, is_expected_running, run_notebook_mock: MagicMock, output_validate_mock, output_needs_eval_mock, get_this_notebook_abs_path_mock, is_job_interactive_mock, get_session_mock):
        """
            Flow does not run a notebook
                if is_lazy is True AND all outputs are evaluated.

            Flow runs a notebook
                if at least one output needs to be evaluated OR is_lazy is False.
        """
        # Arrange
        get_this_notebook_abs_path_mock.return_value = "/Repos/a@b.c/project/main"
        output_needs_eval_mock.return_value = outputs_need_eval
        is_job_interactive_mock.return_value = True
        get_session_mock.return_value = utils.SparkSessionMock()

        def do_nothing(strict):
            pass
        output_validate_mock.side_effect = do_nothing

        # Act

        nb = LazyNotebook("nb", self._ctx).output("foo")
        nb.run(is_lazy=is_lazy, strict_validation=False)

        if is_expected_running:
            run_notebook_mock.assert_called_once_with(
                nb.rel_path,
                0,
                {
                    "foo_out": self._ctx.storage.get_abs_path("project/nb/foo.parquet")
                }
            )
        else:
            run_notebook_mock.assert_not_called()

    @patch("dtflw.databricks.is_job_interactive")
    def test_run_failure_input_not_found(self, is_job_interactive_mock):
        """
        Raises an exeption if a required input table was not found
        before a run.
        """
        is_job_interactive_mock.return_value = False

        nb = LazyNotebook("nb", self._ctx).input("foo")

        with self.assertRaisesRegex(Exception, "Required input not found."):
            nb.run(is_lazy=False)

    @patch("dtflw.databricks.get_spark_session")
    @patch("dtflw.databricks.is_job_interactive")
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.output_table.OutputTable.needs_eval")
    @patch("dtflw.databricks.run_notebook")
    def test_run_failure_output_not_found(self, run_notebook_mock: MagicMock, output_needs_eval_mock, get_this_notebook_abs_path_mock, is_job_interactive_mock, get_session_mock):
        """
        Raises an exeption if an expected output table was not found
        after a run.
        """
        # Arrange
        get_this_notebook_abs_path_mock.return_value = "/Repos/a@b.c/project/main"
        output_needs_eval_mock.return_value = True
        is_job_interactive_mock.return_value = True
        get_session_mock.return_value = utils.SparkSessionMock()

        def do_nothing(path: str, timeout: int, args: dict):
            pass

        run_notebook_mock.side_effect = do_nothing

        nb = LazyNotebook("nb", self._ctx).output("foo")

        # Act/Assert
        with self.assertRaisesRegex(Exception, "Expected output not found."):
            nb.run(is_lazy=False)

    @patch("dtflw.databricks.get_spark_session")
    @patch("dtflw.databricks.is_job_interactive")
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.databricks.run_notebook")
    def test_run_return_value(self, run_notebook_mock: MagicMock, get_this_notebook_abs_path_mock, is_job_interactive_mock, get_session_mock):
        """
        Raises an exeption if an expected output table was not found
        after a run.
        """

        # Arrange
        get_this_notebook_abs_path_mock.return_value = "/Repos/a@b.c/project/main"
        run_notebook_mock.return_value = "foo"
        is_job_interactive_mock.return_value = True
        get_session_mock.return_value = utils.SparkSessionMock()

        actual = LazyNotebook("nb", self._ctx).run(is_lazy=False)

        # Act/Assert
        self.assertEqual(actual, "foo")
