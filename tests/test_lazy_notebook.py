import unittest
from unittest.mock import MagicMock, patch
from dtflw.events import EventHandlerBase, FlowEvents
from dtflw.io.azure import AzureStorage
from dtflw.logger import DefaultLogger
from ddt import ddt, data, unpack
from dtflw.io.storage import file_exists
from dtflw.lazy_notebook import LazyNotebook
from dtflw.flow_context import FlowContext
import dtflw.databricks


@ddt
class LazyNotebookTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self._storage = AzureStorage("account", "container", "", None, None)
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
            LazyNotebook("nb", None).input(name=None)

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
            LazyNotebook("nb", None).output(name=None)

    # .run() tests

    @data(
        (True, False, False),
        (True, True, True),
        (False, False, True),
        (False, True, True),
    )
    @unpack
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.output_table.OutputTable.needs_eval")
    @patch("dtflw.output_table.OutputTable.validate")
    @patch("dtflw.lazy_notebook.LazyNotebook._LazyNotebook__run_notebook")
    def test_run(self, is_lazy, outputs_need_eval, is_expected_running, run_actually_mock, output_validate_mock, output_needs_eval_mock, get_this_notebook_abs_path_mock):
        """
            Flow does not run a notebook
                if is_lazy is True AND all outputs are evaluated.

            Flow runs a notebook
                if at least one output needs to be evaluated OR is_lazy is False.
        """
        # Arrange
        get_this_notebook_abs_path_mock.return_value = "/Repos/a@b.c/project/main"
        output_needs_eval_mock.return_value = outputs_need_eval

        def do_nothing(strict):
            pass
        output_validate_mock.side_effect = do_nothing

        # Act
        (
            LazyNotebook("nb", self._ctx)
            .output("foo")
            .run(is_lazy=is_lazy, strict_validation=False)
        )

        if is_expected_running:
            run_actually_mock.assert_called_once()
        else:
            run_actually_mock.assert_not_called()

    def test_run_failure_input_not_found(self):
        """
        Raises an exeption if a required input table was not found
        before a run.
        """

        nb = LazyNotebook("nb", self._ctx).input("foo")

        with self.assertRaisesRegex(Exception, "Required input not found."):
            nb.run(is_lazy=False)

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("dtflw.output_table.OutputTable.needs_eval")
    @patch("dtflw.lazy_notebook.LazyNotebook._LazyNotebook__run_notebook")
    def test_run_failure_output_not_found(self, run_actually_mock: MagicMock, output_needs_eval_mock, get_this_notebook_abs_path_mock):
        """
        Raises an exeption if an expected output table was not found
        after a run.
        """
        # Arrange
        get_this_notebook_abs_path_mock.return_value = "/Repos/a@b.c/project/main"
        output_needs_eval_mock.return_value = True

        def do_nothing(path: str, timeout: int, args: dict, ctx: FlowContext):
            pass

        run_actually_mock.side_effect = do_nothing

        nb = LazyNotebook("nb", self._ctx).output("foo")

        # Act/Assert
        with self.assertRaisesRegex(Exception, "Expected output not found."):
            nb.run(is_lazy=False)

    @data(
        (True, False),
        (True, True),
        (False, False),
        (False, True),
    )
    @unpack
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_run_success(self, is_nb_lazy, output_needs_eval, get_this_notebook_abs_path_mock):
        """
        Lazy bahavior:

        Flow skips running a notebook
            if is_lazy is True and no outputs need to be evaluated.

        Flow runs a notebook
            if at least one output needs evaluation or is_lazy is False.
        """
        # Arrange

        # Unpublished required input
        unpublished_in_abs_path = self.storage.get_abs_path(
            "nb_01/unpublished.parquet")
        self._df.write.mode("overwrite").parquet(unpublished_in_abs_path)
        self._clean_files.append(unpublished_in_abs_path)

        # Publish a required input
        material_in_abs_path = self.storage.get_abs_path(
            "nb_01/material.parquet")
        self._df.write.mode("overwrite").parquet(material_in_abs_path)
        self._clean_files.append(material_in_abs_path)

        self.ctx.publish_tables(
            {"material_aliased": material_in_abs_path}, "nb_01")

        # Prepare output
        nb_path = "nb_02"
        nb_timeout = 10
        nb_args = {"arg": "value"}

        product_out_abs_path = self.storage.get_abs_path(
            f"project/{nb_path}/product.parquet")

        if not output_needs_eval:
            self._df.write.parquet(product_out_abs_path)
            self._clean_files.append(product_out_abs_path)

        expected_result = None if is_nb_lazy and not output_needs_eval else "Success"

        def mimic_notebook_run(act_nb_path, act_timeout, act_args, ctx):
            # If it is a "skip run" case then not suppose to call this one.
            self.assertFalse(is_nb_lazy == True and output_needs_eval == False)

            self.assertEqual(act_nb_path, nb_path)
            self.assertEqual(act_timeout, nb_timeout)

            exp_args = {
                **nb_args,
                **{
                    f"material{LazyNotebook.INPUT_TABLE_SUFFIX}": material_in_abs_path,
                    f"unpublished{LazyNotebook.INPUT_TABLE_SUFFIX}": self.storage.get_abs_path("nb_01/unpub*.parquet")
                },
                **{f"product{LazyNotebook.OUTPUT_TABLE_SUFFIX}": product_out_abs_path}
            }
            self.assertEqual(act_args, exp_args)

            # Write "product" as an expected output
            self._df.write.mode("overwrite").parquet(product_out_abs_path)
            self._clean_files.append(product_out_abs_path)

            return expected_result

        # Override
        with patch("soley.utils.notebook.flow20.lazy_notebook.LazyNotebook._LazyNotebook__run_notebook", wraps=mimic_notebook_run) as run_notebook_mock:

            get_this_notebook_abs_path_mock.return_value = self.child_nb_abs_path

            flow = Flow(self.ctx)
            flow.subscribe(FlowEvents.NOTEBOOK_RUN_REQUESTED,
                           EvalNotebookArgsEventHandler())

            # Act
            actual_result = (
                flow.notebook(nb_path)
                    .args(nb_args)
                    .timeout(nb_timeout)
                    .input("material", source_table="material_aliased")
                    .input("unpublished", self.storage.get_abs_path("nb_01/unpub*.parquet"))
                    .output("product", cols=self._df.dtypes, alias="product_aliased")
                    .run(is_lazy=is_nb_lazy)
            )

            # Assert
            self.assertEqual(actual_result, expected_result)

            act_file_path = self.ctx.resolve_table("product_aliased", "nb_03")
            self.assertTrue(file_exists(act_file_path))

            # Test args temp view
            actual_eval_args_df = dtflw.databricks.get_spark_session().table(
                self.child_nb_args_temp_view_name)

            self.assert_dataframes_same(
                actual_eval_args_df,
                dtflw.databricks.get_spark_session().createDataFrame(
                    [
                        ("arg", "value", ""),
                        ("material", material_in_abs_path,
                         LazyNotebook.INPUT_TABLE_SUFFIX),
                        ("unpublished", self.storage.get_abs_path(
                            "nb_01/unpub*.parquet"), LazyNotebook.INPUT_TABLE_SUFFIX),
                        ("product", product_out_abs_path,
                         LazyNotebook.OUTPUT_TABLE_SUFFIX)
                    ],
                    ["name", "value", "suffix"]))

   # Events tests

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    @patch("soley.utils.notebook.flow20.lazy_notebook.LazyNotebook._LazyNotebook__run_notebook")
    def test_notebook_run_requested_event(self, run_notebook_mock, get_this_notebook_abs_path_mock):

        # Arrange
        get_this_notebook_abs_path_mock.return_value = self.child_nb_abs_path
        run_notebook_mock.return_value = 42

        class TestHandler(EventHandlerBase):
            def __init__(self):
                self.actual_nb = None
                self.counter = 0

            def handle(self, nb):
                self.actual_nb = nb
                self.counter += 1

        h = TestHandler()
        self.ctx.events.subscribe(FlowEvents.NOTEBOOK_RUN_REQUESTED, h)

        nb = LazyNotebook("nb_02", self.ctx)

        # Act
        nb.run()

        # Assert
        self.assertEqual(h.actual_nb, nb)
        self.assertEqual(h.counter, 1)
