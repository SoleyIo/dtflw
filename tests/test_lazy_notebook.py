import re
from unittest.mock import patch
import pyspark
from dtflw.events import EventHandlerBase, FlowEvents
from dtflw.flow import Flow
from dtflw.logger import DefaultLogger
from ddt import ddt, data, unpack
import dtflw.databricks
from dtflw.lazy_notebook import LazyNotebook
from dtflw.flow_context import FlowContext
import dtflw.databricks
import unittest


@ddt
class LazyNotebookTestCase(unittest.TestCase):

    def setUp(self):
        self._clean_files = []

        self._df = self.spark.createDataFrame(
            [("1", 42)],
            ["id", "number"])

        self.ctx = FlowContext(self.storage, self.spark,
                               self.dbutils, DefaultLogger())

        self.child_nb_abs_path = "/Users/dude@soleynet.soley.io/project/nb_02"

        self.child_nb_args_temp_view_name = re.sub("[^a-zA-Z0-9]", "_",
                                                   self.child_nb_abs_path
                                                   )

    def tearDown(self):
        for path in self._clean_files:
            self.dbutils.fs.rm(path, recurse=True)

        dtflw.databricks.get_spark_session().catalog.dropTempView(
            self.child_nb_args_temp_view_name)

        self.ctx.events.unsubscribe_all()

    def test_input_with_relative_path(self):

        rel_input_path = "custom/relative/path/input.txt"
        expected_input_path = self.ctx.storage.get_abs_path(
            rel_input_path
        )

        nb = LazyNotebook("nb", self.ctx)\
            .input("Table", file_path=rel_input_path)

        self.assertEqual(
            nb.get_inputs()["Table"].abs_file_path,
            expected_input_path
        )

    def test_input_with_absolute_path(self):

        expected_input_path = self.ctx.storage.get_abs_path(
            "custom/relative/path/input.txt"
        )

        nb = LazyNotebook("nb", self.ctx)\
            .input("Table", file_path=expected_input_path)

        self.assertEqual(
            nb.get_inputs()["Table"].abs_file_path,
            expected_input_path
        )

    def test_input_with_default_path(self):

        self.ctx.tables_repo.publish(
            {"Table": self.storage.get_abs_path("nb1/table.parquet")},
            "nb"
        )

        expected_input_path = self.ctx.resolve_table("Table", "nb2")

        nb = LazyNotebook("nb2", self.ctx).input("Table")

        self.assertEqual(
            nb.get_inputs()["Table"].abs_file_path,
            expected_input_path
        )

    def test_input_with_source_table(self):

        self.ctx.tables_repo.publish(
            {"MyAwesomeTable": self.storage.get_abs_path("nb1/table.parquet")},
            "nb"
        )

        expected_input_path = self.ctx.resolve_table("MyAwesomeTable", "nb2")

        nb = (
            LazyNotebook("nb2", self.ctx)
            .input("Table", source_table="MyAwesomeTable")
        )

        self.assertEqual(
            nb.get_inputs()["Table"].abs_file_path,
            expected_input_path
        )

    def test_run_failure_input_not_found(self):
        """
        Raises an exeption if a required input table was not found
        before a run.
        """

        with self.assertRaises(Exception):
            LazyNotebook("nb_01", self.ctx)\
                .input("mara")\
                .run(is_lazy=False)

    def test_output_with_relative_path(self):

        rel_output_path = "custom/relative/path/output.txt"
        expected_output_path = self.ctx.storage.get_abs_path(
            rel_output_path
        )

        nb = LazyNotebook("nb", self.ctx)\
            .output("Table", file_path=rel_output_path)

        self.assertEqual(
            nb.get_outputs()["Table"].abs_file_path,
            expected_output_path
        )

    def test_output_with_absolute_path(self):

        expected_output_path = self.ctx.storage.get_abs_path(
            "custom/relative/path/output.txt"
        )

        nb = LazyNotebook("nb", self.ctx)\
            .output("Table", file_path=expected_output_path)

        self.assertEqual(
            nb.get_outputs()["Table"].abs_file_path,
            expected_output_path
        )

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_output_with_default_path(self, get_this_notebook_abs_path_mock):

        get_this_notebook_abs_path_mock.return_value = "/Repos/user@a.b/project/main"

        expected_output_path = self.ctx.storage.get_abs_path(
            self.ctx.storage.get_path_in_root_dir(
                self.ctx.storage.get_path_with_file_extension(
                    "project/nb/Table"
                )
            )
        )

        nb = LazyNotebook("nb", self.ctx).output("Table")

        self.assertEqual(
            nb.get_outputs()["Table"].abs_file_path,
            expected_output_path
        )

    @patch("soley.utils.notebook.flow20.lazy_notebook.LazyNotebook._LazyNotebook__run_notebook")
    def test_run_failure_output_not_found(self, run_notebook_mock):
        """
        Raises an exeption if an expected output table was not found
        after a run.
        """
        # Arrange
        run_notebook_mock.return_value = None

        # Act/Assert
        with self.assertRaises(Exception):
            LazyNotebook("nb_01", self.ctx)\
                .output("product", cols=self._df.dtypes)\
                .run(is_lazy=False)

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_output_with_alias(self, get_this_notebook_abs_path_mock):

        get_this_notebook_abs_path_mock.return_value = "/Repos/user@a.b/project/main"

        expected_output_path = self.ctx.storage.get_abs_path(
            self.ctx.storage.get_path_in_root_dir(
                self.ctx.storage.get_path_with_file_extension(
                    "project/nb/Table"
                )
            )
        )

        nb = (
            LazyNotebook("nb", self.ctx)
            .output("Table", alias="TableAliased")
        )

        # Assert: the output points to the same actual file
        self.assertEqual(
            nb.get_outputs()["Table"].abs_file_path,
            expected_output_path
        )

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

    # TODO: move this one to plugins module.
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_eval_args_no_args(self, get_this_notebook_abs_path_mock):
        """
        If there are no arguments registered then a temp view will not be created.
        """

        # Arrange
        this_nb_abs_path = "/Users/dude@soleynet.soley.io/project/main"
        get_this_notebook_abs_path_mock.return_value = this_nb_abs_path

        material_in_abs_path = self.storage.get_abs_path(
            "nb_01/material.parquet")

        self.ctx.publish_tables({"material": material_in_abs_path}, "nb_01")

        flow = Flow(self.ctx)
        flow.install(EvalNotebookArgsPlugin())

        # Act
        # No args -> no temp view
        flow.notebook("nb_02").eval_args()

        # Assert
        with self.assertRaises(pyspark.sql.utils.AnalysisException) as cm:
            dtflw.databricks.get_spark_session().table(self.child_nb_args_temp_view_name)

    # TODO: move this one to plugins module.
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_eval_args_success(self, get_this_notebook_abs_path_mock):
        """
        Tests if arguments are properly evaluated and saved to a temp view
        on calling 'eval_args()'
        """

        # Arrange
        main_nb_abs_path = "/Users/dude@soleynet.soley.io/project/main"
        get_this_notebook_abs_path_mock.return_value = main_nb_abs_path

        material_in_abs_path = self.storage.get_abs_path(
            "nb_01/material.parquet")

        self.ctx.publish_tables({"material": material_in_abs_path}, "nb_01")

        flow = Flow(self.ctx)
        flow.install(EvalNotebookArgsPlugin())

        # Act
        flow.notebook("nb_02")\
            .timeout(10)\
            .args({"arg1": "arg 1 value"})\
            .input("material")\
            .output("product")\
            .eval_args()

        actual_args_temp_view_df = dtflw.databricks.get_spark_session().table(
            self.child_nb_args_temp_view_name)

        # Assert
        self.assert_dataframes_same(
            dtflw.databricks.get_spark_session().createDataFrame(
                [
                    ("arg1", "arg 1 value", ""),
                    ("material", material_in_abs_path,
                     LazyNotebook.INPUT_TABLE_SUFFIX),
                    ("product", self.storage.get_abs_path(
                        "project/nb_02/product.parquet"), LazyNotebook.OUTPUT_TABLE_SUFFIX),
                ],
                ["name", "value", "suffix"]),
            actual_args_temp_view_df)

    # TODO: move this one to plugins module.
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_eval_args_non_string_types(self, get_this_notebook_abs_path_mock):
        """
        Tests if arguments are properly evaluated and saved to a temp view
        on calling 'eval_args()'
        """

        # Arrange
        main_nb_abs_path = "/Users/dude@soleynet.soley.io/project/main"
        get_this_notebook_abs_path_mock.return_value = main_nb_abs_path

        material_in_abs_path = self.storage.get_abs_path(
            "nb_01/material.parquet")

        self.ctx.publish_tables({"material": material_in_abs_path}, "nb_01")

        from datetime import datetime
        dt_arg_value = datetime.now()

        flow = Flow(self.ctx)
        flow.install(EvalNotebookArgsPlugin())

        # Act
        (flow.notebook("nb_02")
            .args(
                {
                    "str_arg": "hello",
                    "dt_arg": dt_arg_value,
                    "int_arg": 1,
                    "double_arg": 42.01,
                    "none_arg": None,
                    "empty_list_arg": [],
                    "empty_dict_arg": {},
                    "empty_tuple_arg": (),
                    "list_arg": [1, 2, 3],
                    "dict_arg": {'a': 'b'},
                    "tuple_arg": ('a', 1, 42.01),
                })
            .input("unknown_input")
            .input("material")
            .output("product")
            .eval_args())

        actual_args_temp_view_df = dtflw.databricks.get_spark_session().table(
            self.child_nb_args_temp_view_name)

        # Assert
        self.assert_dataframes_same(
            dtflw.databricks.get_spark_session().createDataFrame(
                [
                    ("str_arg", "hello", ""),
                    ("dt_arg", dt_arg_value.isoformat(), ""),
                    ("int_arg", str(1), ""),
                    ("double_arg", str(42.01), ""),
                    ("none_arg", None, ""),
                    ("empty_list_arg", "[]", ""),
                    ("empty_dict_arg", "{}", ""),
                    ("empty_tuple_arg", "()", ""),
                    ("list_arg", str([1, 2, 3]), ""),
                    ("dict_arg", str({'a': 'b'}), ""),
                    ("tuple_arg", str(('a', 1, 42.01)), ""),

                    ("unknown_input", None, LazyNotebook.INPUT_TABLE_SUFFIX),
                    ("material", material_in_abs_path,
                     LazyNotebook.INPUT_TABLE_SUFFIX),
                    ("product", self.storage.get_abs_path(
                        "project/nb_02/product.parquet"), LazyNotebook.OUTPUT_TABLE_SUFFIX),
                ],
                ["name", "value", "suffix"]),
            actual_args_temp_view_df)

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
