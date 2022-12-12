from unittest.mock import patch
from unittest import skip
from dtflw.flow_context import FlowContext
from dtflw.input_table import InputTable
from dtflw.lazy_notebook import LazyNotebook
from dtflw.logger import DefaultLogger
from dtflw.output_table import OutputTable
from dtflw.runtime import NotebookRun, Runtime
import dtflw.databricks
from dtflw.testing.spark import SparkTestCase


class RuntimeTestCase(SparkTestCase):

    def test_add_run(self):
        # Arrange
        runtime = Runtime()

        # Act
        r1 = NotebookRun(
            "notebook_1",
            {"arg": "val"},
            {"input": "input_path"},
            {"output": "output_path"}
        )
        runtime.add_run(r1)

        # Assert
        actual_run = list(runtime.runs)[0]

        self.assertEqual("notebook_1", actual_run.notebook_path)
        self.assertEqual({"arg": "val"}, actual_run.args)
        self.assertEqual({"input": "input_path"}, actual_run.inputs)
        self.assertEqual({"output": "output_path"}, actual_run.outputs)

    @patch("dtflw.databricks.get_this_notebook_abs_cwd")
    @patch("dtflw.lazy_notebook.LazyNotebook._LazyNotebook__run_notebook")
    def test_notebooks_runtime(self, run_notebook_mock, get_this_notebook_abs_cwd_mock):
        # Arrange
        run_notebook_mock.return_value = None
        get_this_notebook_abs_cwd_mock.return_value = "/Users/dude@soleynet.soley.io/project"

        InputTable.validate = (
            lambda *args, **kwargs: "Stub: do nothing."
        )

        OutputTable.validate = (
            lambda *args, **kwargs: "Stub: do nothing."
        )

        ctx = FlowContext(self.storage, self.spark,
                          self.dbutils, DefaultLogger())

        # Act

        nb1 = (
            LazyNotebook("01_import_material", ctx)
            .input("MARA", f"{self.storage.base_path}/MARA.parquet")
            .output("Material")
        )
        nb1.run()

        nb2 = (
            LazyNotebook("02_import_sales", ctx)
            .input("VBAK", f"{self.storage.base_path}/VBAK.parquet")
            .input("VBAP", f"{self.storage.base_path}/VBAP.parquet")
            .output("SalesOrderItem")
        )
        nb2.run()

        # Re-run the notebook.
        nb2 = (
            LazyNotebook("02_import_sales", ctx)
            # Re-run with different parameters: args and inputs
            .args({"foo": "bar"})
            .input("VBAK", f"{self.storage.base_path}/VBAK.parquet")
            # .input("VBAP", f"{self.storage.base_path}/VBAP.parquet")
            .output("SalesOrderItem")
        )
        nb2.run()

        nb3 = (
            LazyNotebook("03_calc_sales_stats", ctx)
            .input("Material")
            .input("SalesOrderItem")
            .output("SalesStats")
        )
        nb3.run()

        actual_runs = {r.notebook_path: r for r in ctx.runtime.runs}

        # Assert
        self.assertEqual(3, len(actual_runs))

        # nb1_run = actual_runs[get_notebook_abs_path(nb1.rel_path)]
        nb2_run = actual_runs[dtflw.databricks.get_notebook_abs_path(
            nb2.rel_path)]
        # nb3_run = actual_runs[get_notebook_abs_path(nb1.rel_path)]

        self.assertDictEqual({"foo": "bar"}, nb2_run.args)
        self.assertDictEqual(
            {"VBAK": f"{self.storage.base_path}/VBAK.parquet"}, nb2_run.inputs)
        self.assertDictEqual(
            {"SalesOrderItem": f"{self.storage.base_path}/project/02_import_sales/SalesOrderItem.parquet"}, nb2_run.outputs)
