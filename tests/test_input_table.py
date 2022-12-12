from ddt import ddt, data, unpack
from dtflw.logger import DefaultLogger
from dtflw.flow_context import FlowContext
from dtflw.input_table import InputTable
import unittest


@ddt
class InputTableTestCase(unittest.TestCase):

    def setUp(self):
        self._df = self.spark.createDataFrame(
            [("this is test", 1, 0.5, True)],
            ["c_str", "c_int", "c_dbl", "c_bl"])

        rel_file_path = self.gen_rel_file_path()
        self._df_abs_file_path = self.storage.get_abs_path(rel_file_path)

        self._df.write.parquet(self._df_abs_file_path)

        self._ctx = FlowContext(self.storage, self.spark,
                                self.dbutils, DefaultLogger())

    def tearDown(self):
        self.dbutils.fs.rm(self._df_abs_file_path, recurse=True)

    @data(
        ("material", "nb_02", False),
        ("bom", "nb_02", True),
        ("material", "nb_01", True),
        ("bom", "nb_01", True)
    )
    @unpack
    def test_abs_file_path(self, tbl_name, trg_nb, exp_needs_eval):
        """
        This test covers
            InputTable.abs_file_path
            InputTableneeds_eval()
            InputTable.validate()
        """

        # Arrange
        self._ctx.publish_tables({"material": self._df_abs_file_path}, "nb_01")

        input_table = InputTable(
            tbl_name,
            self._ctx.resolve_table(tbl_name, trg_nb),
            self._ctx
        )

        # Act/Assert

        self.assertEqual(input_table.needs_eval(), exp_needs_eval)

        self.assertEqual(input_table.abs_file_path,
                         None if exp_needs_eval else self._df_abs_file_path)

        if exp_needs_eval:
            with self.assertRaises(Exception):
                input_table.validate()
