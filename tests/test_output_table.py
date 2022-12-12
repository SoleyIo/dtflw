from ddt import ddt, data, unpack
from dtflw.logger import DefaultLogger
from dtflw.flow_context import FlowContext
from dtflw.output_table import OutputTable
import unittest


@ddt
class OutputTableTestCase(unittest.TestCase):

    def setUp(self):
        self.delete_paths = []

    def tearDown(self):

        for p in self.delete_paths:
            self.dbutils.fs.rm(p, recurse=True)

    def test_name(self):
        # Arrange
        ctx = FlowContext(self.storage, self.spark,
                          self.dbutils, DefaultLogger())
        o = OutputTable("material", "nb_01", [], ctx)

        # Act/Assert
        self.assertEqual(o.name, "material")

    @data(
        ("material", "nb", ""),
        ("material", "analysis/nb", "root_dir/sub_dir"),
    )
    @unpack
    def test_abs_file_path(self, table_name, nb_rel_path, root_dir):

        # Arrange
        storage = init_storage(AZURE_STORAGE_ACCOUNT_NAME,
                               AZURE_STORAGE_CONTAINER_NAME, root_dir)
        ctx = FlowContext(storage, self.spark, self.dbutils, DefaultLogger())

        expected_abs_path = storage.get_abs_path(
            storage.get_path_in_root_dir(
                storage.get_path_with_file_extension(
                    f"{nb_rel_path}/{table_name}"
                )
            )
        )

        o = OutputTable(table_name, expected_abs_path, [], ctx)

        # Act/Assert
        self.assertEqual(expected_abs_path, o.abs_file_path)

    @data(True, False)
    def test_needs_eval(self, exp_needs_eval):
        # Arrange
        storage = init_storage(AZURE_STORAGE_ACCOUNT_NAME,
                               AZURE_STORAGE_CONTAINER_NAME, AZURE_STORAGE_TEST_DIR)

        table_name = "material"
        nb_rel_path = "nb_01"

        exp_abs_file_path = storage.get_abs_path(
            storage.get_path_in_root_dir(
                storage.get_path_with_file_extension(
                    f"{nb_rel_path}/{table_name}")
            )
        )

        if not exp_needs_eval:
            df = self.spark.createDataFrame(
                [(1, "hello")],
                ["id", "name"])

            self.delete_paths.append(exp_abs_file_path)

            df.write.parquet(exp_abs_file_path)

        ctx = FlowContext(storage, self.spark, self.dbutils, DefaultLogger())
        o = OutputTable(table_name, exp_abs_file_path, [], ctx)
        # Act/Assert

        self.assertEqual(o.needs_eval(), exp_needs_eval)

    @data(
        ("material", [("id", "bigint"), ("name", "string")], False, True),
        ("material", [("name", "string")], False, True),
        ("material", [], False, True),
        ("material", None, False, True),
        ("material", [("id", "bigint"), ("name", "string"),
                      ("price", "double")], False, False),
        ("material", [("id", "int"), ("price", "double")], False, False),
        ("bom", [], False, False),
        ("bom", None, False, False),
        ("material", [("id", "bigint"), ("name", None)], False, True),
        ("material", [("id", None), ("name", None)], False, True),

        ("material", [("id", "bigint")], True, False),
        ("material", [("id", "bigint"), ("name", "string")], True, True),
    )
    @unpack
    def test_validate(self, table_name, cols, strict, exp_is_valid):
        # Arrange
        storage = init_storage(AZURE_STORAGE_ACCOUNT_NAME,
                               AZURE_STORAGE_CONTAINER_NAME, AZURE_STORAGE_TEST_DIR)

        abs_output_path = storage.get_abs_path(
            storage.get_path_in_root_dir(
                storage.get_path_with_file_extension(f"nb_01/{table_name}")
            )
        )

        if exp_is_valid:
            df = self.spark.createDataFrame(
                [(1, "hello")],
                ["id", "name"])

            self.delete_paths.append(abs_output_path)

            df.write.parquet(abs_output_path)

        ctx = FlowContext(storage, self.spark, self.dbutils, DefaultLogger())
        o = OutputTable(table_name, abs_output_path, cols, ctx)

        # Act/Assert
        act_is_valid = None

        try:
            o.validate(strict)
            act_is_valid = True
        except NameError:
            act_is_valid = False
        except LookupError:
            act_is_valid = False

        self.assertEqual(exp_is_valid, act_is_valid)
