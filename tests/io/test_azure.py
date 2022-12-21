from unittest.mock import patch
from ddt import ddt, data, unpack
from datetime import datetime
from dtflw.testing.spark import SparkTestCase
from dtflw.io.azure import AzureStorage
from collections import namedtuple
import os
import unittest

# This test case requires an Azure blob container.
# Set next env variables to run it:

# AZURE_STORAGE_ACCOUNT_NAME
# AZURE_STORAGE_CONTAINER_NAME
# AZURE_STORAGE_TEST_DIR
# AZURE_STORAGE_KEY


@ddt
class AzureStorageTestCase(unittest.TestCase):  # SparkTestCase):

    # def setUp(self):
    #     super().setUp()

    #     self.account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", default=None)
    #     if self.account is None:
    #         raise ValueError(
    #             "Could not find AZURE_STORAGE_ACCOUNT_NAME env var.")

    #     self.container = os.getenv(
    #         "AZURE_STORAGE_CONTAINER_NAME", default=None)
    #     if self.container is None:
    #         raise ValueError(
    #             "Could not find AZURE_STORAGE_CONTAINER_NAME env var.")

    #     self.test_dir = os.getenv("AZURE_STORAGE_TEST_DIR", default=None)
    #     if self.test_dir is None:
    #         raise ValueError("Could not find AZURE_STORAGE_TEST_DIR env var.")

    #     key = os.getenv("AZURE_STORAGE_KEY", default=None)
    #     if key is None:
    #         raise ValueError("Could not find AZURE_STORAGE_KEY env var.")

    #     self.spark.conf.set(
    #         f"fs.azure.account.key.{self.account}.blob.core.windows.net", key)

    # def tearDown(self):
    #     self.dbutils.fs.rm(
    #         f"wasbs://{self.container}@{self.account}.blob.core.windows.net/{self.test_dir}",
    #         recurse=True
    #     )

    #     super().tearDown()

    # @patch("pyspark.dbutils.DBUtils")
    # @patch("pyspark.sql.session.SparkSession")
    @data(
        ("", "", ""),
        ("root", "", "root/"),
        ("", "relative", "relative"),
        ("root", "file.txt", "root/file.txt"),
        ("root/dir", "foo/bar/", "root/dir/foo/bar/")
    )
    @unpack
    def test_get_path_in_root_dir(self, root_dir, rel_path, expected_abs_path):

        storage = AzureStorage("account", "container", root_dir, None, None)

        actual_abs_path = storage.get_path_in_root_dir(rel_path)
        self.assertEqual(expected_abs_path, actual_abs_path)

    def test_get_path_with_file_extension(self):

        storage = AzureStorage("account", "container", "", None, None)

        self.assertEqual(
            storage.get_path_with_file_extension("file"), "file.parquet"
        )

        self.assertEqual(
            storage.get_path_with_file_extension("root/file"),
            "root/file.parquet"
        )

    @data(
        "",
        "root",
        "root/dir"
    )
    def test_root_dir(self, root_dir):

        storage = AzureStorage("acc", "con", root_dir, None, None)
        self.assertEqual(root_dir, storage.root_dir)

    @data(
        # * - matches everything
        ("*", ["dir_1/", "dir_2/", "file.csv"]),
        ("dir_*", ["dir_1/", "dir_2/"]),
        # Path pattern must be normalized
        ("dir_*/", []),
        ("fi*", ["file.csv"]),
        # ? - matches any single character
        ("dir_?/file_*.csv", ["dir_1/file_1.csv", "dir_2/file_3.csv"]),
        ("dir_1/dir_3/file_4.txt", ["dir_1/dir_3/file_4.txt"]),
        # [seq] - matches any character in seq
        ("dir_[24513]/file_[31].csv", ["dir_1/file_1.csv", "dir_2/file_3.csv"])
    )
    @unpack
    def test_list(self, path_pattern, expected_matches):
        """
        Spark reader uses a pattern syntax different from [fmatch](https://docs.python.org/3/library/fnmatch.html).
        Which is also used by [glob](https://docs.python.org/3/library/glob.html).

        `AzureStorage.list` supports only a subset of it.
        It supports:
        - Asterisk: *
        - Question mark: ?
        - Character class: [ab]

        It does not support:
        - Negated character class: [^ab] (in fmatch: [!ab])
        - Character range: [a-b]
        - Negated character range: [^a-b]
        - Alternation: {a,b}

        [Spark reader pattern syntax](https://kb.databricks.com/scala/pattern-match-files-in-path.html)
        """

        # Arrange
        storage = AzureStorage("account", "container", "", None, None)

        fs = {
            storage.base_path: [
                "dir_1/",
                "dir_2/",
                "file.csv"
            ],
            storage.get_abs_path("dir_1/"): [
                "file_1.csv",
                "file_2.txt",
                "dir_3/"
            ],
            storage.get_abs_path("dir_1/dir_3/"): [
                "file_4.txt"
            ],
            storage.get_abs_path("dir_2/"): [
                "file_3.csv"
            ]
        }

        Info = namedtuple('Info', 'name path')

        expected_files = [storage.get_abs_path(m) for m in expected_matches]

        # Act
        actual_files = storage.list(
            storage.get_abs_path(path_pattern),

            # Pass a lister for testing
            lambda path: [Info(name, path + ("" if path.endswith("/") else "/") + name)
                          for name in fs[path]]
        )

        # Assert
        self.assertCountEqual(actual_files, expected_files)

    @data(
        ("foo.parquet", ""),
        ("sub_dir/another_one/foo.parquet", "root"),
        ("sub_dir/dir/", "root/dir")
    )
    @unpack
    def test_get_abs_path(self, rel_path, root_dir):
        # Arrange
        acc_name = "account"
        con_name = "container"

        storage = AzureStorage(acc_name, con_name, root_dir, None, None)

        expected_path = f"wasbs://{con_name}@{acc_name}.blob.core.windows.net/{rel_path}"

        # Act
        actual_path = storage.get_abs_path(rel_path)

        # Assert
        self.assertEqual(actual_path, expected_path)

    # def test_exists_if_true(self):
    #     # Arrange
    #     df = self.spark.createDataFrame([("this is test",)], ["msg"])

    #     storage = AzureStorage(self.account, self.container,
    #                            "", self.spark, self.dbutils)

    #     rel_file_path = f"{self.test_dir}/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt"
    #     df.write.text(storage.get_abs_path(rel_file_path))

    #     # Act/Assert
    #     self.assertTrue(storage.exists(rel_file_path))

    # def test_exists_if_false(self):
    #     # Arrange
    #     storage = AzureStorage(self.account, self.container,
    #                            "", self.spark, self.dbutils)
    #     rel_file_path = f"{self.test_dir}/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt"

    #     # Act/Assert
    #     self.assertFalse(storage.exists(rel_file_path))

    @data(
        ("", "", False),
        ("wasbs://con@acc.blob.core.windows.net", "root", True),
        ("wasbs://con@acc.blob.core.windows.net/dir/file.txt", "root", True),
        ("wasbs://con@acc.blob.core.windows.net/dir/file.txt", "", True),
        ("root/sub_root/", "root", False)
    )
    @unpack
    def test_is_abs_path(self, path, root_dir, is_abs):
        # Arrange
        storage = AzureStorage("acc", "con", root_dir, None, None)

        # Act/Assert
        self.assertEqual(storage.is_abs_path(path), is_abs)

    def test_get_path_with_file_extension(self):

        storage = AzureStorage("account", "account", "", None, None)

        self.assertEqual(
            storage.get_path_with_file_extension("file"),
            "file.parquet"
        )

        self.assertEqual(
            storage.get_path_with_file_extension("root/file"),
            "root/file.parquet"
        )

    # @data(
    #     ("", "material", True),
    #     ("", "bom", False),
    #     ("root_dir", "material", True),
    #     ("root_dir/sub_dir", "material", True)
    # )
    # @unpack
    # def test_read_table(self, root_dir, table_name, exists):
    #     # Arrange
    #     storage = AzureStorage(
    #         self.account, self.container, f"{self.test_dir}/{root_dir}",
    #         self.spark, self.dbutils
    #     )

    #     expected_df = self.spark.createDataFrame(
    #         [(1, "hello")], ["id", "name"])

    #     file_path = storage.get_abs_path(
    #         storage.get_path_with_file_extension(table_name)
    #     )

    #     expected_df.write.mode("overwrite").parquet(file_path)

    #     # Act
    #     act_df = storage.read_table(file_path)

    #     # Assert
    #     self.assert_dataframes_same(act_df, expected_df)

    def test_base_path(self):

        storage = AzureStorage("account", "container", "root_dir", None, None)

        self.assertEqual(
            storage.base_path,
            f"wasbs://container@account.blob.core.windows.net"
        )
