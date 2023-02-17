import unittest
from unittest.mock import patch
from ddt import ddt, data, unpack
from dtflw.storage.azure import AzureStorage
from collections import namedtuple
import tests.utils as utils


@ddt
class AzureStorageTestCase(unittest.TestCase):

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

    @data(
        ("file.txt", True),
        ("file.txt", False),
        ("wasbs://container@account.blob.core.windows.net/file.txt", True),
        ("wasbs://container@account.blob.core.windows.net/file.txt", False)

    )
    @unpack
    def test_exists(self, path_to_check, does_exist):

        dbutils_mock = utils.mock_dbutils(fs_ls_raises_error=not does_exist)

        storage = AzureStorage(
            "account", "container", "", None, dbutils_mock)

        self.assertEqual(does_exist, storage.exists(path_to_check))

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

    @patch("pyspark.sql.session.SparkSession")
    def test_read_table(self, spark_class_mock):

        spark_mock = utils.mock_spark(spark_class_mock)

        storage = AzureStorage(
            "account", "container", "root_dir", spark_mock, None)

        file_path = storage.get_abs_path(
            storage.get_path_with_file_extension("table")
        )

        actual_df = storage.read_table(file_path)
        self.assertTrue(actual_df)

    def test_base_path(self):

        storage = AzureStorage("account", "container", "root_dir", None, None)

        self.assertEqual(
            storage.base_path,
            "wasbs://container@account.blob.core.windows.net"
        )

    @data(
        ("file", "file.parquet"),
        ("root/file", "root/file.parquet"),
        ("wasbs://container@account.blob.core.windows.net/foo/bar",
         "wasbs://container@account.blob.core.windows.net/foo/bar.parquet")
    )
    @unpack
    def test_get_path_with_file_extension(self, base, expected):

        storage = AzureStorage("account", "container", "", None, None)

        self.assertEqual(
            storage.get_path_with_file_extension(base), expected
        )
