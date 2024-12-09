import unittest
from unittest.mock import patch
from ddt import ddt, data, unpack
from dtflw.storage.volume import VolumeStorage
from collections import namedtuple
import tests.utils as utils


@ddt
class VolumeStorageTestCase(unittest.TestCase):

    @data(
        ("", "", "", "", "", ""),
        ("catalog", "schema", "volume", "", "", ""),
        ("", "", "", "", "relative", "relative"),
        ("catalog", "schema", "volume", "nb", "file.txt", "nb/file.txt"),
    )
    @unpack
    def test_get_path_in_root_dir(self, catalog, schema, volume, root_dir, rel_path, expected_abs_path):

        import os
        storage = VolumeStorage(catalog, schema, volume, root_dir, None, None)

        actual_abs_path = storage.get_path_in_root_dir(rel_path)
        self.assertEqual(expected_abs_path, actual_abs_path)

    @data(
        ("", "", "", "", ""),
        ("catalog", "", "", "root_dir", "root_dir"),
        ("catalog", "schema", "", "root_dir/sub_dir", "root_dir/sub_dir"),
    )
    @unpack
    def test_root_dir(self, catalog, schema, volume, root_dir, expected_path):

        storage = VolumeStorage(catalog, schema, volume, root_dir, None, None)
        self.assertEqual(expected_path, storage.root_dir)

    @data(
        ("foo.parquet", "", "", "", ""),
        ("root_dir/foo.parquet", "catalog", "schema", "volume", "root_dir"),
    )
    @unpack
    def test_get_abs_path(self, rel_path, catalog, schema, volume, root_dir):
        # Arrange

        storage = VolumeStorage(catalog, schema, volume, root_dir, None, None)
        expected_path = f"{storage.base_path}{rel_path}"

        # Act
        actual_path = storage.get_abs_path(rel_path)

        # Assert
        self.assertEqual(actual_path, expected_path)

    def test_base_path(self):

        storage = VolumeStorage("", "", "", None, None, None)

        self.assertEqual(
            storage.base_path,
            "/Volumes/"
        )
