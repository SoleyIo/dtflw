import unittest
from ddt import ddt, data
from dtflw.tables_repo import TablesRepository


@ddt
class TablesRepositoryTestCase(unittest.TestCase):

    @data("material", "unknown")
    def test_resolve_table_1(self, tbl_name_to_resolve):
        """
        Expected: `None` because either
        - there is no table published by a source notebook, but not the target one;
        - or a table name is unknown.
        """

        # Arrane
        source_nb_path = "nb_01"
        repo = TablesRepository()
        repo.publish(
            {"material": f"{source_nb_path}/material.parquet"}, source_nb_path)

        # Act
        # Mimic a cell evaluated multiple times
        for i in range(0, 10):
            actual_table = repo.resolve(tbl_name_to_resolve, source_nb_path)

            # Assert
            self.assertIsNone(actual_table)

    def test_resolve_table_2(self):
        """
        Expected: the last table published right before the table 
        published by the target notebook.
        """

        # Arrane
        repo = TablesRepository()

        expected_table = "nb_01/material.parquet"
        repo.publish({"material": expected_table}, "nb_01")
        repo.publish({"material": "nb_02/material.parquet"}, "nb_02")

        # Act
        # Mimic a cell evaluated multiple times
        for i in range(0, 10):
            actual_table = repo.resolve("material", "nb_02")

            # Assert
            self.assertEqual(actual_table, expected_table)

    def test_resolve_table_3(self):
        """
        Expected: the last table published if a target notebook
        has not published anything yet (had no run).
        """

        # Arrane
        repo = TablesRepository()

        expected_table = "nb_02/material.parquet"
        repo.publish({"material": "nb_01/material.parquet"}, "nb_01")
        repo.publish({"material": expected_table}, "nb_02")

        # Act
        # Mimic a cell evaluated multiple times
        for i in range(0, 10):
            actual_table = repo.resolve("material", "nb_03")

            # Assert
            self.assertEqual(actual_table, expected_table)

    @data("", None, "root_dir")
    def test_resolve_table_4(self, storage_root_dir):
        """
        Resolve inputs only to tables published before and not after.
        """

        # Arrange
        repo = TablesRepository()
        if storage_root_dir:
            storage_root_dir = storage_root_dir + '/'

        # Mimic a run of 'nb_01': publish an output 'material'
        repo.publish(
            {"material": f"{storage_root_dir}nb_01/material.parquet"}, "nb_01")

        # Mimic a run of 'nb_02': resolve an input and publish an output
        actual_table = repo.resolve("material", "nb_02")
        self.assertEqual(
            actual_table, f"{storage_root_dir}nb_01/material.parquet")

        repo.publish(
            {"material": f"{storage_root_dir}nb_02/material.parquet"}, "nb_02")

        # Mimic a run of 'nb_03': resolve an input and publish an output
        actual_table = repo.resolve("material", "nb_03")
        self.assertEqual(
            actual_table, f"{storage_root_dir}nb_02/material.parquet")

        repo.publish(
            {"material": f"{storage_root_dir}nb_03/material.parquet"}, "nb_03")

        # Act
        # Mimic a run of 'nb_02' again
        # Expected input resolved to "nb_01/material.parquet"
        for i in range(0, 10):
            actual_table = repo.resolve("material", "nb_02")
            # Assert
            self.assertEqual(
                actual_table, f"{storage_root_dir}nb_01/material.parquet")

    def test_resolve_table_if_trg_nb_is_none(self):
        """
        If target notebook is None then expected is the latest table
        published under a given name.
        """

        # Arrane
        nb_path = "nb_01"
        expected_table = f"{nb_path}/material.parquet"

        repo = TablesRepository()
        repo.publish({"material": expected_table}, nb_path)

        # Act
        actual_table = repo.resolve("material", trg_nb_path=None)

        # Assert
        self.assertEqual(actual_table, expected_table)
