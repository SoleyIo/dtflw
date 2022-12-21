import unittest
from unittest.mock import MagicMock, patch
from dtflw.databricks import get_notebook_abs_path, get_path_relative_to_project_dir, get_this_notebook_abs_cwd
from ddt import ddt, data, unpack


@ddt
class DatabricksTestCase(unittest.TestCase):

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_get_this_notebook_abs_cwd(self, get_this_notebook_abs_path_mock: MagicMock):

        get_this_notebook_abs_path_mock.return_value = "/Users/a@b.c/project_a/main"

        self.assertEqual(
            "/Users/a@b.c/project_a",
            get_this_notebook_abs_cwd())

    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_get_notebook_abs_path(self, get_this_notebook_abs_path_mock: MagicMock):
        get_this_notebook_abs_path_mock.return_value = "/Users/a@b.c/project_b/main"

        self.assertEqual(
            "/Users/a@b.c/project_b/subdir/child",
            get_notebook_abs_path("subdir/child"))

    @data(
        ("/Users/a@b.c/main", "dir/nb", "dir/nb"),
        ("/Users/a@b.c/main", "nb", "nb"),
        ("/Users/a@b.c/project_c/main", "dir/nb", "project_c/dir/nb"),
        ("/Users/a@b.c/project_c/subproject/main", "nb", "project_c/subproject/nb")
    )
    @unpack
    @patch("dtflw.databricks.get_this_notebook_abs_path")
    def test_get_path_relative_to_project_dir(self, this_nb_abs_path, rel_path, expected_project_based_nb_path, get_this_notebook_abs_path_mock: MagicMock):
        get_this_notebook_abs_path_mock.return_value = this_nb_abs_path

        self.assertEqual(
            expected_project_based_nb_path,
            get_path_relative_to_project_dir(rel_path)
        )
