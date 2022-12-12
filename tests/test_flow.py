from unittest.mock import patch
import unittest
from ddt import ddt, data, unpack


@ddt
class FlowTestCase(unittest.TestCase):

    @data(
        ("material", "nb_01/material.parquet"),
        ("product", None),
        (None, None)
    )
    @unpack
    @patch("dtflw.databricks.is_this_workflow")
    def test_flow_indexer(self, tbl_name, expected_tbl_path, is_this_workflow_mock):
        # Arrange
        is_this_workflow_mock.return_value = True

        flow = init_flow(self.storage)
        flow.ctx.publish_tables({tbl_name: expected_tbl_path}, "nb_01")

        # Act
        actual_tbl_path = flow[tbl_name]

        # Assert
        self.assertEqual(actual_tbl_path, expected_tbl_path)
