from dtflw.flow_context import FlowContext
from dtflw.logger import DefaultLogger
import unittest


class FlowContextTestCase(unittest.TestCase):

    def test_context_init(self):
        logger = DefaultLogger()

        actual_ctx = FlowContext(
            self.storage,
            self.spark,
            self.dbutils,
            logger)

        # Assert
        self.assertEqual(actual_ctx.storage, self.storage)
        self.assertEqual(actual_ctx.spark, self.spark)
        self.assertEqual(actual_ctx.dbutils, self.dbutils)
        self.assertEqual(actual_ctx.logger, logger)

    def test_logger_getter_setter(self):
        logger = DefaultLogger()

        actual_ctx = FlowContext(
            self.storage,
            self.spark,
            self.dbutils,
            logger)

        # Assert
        self.assertEqual(actual_ctx.logger, logger)

        new_logger = DefaultLogger()
        actual_ctx.logger = new_logger

        # Assert
        self.assertEqual(actual_ctx.logger, new_logger)
