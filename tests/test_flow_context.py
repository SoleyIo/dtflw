from soley.utils.notebook.flow20.flow_context import FlowContext
from soley.utils.notebook.flow20.logger import DefaultLogger
from soley.utils.testing.spark import StorageTestCase


class FlowContextTestCase(StorageTestCase):

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
