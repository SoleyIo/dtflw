import unittest
from dtflw.logger import DefaultLogger
import sys
from io import StringIO


class DefaultLoggerTestCase(unittest.TestCase):

    def test_default_logger_log(self):

        default_logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out
        default_logger.log('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
