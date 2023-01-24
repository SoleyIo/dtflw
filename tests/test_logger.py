import unittest
from dtflw.logger import DefaultLogger
import sys
from io import StringIO


class DefaultLoggerTestCase(unittest.TestCase):

    def log(self):

        logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out
        logger.log('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
