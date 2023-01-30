import unittest
from dtflw.logger import DefaultLogger
import sys
from io import StringIO


class DefaultLoggerTestCase(unittest.TestCase):

    def test_info_verbose(self):

        logger = DefaultLogger()
        logger.verbosity = "verbose"
        
        out = StringIO()
        sys.stdout = out
        logger.info('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
