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

    def test_info_default(self):

        logger = DefaultLogger()
        logger.verbosity = "default"
        
        out = StringIO()
        sys.stdout = out
        logger.info('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "")
            
    def test_info_verbosity_wrong_state_fail(self):

        logger = DefaultLogger()
        logger.verbosity = "wrong_state"
        
        # Assert
        with self.assertRaisesRegex(AssertionError, f"Verbosity variable can not be set to: \"{logger.verbosity}\", it must be set either \"verbose\" or \"default\"."):
            logger.info('')
        
    def test_error_verbose(self):

        logger = DefaultLogger()
        logger.verbosity = "verbose"
        
        out = StringIO()
        sys.stdout = out
        logger.error('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
            
    def test_error_verbose(self):

        logger = DefaultLogger()
        logger.verbosity = "default"
        
        out = StringIO()
        sys.stdout = out
        logger.error('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
