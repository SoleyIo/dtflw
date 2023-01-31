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
            
    def test_info_verbosity_unkown_state(self):

        logger = DefaultLogger()
        logger.verbosity = "unkown_state"
        
        out = StringIO()
        sys.stdout = out
        logger.info('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "")
        
    def test_error_verbose(self):

        logger = DefaultLogger()
        logger.verbosity = "verbose"
        
        out = StringIO()
        sys.stdout = out
        logger.error('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
        
    def test_error_default(self):

        logger = DefaultLogger()
        logger.verbosity = "default"
        
        out = StringIO()
        sys.stdout = out
        logger.error('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
            
    def test_error_verbosity_unkown_state(self):

        logger = DefaultLogger()
        logger.verbosity = "unkown_state"
        
        out = StringIO()
        sys.stdout = out
        logger.error('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")

    def test_msg_no_verbosity_set(self):

        logger = DefaultLogger()
        
        out = StringIO()
        sys.stdout = out
        logger.info('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "")

    def test_error_no_verbosity_set(self):

        logger = DefaultLogger()
        
        out = StringIO()
        sys.stdout = out
        logger.error('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
