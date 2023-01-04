import unittest
from dtflw.logger import DefaultLogger
import sys
from io import StringIO


class DefaultLoggerTestCase(unittest.TestCase):
        

    def test_default_logger_log_in_default_verbosity(self):

        default_logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out

        default_logger.log('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "")


    def test_default_logger_log_in_verbose_verbosity(self):

        default_logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out
        default_logger.verbosity="verbose"

        default_logger.log('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")

                
    def test_default_logger_log_in_true_info(self):

        default_logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out
        default_logger.info = True

        default_logger.log('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")

                      
    def test_default_logger_log_in_false_info(self):

        default_logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out
        default_logger.info = False

        default_logger.log('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "")
                      

    def test_default_logger_log_fails_wrong_verbosity(self):

        default_logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out
        default_logger.verbosity = "False"

        # Assert
        with self.assertRaisesRegex(AssertionError, f"Verbosity variable: \"{default_logger.verbosity}\" must be set either \"verbose\" or \"default\""):
            
            default_logger.log('hello world!')
            output = out.getvalue().strip()


    def test_default_logger_log_fails_wrong_info(self):

        default_logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out
        default_logger.info = "String"

        # Assert
        with self.assertRaisesRegex(AssertionError, f"Info variable: \"{default_logger.info}\" must be boolean!"):
            
            default_logger.log('hello world!')
            output = out.getvalue().strip()


    def test_default_logger_log_default_verbosity_true_info(self):

        default_logger = DefaultLogger()

        out = StringIO()
        sys.stdout = out
        default_logger.info = True

        default_logger.log('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
     
