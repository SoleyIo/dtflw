import unittest
from dtflw.logger import DefaultLogger
import sys
from io import StringIO
from ddt import ddt, data, unpack


@ddt
class DefaultLoggerTestCase(unittest.TestCase):

    def test_verbosity_set_unknown_value(self):

        logger = DefaultLogger()
        logger.verbosity = "unknown value"

        self.assertEqual(logger.verbosity, "default")

    @data(
        ("verbose", "foo", "foo"),
        ("default", "foo", "")
    )
    @unpack
    def test_log(self, verbosity, info_msg, expected_msg):

        logger = DefaultLogger(verbosity)

        out = StringIO()
        sys.stdout = out
        logger.log(info_msg)

        actual_msg = out.getvalue().strip()

        # Assert
        self.assertEqual(actual_msg, expected_msg)
