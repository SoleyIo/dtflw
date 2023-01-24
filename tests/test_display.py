import unittest
from dtflw.display import DefaultDisplay
import sys
from io import StringIO


class DefaultDisplayTestCase(unittest.TestCase):

    def test_show(self):

        display = DefaultDisplay()

        out = StringIO()
        sys.stdout = out
        display.show('hello world!')
        output = out.getvalue().strip()

        # Assert
        self.assertEqual(output, "hello world!")
