import unittest
from dtflw.arguments import init_args, init_inputs, init_outputs


class ArgumentsTestCase(unittest.TestCase):

    def test_init_args_names(self):

        actual_args = init_args("foo", "bar")

        self.assertDictEqual(
            actual_args,
            {"foo": "", "bar": ""}
        )
