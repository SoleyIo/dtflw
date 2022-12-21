import dtflw.assertions as A
import unittest
from ddt import ddt, data, unpack


@ddt
class AssertSchemaCompatibleTestCase(unittest.TestCase):

    @data(
        ([("foo", "int"), ("bar", "double")], [
         ("foo", "bigint"), ("bar", "double")], True),
        ([], [], True),
        ([("foo", "int"), ("bar", "double")], [("foo", "int")], False),
        ([("foo", "int"), ("bar", "double")], [], False),
        ([], [], False)
    )
    @unpack
    def test_compatile_succeeds(self, actual, expected, strict):

        try:
            A.assert_schema_compatible(actual, expected, is_strict=strict)
        except Exception as e:
            self.fail(f"Oops: {e}")

    @data(
        ([("foo", "int"), ("bar", "double")], [
         ("foo", "bigints"), ("bar", "double"), ("baz", "string")], True),
        ([("foo", "int"), ("bar", "double")], [("foo", "bigints")], True),
        ([], [("foo", "bigints")], True),
        ([("foo", "int"), ("bar", "double")], [], True),
        ([("foo", "int")], [("foo", "bool")], False),
        ([], [("foo", "bool")], False)
    )
    @unpack
    def test_compatible_fails(self, actual, expected, strict):

        with self.assertRaises(AssertionError):
            A.assert_schema_compatible(actual, expected, strict)
