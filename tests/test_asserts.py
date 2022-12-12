from dtflw.testing.spark import SparkTestCase
from ddt import ddt
import dtflw.asserts as A
import pyspark.sql.types as T


@ddt
class AssertNoNullsTestCase(SparkTestCase):

    def test_assert_no_nulls_without_given_columns(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (2, 25, "Berlin"),
                (3, 40, "Kyiv")
            ], test_schema)

        self.assertIsNone(A.assert_no_nulls(test_df))

    def test_assert_no_nulls_with_special_characters(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("Werk ausliefernd", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 25, "Berlin"),
                (1, 40, "Kyiv")
            ], test_schema)

        self.assertIsNone(A.assert_no_nulls(
            test_df, check_cols="Werk ausliefernd"))

    def test_assert_no_nulls_with_given_columns_in_list(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (None, 10, "Munich"),
                (None, 25, "Berlin"),
                (3, None, "Kyiv")
            ], test_schema)

        self.assertIsNone(A.assert_no_nulls(test_df, check_cols=["address"]))

    def test_assert_no_nulls_without_given_columns_with_duplicated_id(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 25, "Berlin"),
                (1, 40, "Kyiv")
            ], test_schema)

        self.assertIsNone(A.assert_no_nulls(test_df, check_cols=[]))

    def test_assert_no_nulls_with_given_columns_in_string_with_duplicated_id(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 25, "Berlin"),
                (1, 40, "Kyiv")
            ], test_schema)

        self.assertIsNone(A.assert_no_nulls(test_df, check_cols="address"))

    def test_assert_no_nulls_fails_without_given_columns(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (None, 10, "Munich"),
                (2, None, "Berlin"),
                (3, 40, None)
            ], test_schema)

        with self.assertRaises(AssertionError):
            A.assert_no_nulls(test_df)

    def test_assert_no_nulls_fails_with_given_columns_in_string(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (None, None, "Munich"),
                (2, 20, "Berlin"),
                (3, 40, "Istanbul")
            ], test_schema)

        with self.assertRaises(AssertionError):
            A.assert_no_nulls(test_df, "id")

    def test_assert_no_nulls_fails_exception_unexisting_column_name(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (2, 20, "Berlin"),
                (3, 40, "Istanbul")
            ], test_schema)

        with self.assertRaises(Exception):
            A.assert_no_nulls(test_df, "idd")

    def test_assert_no_nulls_fails_with_given_columns_in_list(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])

        test_df = self.spark.createDataFrame(
            [
                (1, 10, None),
                (2, 20, "Berlin"),
                (3, 40, "Istanbul")
            ], test_schema)

        with self.assertRaises(AssertionError):
            A.assert_no_nulls(test_df, ["address"])


@ddt
class AssertNoDuplicatesTestCase(SparkTestCase):

    def test_assert_no_duplicates_without_given_columns(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])
        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (2, 25, "Berlin"),
                (3, 40, "Kyiv"),
            ], test_schema)
        self.assertIsNone(A.assert_no_duplicates(test_df))

    def test_assert_no_duplicates_with_special_characters_without_given_columns(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("Werk, ausliefernd", T.StringType()),
        ])
        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (2, 25, "Berlin"),
                (3, 40, "Kyiv"),
            ], test_schema)
        self.assertIsNone(A.assert_no_duplicates(test_df))

    def test_assert_no_duplicates_with_given_columns_in_list(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])
        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 25, "Berlin"),
                (3, 40, "Kyiv"),
            ], test_schema)
        self.assertIsNone(A.assert_no_duplicates(
            test_df, check_cols=["test_amount"]))

    def test_assert_no_duplicates_with_given_empty_column_list(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])
        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 25, "Berlin"),
                (3, 40, "Kyiv"),
            ], test_schema)
        self.assertIsNone(A.assert_no_duplicates(test_df, check_cols=[]))

    def test_assert_no_duplicates_with_given_columns_in_string(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])
        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 25, "Berlin"),
                (3, 40, "Kyiv"),
            ], test_schema)
        self.assertIsNone(A.assert_no_duplicates(
            test_df, check_cols="address"))

    def test_assert_no_duplicates_fails_with_given_columns_in_list(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])
        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 25, "Berlin"),
                (1, 40, "Kyiv"),
            ], test_schema)

        with self.assertRaises(AssertionError):
            self.assertIsNone(A.assert_no_duplicates(test_df, ["id"]))

    def test_assert_no_duplicates_fails_without_given_columns(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])
        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 10, "Munich"),
                (1, 40, "Kyiv"),
            ], test_schema)

        with self.assertRaises(AssertionError):
            self.assertIsNone(A.assert_no_duplicates(test_df))

    def test_assert_no_duplicates_fails_exception_unexisting_column_name(self):

        test_schema = T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("test_amount", T.IntegerType()),
            T.StructField("address", T.StringType()),
        ])
        test_df = self.spark.createDataFrame(
            [
                (1, 10, "Munich"),
                (1, 25, "Berlin"),
                (1, 40, "Kyiv"),
            ], test_schema)

        with self.assertRaises(Exception):
            self.assertIsNone(A.assert_no_duplicates(test_df, ["idd"]))


@ddt
class AssertSameSchemaTestCase(SparkTestCase):

    def test_assert_same_schemas(self):
        actual_schema = T.StructType([
            T.StructField("from_id", T.StringType(), True),
            T.StructField("revenue", T.DoubleType(), True),
            T.StructField("sales_quantity", T.LongType(), True),
            T.StructField("cost", T.ArrayType(T.LongType()), True)
        ])

        expected_schema = T.StructType([
            T.StructField("from_id", T.StringType(), True),
            T.StructField("revenue", T.DoubleType(), True),
            T.StructField("sales_quantity", T.LongType(), True),
            T.StructField("cost", T.ArrayType(T.LongType()), True)
        ])

        expected_response = None
        actual_response = A.assert_same_schemas(
            actual_schema, expected_schema)
        self.assertEqual(actual_response, expected_response)

    def test_assert_same_schemas_fails_1(self):
        actual_schema = T.StructType([
            T.StructField("material_id", T.StringType(), True)
        ])

        expected_schema = T.StructType([
            T.StructField("to_id", T.StringType(), True)
        ])

        with self.assertRaises(Exception):
            self.assertIsNone(A.assert_same_schemas(
                actual_schema, expected_schema))

    def test_assert_same_schemas_fails_2(self):
        actual_schema = T.StructType([
            T.StructField("to_id", T.StringType(), True)
        ])

        expected_schema = T.StructType([
            T.StructField("material_id", T.StringType(), True)
        ])

        with self.assertRaises(Exception):
            self.assertIsNone(A.assert_same_schemas(
                actual_schema, expected_schema))

    def test_assert_same_schemas_fails_3(self):
        actual_schema = T.StructType([
            T.StructField("cost", T.LongType(), True)
        ])

        expected_schema = T.StructType([
            T.StructField("cost", T.DoubleType(), True)
        ])

        with self.assertRaises(Exception):
            self.assertIsNone(A.assert_same_schemas(
                actual_schema, expected_schema))
