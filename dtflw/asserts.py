from pyspark.sql.types import StructType


def assert_no_nulls(df, check_cols=[]):
    """
    Function to check for null values in the DataFrame by given columns.
    If null are found exception is thrown.

    Paramaters
    ------------
    df: DataFrame
        Input dataframe

    check_cols: list<string>
        List of columns to check for null values
    """

    if isinstance(check_cols, str):
        check_cols = [check_cols]
    elif isinstance(check_cols, list):
        check_cols = check_cols if check_cols else df.columns
    else:
        raise TypeError(
            '`check_cols` parameter must be of type string or list<string>')

    for c in check_cols:
        assert df.where(f"`{c}` is null").count(
        ) == 0, f"Column `{c}` contains null values"


def assert_no_duplicates(df, check_cols=[]):
    """
    Function to check for duplicates in the DataFrame by given columns.
    If duplicates are found exception is thrown.

    Paramaters
    ------------
    df: DataFrame
        Input dataframe

    check_cols: list<string>
        List of columns to check for duplicates
    """

    if isinstance(check_cols, str):
        check_cols = [check_cols]
    elif isinstance(check_cols, list):
        check_cols = check_cols if check_cols else df.columns
    else:
        raise TypeError(
            '`check_cols` parameter must be of type string or list<string>')

    duplicates = df.groupBy(*check_cols).count().where('count>1')
    duplicates_count = duplicates.count()

    assert duplicates_count == 0, f'{duplicates_count} duplicated keys were found. First 10 duplicates are: ' + str(
        duplicates.head(10))


def assert_same_schemas(first_schema: StructType, second_schema: StructType):
    """
    Compares two given dataframe schemas if they are the same.
    Two schemas are same if names and types of all fields are the same.
    Raises an AssertionError if schemas are not compatible.

    Parameters
    ------------
    first_schema : StructType
      First dataframe schema.
    second_schema : StructType
      Second dataframe schema.
    """

    assert_schema_compatible(
        [(field.name, field.dataType) for field in first_schema.fields],
        [(field.name, field.dataType) for field in second_schema.fields],
        is_strict=True
    )


def assert_schema_compatible(actual_dtypes, expected_dtypes, is_strict: bool = False):
    """
    Compares two given dataframe schemas if they are the same.
    Two schemas are same if names and types of all fields are the same.
    Raises an AssertionError if schemas are not compatible.

    Parameters
    ------------
    actual_dtypes: list[(str, str)]
        An actual schema given as a list of columns with types.
    expected_dtypes: list[(str, str)]
        An expected schema given as a list of columns with types.
    is_strict: bool = False
        If set to False then the actual schema may have more columns then expected one.
        If set to True and the actual and expected schemas have different number of columns then an error is raised.

    """

    if actual_dtypes is None:
        raise ValueError("actual_dtypes must not be None.")

    if expected_dtypes is None:
        raise ValueError("expected_dtypes must not be None.")

    df_dtypes = actual_dtypes
    df_columns = map(lambda dtype: dtype[0], actual_dtypes)
    expected_columns = expected_dtypes

    errors = []

    for exp_col_name, exp_col_type in expected_columns:
        act_col = next(
            (t for t in df_dtypes if t[0] == exp_col_name), None)
        if not act_col:
            errors.append(
                f"Column '{exp_col_name}' is not present in the table.")
            continue
        act_col_type = act_col[1]

        # Pass validation for column of type `int` in case of `bigint` type is expected.
        if act_col_type == 'int' and exp_col_type == 'bigint':
            act_col_type = 'bigint'

        if exp_col_type != act_col_type and exp_col_type is not None:
            errors.append(
                f"Column's '{exp_col_name}' expected type is '{exp_col_type}' but '{act_col_type}' is given.")

    if is_strict:
        expected_column_names = set(col_name for (
            col_name, col_type) in expected_columns)
        additional_columns = [
            c for c in df_columns if c not in expected_column_names]
        if additional_columns:
            errors.append(
                f"Additional columns were present in the table. Columns: {additional_columns}")

    if errors:
        raise AssertionError('\n'.join(errors))
