def assert_schema_compatible(actual_dtypes, expected_dtypes, is_strict: bool = False):
    """
    Checks if two given dataframe schemas are compatible.
    IMPORTANT: types 'int' and 'bigint' are considered the same.

    Parameters
    ------------
    actual_dtypes: list[(str, str)]
        An actual schema given as a list of columns with types.
    expected_dtypes: list[(str, str)]
        An expected schema given as a list of columns with types.
    is_strict: bool = False
        If False then the actual schema is allowed to have more columns then expected one.
        If True then actual schema must have the same columns as the expected one.
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
