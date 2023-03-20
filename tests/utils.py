from collections import namedtuple
from unittest.mock import MagicMock, patch
from dtflw.storage.fs import FileStorageBase


class StorageMock(FileStorageBase):
    def __init__(self, root_dir="", spark=None, dbutils=None):

        super().__init__(spark, dbutils, root_dir)

    @property
    def base_path(self):
        return f"mock:/"


def mock_dbutils(fs_ls_raises_error=False, ls_return=[]):
    """
    Returns an object which mocks dbutils.fs.ls method.

    Parameters
    ----------
    fs_ls_raises_error: bool (False)
        If True raises a 'java.io.FileNotFoundException' exception what mimics "file does not exist" behavior.

    ls_return: lit[(<file name>: str, <file abs path>: str)]
        A list of files infos returned by dbutils.fs.ls. fs_ls_raises_error must be False.
    """
    # Custom mock, since pyspark.dbutils.DBUtils is not available.

    def dbutils_fs_ls(path):
        if fs_ls_raises_error:
            raise Exception("java.io.FileNotFoundException")

        FileInfo = namedtuple("FileInfo", ["name", "path"])

        return [FileInfo(name=r[0], path=r[1]) for r in ls_return]

    class MockObj:
        pass
    dbutils_mock = MockObj()
    dbutils_mock.fs = MockObj()
    dbutils_mock.fs.ls = dbutils_fs_ls

    return dbutils_mock


def mock_spark(spark_class_mock: MagicMock, df_dtypes=[("id", "bigint")]):
    """
    Returns a mock of SparkSession with mocked 
    spark.read.parquet which returns a mock of a pyspark.sql.dataframe.DataFrame
    with given df_dtypes.
    """
    spark_mock = spark_class_mock.return_value()

    def spark_read_parquet(path, df_dtypes=df_dtypes):
        # Mock the real behavior.

        with patch("pyspark.sql.dataframe.DataFrame") as DataFrameMock:
            df_mock = DataFrameMock.return_value()
            df_mock.dtypes = df_dtypes
            return df_mock

    spark_mock.read.parquet = spark_read_parquet
    return spark_mock


class DButilsMock:
    """
    Mocks certain public API and its behavior of DBUtils.
    """
    class WidgetsMock:
        def __init__(self):
            self._widget_values = {}

        def text(self, name, value, title):

            if value is None:
                raise Exception(
                    "Mimics the behavior in a notebook: 'dbutils.widgets.text(..., None, ...)' raises a 'java.lang.NullPointerException'.")

            self._widget_values[name] = value

        def get(self, name):
            return self._widget_values[name]

    def __init__(self) -> None:
        self.widgets = self.WidgetsMock()


class SparkSessionMock:
    """
    Mocks certain public API and its behavior of SparkSession.
    """
    class RuntimeConfigMock:
        def __init__(self):
            self._conf = {}

        def get(self, key):
            if key not in self._conf:
                raise Exception("java.util.NoSuchElementException")
            return self._conf[key]

        def set(self, key, value):
            self._conf[key] = value

    def __init__(self):
        self.conf = self.RuntimeConfigMock()
