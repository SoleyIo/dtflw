from __future__ import annotations
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from dtflw.databricks import get_dbutils
import fnmatch
from abc import ABC, abstractmethod


def file_exists(abs_path: str, dbutils=None) -> bool:
    """
    Returns True if a file by the given absolute path exists, and False otherwise.
    Works for Azure Storage container and DBFS.

    Parameters
    ----------
    abs_path : str
        Absolute file path.

    dbutils : DBUtils

    Returns
    -------
    True if a file exists and False otherwise.
    """
    if abs_path is None or len(abs_path) == 0:
        raise ValueError("'abs_path' cannot be None neither an empty string.")

    if dbutils is None:
        dbutils = get_dbutils()

    try:
        dbutils.fs.ls(abs_path)
        return True
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            return False
        else:
            raise


class FileStorageBase(ABC):
    """
    Inherit from this class to implement a specific file storage.
    The base class provides generic functionality for working with *.parquet files.
    """

    def __init__(self, spark: SparkSession, dbutils, root_dir: str = ""):
        """
        Initializes an instance.

        Parameters
        ----------
        spark: SparkSession
            A Spark session object.
        dbutils: DBUtils
            A DBUtils object.
        root_dir : str
            Root dir in the container. Default is an empty string.
        """
        self.__spark = spark
        self.__dbutils = dbutils
        self.__root_dir = root_dir

    @property
    def root_dir(self):
        """
        Root directory in the storage.
        """
        return self.__root_dir

    @property
    @abstractmethod
    def base_path(self):
        """
        Returns the base path.
        """
        pass

    def list(self, path_pattern: str, lister=None):
        """
        Returns a possibly empty list of paths that match a given pattern.
        Uses [fnmatch](https://docs.python.org/3/library/fnmatch.html).

        IMPORTANT: This function supports only a subset of 
        [pattern syntax used by a Spark reader](https://kb.databricks.com/scala/pattern-match-files-in-path.html).
        It supports:
            - [Asterisk](https://kb.databricks.com/scala/pattern-match-files-in-path.html#asterisk)
            - [Question mark](https://kb.databricks.com/scala/pattern-match-files-in-path.html#question-mark)
            - [Character class](https://kb.databricks.com/scala/pattern-match-files-in-path.html#character-class)

        Parameters
        ----------
        path_pattern: str
            A fmatch pattern.
        lister: lambda str -> list[(name:str, path:str)]
            Returns a list of (name:str, path:str) for a given path.
            Keep it None. It is used in unit test.
        """
        if path_pattern is None:
            raise ValueError("'path_pattern' cannot be None.")

        if not path_pattern.startswith(self.base_path):
            raise ValueError("'path_pattern' has a different base.")

        if lister is None:
            def lister(path): return self.__dbutils.fs.ls(path)

        def collect_matches(current_path, parts, matches):
            if len(parts) == 0:
                return

            pattern_part = parts[0]

            for info in [info for info in lister(current_path)]:
                current_name = info.name
                if current_name.endswith("/"):
                    current_name = current_name[:-1]

                if fnmatch.fnmatch(current_name, pattern_part):

                    if len(parts) == 1:
                        # Match found
                        matches.append(info.path)
                    else:
                        collect_matches(info.path, parts[1:], matches)

        parts = path_pattern[len(self.base_path) + 1:].split("/")

        matches = []
        collect_matches(self.base_path, parts, matches)
        return matches

    def is_abs_path(self, path: str) -> bool:
        """
        Returns True if a given path starts with base path.
        """
        return path is not None and path.startswith(self.base_path)

    def get_abs_path(self, rel_path: str) -> str:
        """
        Returns an absolute path to a file or a directory given by a relative path.
        Absolute path gets built as {base_path}/{rel_path}.

        Parameters
        ----------
        rel_path : str
            Relative file path.
        """
        return f"{self.base_path}/{rel_path}"

    def exists(self, path: str) -> bool:
        """
        Returns True if a given path exists in a storage and False otherwise.
        This method does not support path patterns. See `list` method for that.
        """
        if self.is_abs_path(path):
            return file_exists(path, self.__dbutils)

        return file_exists(self.get_abs_path(path), self.__dbutils)

    def get_path_with_file_extension(self, file_path):
        """
        Returns a given file path with a file extension.

        Base implementation uses `*.parquet` files by default.
        Override this method in a subclass to change the file extension.
        """
        return f"{file_path}.parquet"

    def get_path_in_root_dir(self, rel_path):
        """
        Returns a path relative to the base path with the root directory preceding.
        """
        return f"{self.__root_dir}{'/' if self.__root_dir != '' else ''}{rel_path}"

    def read_table(self, path) -> DataFrame:
        """
        Reads a table and returns a DataFrame.

        Base implementation works with `*.parquet` files by default.
        Override this method in a subclass to change to a different file format.
        """
        return self.__spark.read.parquet(path)
