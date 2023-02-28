from dtflw.storage.fs import FileStorageBase
from pyspark.sql.session import SparkSession
import dtflw.databricks as db


class DbfsStorage(FileStorageBase):
    """
    Provides operations with files and directories on DBFS (the DBFS root).
    """
    def __init__(self, root_dir: str, spark: SparkSession, dbutils):

        super().__init__(spark, dbutils, root_dir)

    @property
    def base_path(self):
        return "dbfs:/"

def init_storage(root_dir: str = None, spark: SparkSession = None, dbutils=None) -> DbfsStorage:
    """
    Returns a new instance of DbfsStorage. 
    It is suggested using this factory function instead of the constructor of the class.

    Parameters
    ----------
    root_dir : str (None)
        Root dir in a container.
        If None then the 'dbfs:/FileStore/{username}' will be used.
    spark: SparkSession (None)
        A Spark session object.
        If None then the current instance is used.
    dbutils: DBUtils (None)
        A DBUtils object.
        If None then the current instance is used.
    """

    if root_dir is None:
        root_dir = f"FileStore/{db.get_current_username()}"

    if spark is None:
        spark = db.get_spark_session()

    if dbutils is None:
        dbutils = db.get_dbutils()

    return DbfsStorage(root_dir, spark, dbutils)
