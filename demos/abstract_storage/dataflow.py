from dtflw import init_flow
from dtflw.storage.fs import FileStorageBase
import dtflw.databricks as db
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

class DbfsStorage(FileStorageBase):
    """
    Provides operations with files and directories on DBFS (the DBFS root).
    """
    def __init__(self, root_dir: str, spark: SparkSession, dbutils):

        super().__init__(spark, dbutils, root_dir)

    @property
    def base_path(self):
        return "dbfs:/"

    def write_table(self, df: DataFrame, path: str) -> None:
        df.write.mode("overwrite").parquet(path)
      
def get_storage():
    root_dir = f"FileStore/{db.get_current_username()}"
    spark = db.get_spark_session()
    dbutils = db.get_dbutils()

    return DbfsStorage(root_dir, spark, dbutils)

def get_flow():
    return init_flow(get_storage())