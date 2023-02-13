from dtflw.storage.fs import FileStorageBase
from pyspark.sql.session import SparkSession
import dtflw.databricks as db


class AzureStorage(FileStorageBase):
    """
    Provides operations with files and directories on an Azure Storage container.
    """

    def __init__(self, account_name: str, container_name: str, root_dir: str, spark: SparkSession, dbutils):
        """
        Initializes an instance.

        Parameters
        ----------
        account_name : str
            Azure Storage account name.
        container_name : str
            Azure Storage container name.
        root_dir : str
            Root dir in a container.
        spark: SparkSession
            A Spark session object.
        dbutils: DBUtils
            A DBUtils object.
        """
        super().__init__(spark, dbutils, root_dir)

        self.__account_name = account_name
        self.__container_name = container_name

    @property
    def account_name(self) -> str:
        return self.__account_name

    @property
    def container_name(self) -> str:
        return self.__container_name

    @property
    def base_path(self):
        """
        Returns the base path.
        """
        return f"wasbs://{self.__container_name}@{self.__account_name}.blob.core.windows.net"


def init_storage(account_name: str, container_name: str, root_dir: str = None, spark: SparkSession = None, dbutils=None):
    """
    Returns a new instance of AzureStorage. 
    It is suggested using this factory function instead of the constructor of AzureStorage class.

    Parameters
    ----------
    account_name : str
        Azure Storage account name.
    container_name : str
        Azure Storage container name.
    root_dir : str (None)
        Root dir in a container.
        If None then a username of the current user will be used.
    spark: SparkSession (None)
        A Spark session object.
        If None then the current instance is used.
    dbutils: DBUtils (None)
        A DBUtils object.
        If None then the current instance is used.
    """
    if account_name is None or len(account_name) == 0:
        raise ValueError("account_name cannot be None nor empty string.")

    if container_name is None or len(container_name) == 0:
        raise ValueError("container_name cannot be None nor empty string.")

    if root_dir is None:
        root_dir = db.get_current_username()

    if spark is None:
        spark = db.get_spark_session()

    if dbutils is None:
        dbutils = db.get_dbutils()

    return AzureStorage(account_name, container_name, root_dir, spark, dbutils)
