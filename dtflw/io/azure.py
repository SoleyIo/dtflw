from dtflw.io.storage import FileStorageBase
from pyspark.sql.session import SparkSession


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
            Azure Storage account name
        container_name : str
            Azure Storage container name
        root_dir : str
            Root dir in a container
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
