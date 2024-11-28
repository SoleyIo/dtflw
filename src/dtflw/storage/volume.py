from pyspark.sql import SparkSession

import dtflw.databricks as db
from dtflw.storage.fs import FileStorageBase

def _build_path(parts: list):
    path = ""
    if parts and parts[0]:
        path = "/" + parts[0] + _build_path(parts[1:]) 
    return path


class VolumeStorage(FileStorageBase):
    """
    Provides operations with files, volumes and schemas on Unity Catalog (the /Volumes root).
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        volume: str,
        root_dir: str,
        spark: SparkSession,
        dbutils,
    ):
        super().__init__(spark, dbutils, root_dir)
        self._path = _build_path(["Volumes", catalog, schema, volume])
        

    @property
    def base_path(self):
        return f"{self._path}/"


def init_storage(
    catalog: str,
    schema: str,
    volume: str,
    root_dir: str = None,
    spark: SparkSession = None,
    dbutils=None,
) -> VolumeStorage:
    """
    Returns a new instance of VolumeStorage.
    It is suggested using this factory function instead of the constructor of the class.

    Parameters
    ----------
    catalog : str
        A catalog is the first layer of Unity Catalog
    schema: str
        A schema is the second layer of Unity Catalog
    volume: str
        A Unity Catalog object that handles non-tabular datasets
    spark: SparkSession (None)
        A Spark session object.
        If None then the current instance is used.
    dbutils: RemoteDbUtils (None)
        A RemoteDbUtils object.
        If None then the current instance is used.
    """

    if root_dir is None:
        root_dir = ""

    if spark is None:
        spark = db.get_spark_session()

    if dbutils is None:
        dbutils = db.get_dbutils()

    return VolumeStorage(catalog, schema, volume, root_dir, spark, dbutils)
