import typing
from pyspark.sql.session import SparkSession
from dtflw.io.storage import FileStorageBase
from dtflw.events import EventDispatcher
from dtflw.logger import LoggerBase
from dtflw.runtime import Runtime
from dtflw.tables_repo import TablesRepository


class FlowContext():
    """
    Provides a context to all Flow subsystems to function. 
    Also, keeps and resolves evaluated at runtime tables.
    """

    def __init__(self, storage: FileStorageBase, spark: SparkSession, dbutils, logger: LoggerBase):
        self.storage = storage
        self.spark = spark
        self.dbutils = dbutils
        self.tables_repo = TablesRepository()
        self.logger = logger
        self.events = EventDispatcher()
        self.runtime = Runtime()

    @property
    def logger(self) -> LoggerBase:
        """
        Returns logger object.
        """
        return self.__logger

    @logger.setter
    def logger(self, value):
        """
        Sets logger object.
        """
        if isinstance(value, LoggerBase):
            self.__logger = value
        else:
            raise ValueError("Unexpected logger type.")

    def publish_tables(self, tables: typing.Dict[str, str], src_nb_path: str) -> None:
        """
        Registers tables as being available to be resolved for a notebook as input.

        Parameters
        ----------
        tables: Dict[str, str] 
            A dictionary where key is a table's name and value is an absolute file path.

        src_nb_path: str
            Name (path) of a notebook which produced the tables.
        """
        self.tables_repo.publish(tables, src_nb_path)

    def resolve_table(self, name: str, trg_nb_path: str = None) -> str:
        """
        Searches for a table to be returned as an input for a notebook.
        Returns None if no suitable table was found.

        Parameters
        ----------
        name: str
            Name of a table to search for.
        trg_nb_path: str
            Name (path) of a notebook which will consume the table as an input.

        Returns
        -------
        str
            Absolute path to the table if it has been found, and None otherwise.
        """
        return self.tables_repo.resolve(name, trg_nb_path)

    def show(self):
        """
        Prints all published tables.
        """
        self.logger.log("Evaluated tables:")
        for (t_name, t_pubs) in self.tables_repo.tables.items():
            self.logger.log(f"'{t_name}'")
            for t_nb_path, t_abs_path in t_pubs.items():
                self.logger.log(f"  '{t_nb_path}' : '{t_abs_path}'")
