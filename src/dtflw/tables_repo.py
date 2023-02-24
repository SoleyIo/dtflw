from collections import OrderedDict
import typing


class TablesRepository:
    """
    Repository to keep relations between notebooks and tables they produce.
    """

    def __init__(self):
        self.__tables = {}

    @property
    def tables(self):
        """
        Published tables.

        Returns
        -------
        A dictionary of structure below
        { 
            <table name> : { 
                <notebook path> : <table abs path on a storage>
                } 
        }
        """
        return self.__tables

    def publish(self, tables: typing.Dict[str, str], src_nb_path: str) -> None:
        """
        Registers tables as being available to be resolved for a notebook as input.

        Parameters
        ----------
        tables: Dict[str, str] 
            A dictionary where key is a table's name and value is an absolute file path.

        src_nb_path: str
            Path of a notebook which has produced tables (a source notebook).
        """
        for t_name, t_abs_path in tables.items():
            if t_name not in self.__tables:
                self.__tables[t_name] = OrderedDict()

            self.__tables[t_name][src_nb_path] = t_abs_path

    def resolve(self, name: str, trg_nb_path: str = None) -> str:
        """
        Searches for a table to be returned as an input for a notebook.
        Returns None if no suitable table was found.

        Table Search Algorithm
        1. IF the target notebook has not published tables yet
            THEN return the latest table path available.
        2. IF the target notebook has already published tables
            THEN return the first table path evalueted right before it.
        3. IF the target notebook is None
            THEN return the latest table published by a given name 
            (if exsts otherwise return None).

        Parameters
        ----------
        name: str
            Name of a table to search for.
        trg_nb_path: str
            Path of a notebook for which a table will be resolved as an input (a target notebook).

        Returns
        -------
        str
            Absolute path to the table if it has been found, and None otherwise.
        """

        if name not in self.__tables:
            return None

        if not trg_nb_path or trg_nb_path not in self.__tables[name]:
            return next(reversed(self.__tables[name].values()))

        # Iterate backwards to pick the latest table published.
        is_found = False
        for t_nb_path, t_abs_path in reversed(self.__tables[name].items()):
            if is_found:
                return t_abs_path
            is_found = trg_nb_path == t_nb_path

        return None
