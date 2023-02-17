import dtflw.databricks as db
import dtflw.com as com


class Argument:
    """
    An argument passed to a notebook.
    """

    NAME_SUFFIX = ""

    def __init__(self, name, value):
        if name is None or len(name) == 0:
            raise ValueError("'name' cannot be None nor an empty string.")
        self.__name = name

        if value is None or type(value) != str:
            raise ValueError("'value' cannot be None and must be a string.")
        self.__value = value

    @property
    def name(self) -> str:
        return self.__name

    @property
    def value(self) -> str:
        """
        Value is always of type string.
        """
        return self.__value

    @property
    def has_value(self):
        """
        Always True for Arg.
        """
        return True

    def __repr__(self):
        return self.value

    @classmethod
    def _update_name_and_value(cls, name, value):
        widget_name = cls.get_full_name(name)
        # If a widget already exists then the line below has not effect.
        dbutils = db.get_dbutils()
        dbutils.widgets.text(widget_name, str(value), widget_name)

        return name, dbutils.widgets.get(widget_name)

    @classmethod
    def create(cls, *arguments):
        """
        Returns a dict[str, Argument | Input | Output].
        """
        names_and_values = {}
        if len(arguments) == 1 and isinstance(arguments[0], dict):
            names_and_values = arguments[0]
        else:
            names_and_values = {name: "" for name in arguments}
            shared_values = cls._get_shared_values()

            names_and_values = {**names_and_values, ** shared_values}

        return {
            name: cls(*cls._update_name_and_value(name, value))
            for name, value
            in names_and_values.items()
        }

    @classmethod
    def _get_shared_values(cls):
        return com.NotebooksChannel().get_args(
            db.get_this_notebook_abs_path()
        )

    @classmethod
    def get_full_name(cls, name: str):
        return f"{name}{cls.NAME_SUFFIX}"


class Input(Argument):
    """
    An input table required by a notebook.
    """
    NAME_SUFFIX = "_in"

    @property
    def has_value(self):
        """
        Returns False if value is an empty string. Otherwise True.
        """
        return len(self.value) > 0

    @classmethod
    def _get_shared_values(cls):
        return com.NotebooksChannel().get_inputs(
            db.get_this_notebook_abs_path()
        )


class Output(Argument):
    """
    An output table promissed by a notebook.
    """
    NAME_SUFFIX = "_out"

    @property
    def has_value(self):
        """
        Returns False if value is an empty string. Otherwise True.
        """
        return len(self.value) > 0

    @classmethod
    def _get_shared_values(cls):
        return com.NotebooksChannel().get_outputs(
            db.get_this_notebook_abs_path()
        )
