import dtflw.databricks as db


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

    @staticmethod
    def _get_name_and_value(name, value, widget_name_suffix):
        widget_name = f"{name}{widget_name_suffix}"
        # If a widget already exists then the line below has not effect.
        dbutils = db.get_dbutils()
        dbutils.widgets.text(widget_name, str(value), widget_name)

        return name, dbutils.widgets.get(widget_name)

    @classmethod
    def create(cls, name, value):
        """
        Factory method.
        """
        return cls(*cls._get_name_and_value(name, value, cls.NAME_SUFFIX))


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


def initialize_arguments(argument_type, *values):
    """
    Initializes notebook arguments of a certain type given by `argument_type`.

    Parameters
    ----------
    argument_type: Arg | Input | Output
    *values: initial values.

    Returns
    -------
    dict[str, Arg | Input | Output]
    """

    names_and_values = {}
    if len(values) == 1 and isinstance(values[0], dict):
        names_and_values = values[0]
    else:
        names_and_values = {name: "" for name in values}

    return {name: argument_type.create(name, value) for name, value in names_and_values.items()}
