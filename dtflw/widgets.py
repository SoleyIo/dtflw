import dtflw.databricks as db


class ArgumentWidget:
    """
    A widget of an argument passed to a notebook.
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
    def _create_widget(cls, name, value, widget_name_suffix):
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
        return cls(*cls._create_widget(name, value, cls.NAME_SUFFIX))


class InputTableWidget(ArgumentWidget):
    """
    A widget of an input table required by a notebook.
    """
    NAME_SUFFIX = "_in"

    @property
    def has_value(self):
        """
        Returns False if value is an empty string. Otherwise True.
        """
        return len(self.value) > 0


class OutputTableWidget(ArgumentWidget):
    """
    A widget of an output table promissed by a notebook.
    """
    NAME_SUFFIX = "_out"

    @property
    def has_value(self):
        """
        Returns False if value is an empty string. Otherwise True.
        """
        return len(self.value) > 0


def create_widgets(widget_type, *values):
    """
    Creates widgets using `dbutils.widgets.text`.
    """

    names_and_values = {}
    if len(values) == 1 and isinstance(values[0], dict):
        names_and_values = values[0]
    else:
        names_and_values = {name: "" for name in values}

    return {name: widget_type.create(name, value) for name, value in names_and_values.items()}
