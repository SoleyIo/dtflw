import dtflw.databricks as db


class Arg:
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


class Input(Arg):
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


class Output(Arg):
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


def initialize_arguments(argument_class, *values):
    """
    Initializes widgets using `dbutils.widgets.text`. Parameter `argument_calss` can be `Arg`, `Input` or `Output`.
    """

    names_and_values = {}
    if len(values) == 1 and isinstance(values[0], dict):
        names_and_values = values[0]
    else:
        names_and_values = {name: "" for name in values}

    return {name: argument_class.create(name, value) for name, value in names_and_values.items()}


def init_args(*args):
    """
    Initializes widgets using `dbutils.widgets.text` for args:
    ```
    flow.notebook(...)
        .args(...)
    ```

    Parameters
    ----------
    *args
        Option 1:
            Given as a variable number of non-keyword arguments, e.g. `args = init_args("a", "b", "c")`.
            All the args will have an empty string as a value.
        Option 2:
            Given as a dictionary with default values, e.g. `args = init_args({'a': 42})`.
            All non-string values will be stringified.

    Returns
    -------
    dict[str: Arg]
    """
    return initialize_arguments(Arg, *args)


def init_inputs(*inputs):
    """
    Initializes widgets using `dbutils.widgets.text` for input tables:
    ```
    flow.notebook(...)
        .input('Orders')
    ```

    Parameters
    ----------
    *inputs
        Option 1:
            Given as a variable number of non-keyword arguments, e.g. `inputs = init_inputs("Orders", "Customers")`.
            All the inputs will have an empty string as a value.
        Option 2:
            Given as a dictionary with default values, e.g. `inputs = init_inputs({'Orders': 'orders.parquet'})`.
            All non-string values will be stringified.

    Returns
    -------
    dict[str: Input]
    """
    return initialize_arguments(Input, *inputs)


def init_outputs(*values):
    """
    Initializes widgets using `dbutils.widgets.text` for output tables:
    ```
    flow.notebook(...)
        .output('Orders')
    ```

    Parameters
    ----------
    *args
        Option 1:
            Given as a variable number of non-keyword arguments, e.g. `outputs = init_outputs("Orders", "Customers")`.
            All the args will have an empty string as a value.
        Option 2:
            Given as a dictionary with default values, e.g. `outputs = init_outputs({'Orders': 'orders.parquet'})`.
            All non-string values will be stringified.

    Returns
    -------
    dict[str: Output]
    """
    return initialize_arguments(Output, *values)
