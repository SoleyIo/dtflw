import databricks as db


class Arg:

    NAME_SUFFIX = ""

    def __init__(self, name, value):
        self.__name = name
        self.__value = value

    @property
    def name(self):
        return self.__name

    @property
    def value(self):
        return self.__value

    @property
    def hasValue(self):
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
        return cls(*cls._create_widget(name, value, cls.NAME_SUFFIX))


class Input(Arg):
    NAME_SUFFIX = "_in"

    @property
    def hasValue(self):
        return len(self.value) > 0


class Output(Arg):
    NAME_SUFFIX = "_out"

    @property
    def hasValue(self):
        return len(self.value) > 0


def initialize_arguments(c, *values):

    names_and_values = {}
    if len(values) == 1 and isinstance(values[0], dict):
        names_and_values = values[0]
    else:
        names_and_values = {name: "" for name in values}

    return {name: c.create(name, value) for name, value in names_and_values.items()}


def init_args(*values):
    return initialize_arguments(Arg, *values)


def init_inputs(*values):
    return initialize_arguments(Input, *values)


def init_outputs(*values):
    return initialize_arguments(Output, *values)
