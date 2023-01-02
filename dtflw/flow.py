import types
from typing import Union
from dtflw.flow_context import FlowContext
from dtflw.lazy_notebook import LazyNotebook
from dtflw.plugin import FlowPluginBase, NotebookPluginBase


class Flow:
    """
    Represents the main object for building a data pipeline.
    """

    def __init__(self, ctx: FlowContext):
        if ctx is None:
            raise ValueError("Context cannot be None.")

        self.__ctx = ctx
        self.__nb_plugins = {}

    def __setitem__(self, _):
        raise NotImplementedError("Not supported.")

    def __getitem__(self, table_name: str):
        """
        Returns the latest evaluation of a table's file path.
        If not found then returns None.

        Parameters
        ----------
        table_name: str
            Name of a table.
        """
        return self.ctx.tables_repo.resolve(table_name)

    @property
    def ctx(self) -> FlowContext:
        """
        Returns the current context.
        """
        return self.__ctx

    def notebook(self, path: str) -> LazyNotebook:
        """
        Adds a notebook to a pipeline.

        Parameters
        ----------
        path: str
            Path to a notebook.
        """
        new_nb = LazyNotebook(path, self.__ctx)

        # Attach plugin actions to a notebook.
        for plugin in self.__nb_plugins.values():
            setattr(
                new_nb,
                plugin.action_name,
                types.MethodType(
                    lambda notebook, *args, **kwargs:
                        plugin.act(notebook, self, *args, **kwargs),
                    new_nb
                ))

        return new_nb

    def show(self):
        """
        Prints the current state.
        """
        self.__ctx.show()

    def install(self, plugin: Union[FlowPluginBase, NotebookPluginBase]) -> None:
        """
        Installs a plugin to the current instance of Flow.

        Parameters
        ----------
        plugin : FlowPluginBase | NotebookPluginBase
            A plugin object to be installed.
        """
        if isinstance(plugin, FlowPluginBase):
            setattr(
                self,
                plugin.action_name,
                types.MethodType(plugin.act, self))

        elif isinstance(plugin, NotebookPluginBase):
            if plugin.action_name in self.__nb_plugins:
                raise ValueError(
                    f"Notebook plugin {plugin.action_name} is already installed.")
            self.__nb_plugins[plugin.action_name] = plugin

        else:
            raise ValueError("Unexpected plugin type.")
