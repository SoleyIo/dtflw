import types
from typing import Union
from dtflw.events import EventHandlerBase
from dtflw.flow_context import FlowContext
from dtflw.lazy_notebook import LazyNotebook
from dtflw.plugin import FlowPluginBase, NotebookPluginBase


class Flow:
    """
    Orchestrates runs of notebooks.
    """

    def __init__(self, ctx: FlowContext):
        if ctx is None:
            raise ValueError("Context cannot be None.")

        self.__ctx = ctx
        self.__nb_plugins = {}

    def __setitem__(self, _):
        raise NotImplementedError("Not supported.")

    def __getitem__(self, name: str):
        """
        Returns absolute file path to the latest evaluation of a given table.
        If it finds it, otherwise returns None.
        """
        return self.ctx.tables_repo.resolve(name)

    @property
    def ctx(self) -> FlowContext:
        return self.__ctx

    def notebook(self, path: str) -> LazyNotebook:
        new_nb = LazyNotebook(path, self.__ctx)

        # attach plugin actions to a notebook
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

    def subscribe(self, event_name: str, handler: EventHandlerBase) -> None:
        """
        Registers an event handler for a certain event.

        Parameters
        ----------
        event_name: str
            Name of an event to call a handler.
        handler: EventHandlerBase
            Event handler instance.
        """
        self.ctx.events.subscribe(event_name, handler)

    def unsubscribe(self, event_name: str, handler: EventHandlerBase) -> None:
        """
        Unregisters an event handler for a certain event.

        Parameters
        ----------
        event_name: str
            Name of an event to call a handler.
        handler: EventHandlerBase
            Event handler instance.
        """
        self.ctx.events.unsubscribe(event_name, handler)
