from abc import ABC, abstractmethod


class FlowPluginBase(ABC):
    """
    Inherit from this class to implement a Flow plugin.
    """
    @property
    @abstractmethod
    def action_name(self) -> str:
        """
        Name of a method that will be attached to a Flow instance.
        """
        raise NotImplementedError()

    @abstractmethod
    def act(self, flow, *args, **kwargs):
        """
        Implementation of the method.

        Parameters
        ----------
        flow : Flow
            Flow instance to which this plugin is installed.
        """
        raise NotImplementedError()


class NotebookPluginBase(ABC):
    """
    Inherit from this class to implement a LazyNotebook plugin.
    """
    @property
    @abstractmethod
    def action_name(self) -> str:
        """
        Name of a method that will be attached to LazyNotebook instances.
        """
        raise NotImplementedError()

    @abstractmethod
    def act(self, notebook, flow, *args, **kwargs):
        """
        Implementation of the method.

        Parameters
        ----------
        flow : Flow
            Flow instance which created the current notebook.

        notebook : LazyNotebook
            Instance of LazyNotebook to which this plugin is installed. 

        """
        raise NotImplementedError()
