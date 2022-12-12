class FlowPluginBase:
    """
    Inherit from this class to implement a Flow plugin.
    """
    @property
    def action_name(self) -> str:
        """
        Name of a method that will be attached to a Flow instance.
        """
        raise NotImplementedError()

    def act(self, flow, *args, **kwargs):
        """
        Implementation of the method.

        Parameters
        ----------
        flow : Flow
            Flow instance to which this plugin is installed.
        """
        raise NotImplementedError()


class NotebookPluginBase:
    """
    Inherit from this class to implement a LazyNotebook plugin.
    """
    @property
    def action_name(self) -> str:
        """
        Name of a method that will be attached to LazyNotebook instances.
        """
        raise NotImplementedError()

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
