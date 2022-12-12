class FlowEvents:
    """
    Events fired by a Flow instance.
    """

    NOTEBOOK_RUN_REQUESTED = "notebook_run_requested"
    """
    Gets fired right after a notebook's run has been requested by calling `LazyNotebook.run` method.
    """


class EventHandlerBase:
    """
    Inherit from this class to implement a Flow event handler.
    """
    def handle(*args, **kwargs) -> None:
        """
        Gets called by a dispatcher on a certain event occuring.
        """
        raise NotImplementedError()


class EventDispatcher:
    """
    Dispatches events in a Flow instance.
    """

    def __init__(self):
        self.__handlers = {}

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
        if event_name not in self.__handlers:
            self.__handlers[event_name] = []
        self.__handlers[event_name].append(handler)

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
        if event_name in self.__handlers:
            self.__handlers[event_name].remove(handler)

    def unsubscribe_all(self, event_name: str = None) -> None:
        """
        Unregisters all event handler for a certain event.

        Parameters
        ----------
        event_name: str
            Name of an event to unregister handlers from.
            If None then handlers from all events are unregistered.
        """
        if event_name is None:
            self.__handlers = {}
        else:
            self.__handlers[event_name] = []

    def fire(self, event_name: str, *args, **kwargs):
        """
        Calls handlers for a given event.

        Parameters
        ----------
        event_name: str
            Name of an event to call a handler.

        *args, **kwargs
            Arguments which are passed to handlers.
        """
        if event_name in self.__handlers:
            for h in self.__handlers[event_name]:
                h.handle(*args, **kwargs)
