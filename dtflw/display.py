from abc import ABC, abstractmethod


class DisplayBase(ABC):
    """
    A base type for implementing a display service.
    Display service is used by Flow for interacting with a user in a notebook.
    """
    @abstractmethod
    def show(self, content):
        """
        Shows a given `content` object to a user.
        """
        raise NotImplemented()


class DefaultDisplay(DisplayBase):
    """
    A default display service to interact with a user in a notebook.
    """

    def show(self, content):
        """
        Shows a given `content` object to a user.
        """
        print(content)
