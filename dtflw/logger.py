from abc import ABC, abstractmethod


class LoggerBase(ABC):
    """
    A base class to implement a Flow logger.
    """

    def __init__(self, verbosity: str = "default"):
        self.verbosity = verbosity

    @property
    def verbosity(self):
        return self.__verbosity

    @verbosity.setter
    def verbosity(self, value):
        """
        Posible values are
        - "default": log only errors.
        - "verbose": log errors as well as info messages.
        """
        if value in ("verbose", "default"):
            self.__verbosity = value
        else:
            self.__verbosity = "default"

    @abstractmethod
    def log(self, msg: str):
        """
        Logs an info message.
        """
        raise NotImplementedError()


class DefaultLogger(LoggerBase):
    """
    Default logger which logs to stdout using `print`.
    """

    def __log(self, msg):
        """
        Logs a message.
        """
        print(msg)

    def log(self, msg: str):
        """
        Logs an info message.
        """
        if self.verbosity == "verbose":
            self.__log(msg)
