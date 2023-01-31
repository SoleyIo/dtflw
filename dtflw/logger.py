from abc import ABC, abstractmethod


class LoggerBase(ABC):
    """
    A base class to implement a Flow logger.
    """

    @abstractmethod
    def info(self, msg: str):
        """
        Logs an info message.
        """
        raise NotImplementedError()

    @abstractmethod
    def error(self, msg: str):
        """
        Logs an error message.
        """
        raise NotImplementedError()


class DefaultLogger(LoggerBase):
    """
    Default logger which logs to stdout using `print`.
    """

    def __init__(self, verbosity: str = "default"):
        self.verbosity = verbosity

    @property
    def verbosity(self):
        """
        Posible values are
        - "default": log only errors.
        - "verbose": log errors as well as info messages.
        """
        return self.__verbosity

    @verbosity.setter
    def verbosity(self, value):
        if value in ("verbose", "default"):
            self.__verbosity = value
        else:
            self.__verbosity = "default"

    def __log(self, msg):
        """
        Logs a message.
        """
        print(msg)

    def info(self, msg: str):
        """
        Logs an info message.
        """
        if self.__verbosity == "verbose":
            self.__log(msg)

    def error(self, msg: str):
        """
        Logs an error message.
        """
        self.__log(msg)
