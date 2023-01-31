from abc import ABC, abstractmethod


class LoggerBase(ABC):
    """
    An interface for flow logger.
    Inherit from this class to implement flow logger.
    """

    @abstractmethod
    def info(self, msg: str = ""):
        """
        Logs an info message.
        """
        raise NotImplementedError()

    @abstractmethod
    def error(self, msg: str = ""):
        """
        Logs an error message.
        """
        raise NotImplementedError()



class DefaultLogger(LoggerBase):
    """
    A flow default logger which logs to stdout using `print`.
    """
    def __init__(self, verbosity: str = "default"):
        self.verbosity = verbosity

    def __log(self, msg):
        """
        Logs a message.
        """
        print(msg)

    def info(self, msg: str = ""):
        """
        Logs an info message.
        """
        if self.verbosity == "verbose":
            self.__log(msg)
        else:
            self.verbosity = "default"

    def error(self, msg: str = ""):
        """
        Logs an error message.
        """
        self.__log(msg)
