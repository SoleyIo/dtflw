from abc import ABC, abstractmethod


class LoggerBase(ABC):
    """
    An interface for flow logger.
    Inherit from this class to implement flow logger.
    """

    @abstractmethod
    def log(self, msg: str = ""):
        """
        Logs a message.
        """
        raise NotImplementedError()


class DefaultLogger(LoggerBase):
    """
    A flow default logger which logs to stdout using `print`.
    """

    def log(self, msg: str = ""):
        """
        Logs a message.
        """
        print(msg)
