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
    def __init__(self):
        self.verbosity = "default"
        self.info = False
        
    def log(self, msg: str = ""):
        """
        Logs a message.
        """
        assert self.verbosity == "verbose" or self.verbosity == "default", f"Verbosity variable: \"{self.verbosity}\" must be set either \"verbose\" or \"default\""
        assert isinstance(self.info, bool) , f"Info variable: \"{self.info}\" must be boolean!"

        if self.verbosity == "verbose" or self.info:
            print(msg)
