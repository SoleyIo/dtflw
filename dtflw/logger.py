from abc import ABC, abstractmethod


class LoggerBase(ABC):
    """
    An interface for flow logger.
    Inherit from this class to implement flow logger.
    """
        
    @abstractmethod
    def info(self, msg: str = ""):
        """
        Logs a message.
        """
        raise NotImplementedError()

    @abstractmethod
    def error(self, msg: str = ""):
        """
        Logs a message.
        """
        raise NotImplementedError()



class DefaultLogger(LoggerBase):
    """
    A flow default logger which logs to stdout using `print`.
    """
    
    verbosity = "default"
    
    def info(self, msg: str = ""):
        """
        Logs info messages.
        """
        assert self.verbosity == "verbose" or self.verbosity == "default", f"Verbosity variable can not be set to: \"{self.verbosity}\", it must be set either \"verbose\" or \"default\"."

        if self.verbosity == "verbose" :
            print(msg)
        

    def error(self, msg: str = ""):
        """
        Logs error messages.
        """
        print(msg)
