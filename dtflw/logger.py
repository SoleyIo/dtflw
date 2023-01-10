class LoggerBase():
    """
    An interface for flow logger.
    Inherit from this class to implement flow logger.
    """

    def log(self, msg: str = ""):
        """
        Method which will log a message.
        """
        raise NotImplementedError()


class DefaultLogger(LoggerBase):
    """
    A flow default logger which logs to stdout using `print`.
    """
    def __init__(self):
        self.verbosity = "default"

    def log(self, msg: str = ""):
        """
        Method which will log a message.
        """
        assert self.verbosity == "verbose" or self.verbosity == "default", f"Verbosity variable: \"{self.verbosity}\" must be set either \"verbose\" or \"default\""

        if self.verbosity == "verbose":
            print(msg)
