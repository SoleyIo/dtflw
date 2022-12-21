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

    def log(self, msg: str = ""):
        """
        Method which will log a message.
        """
        print(msg)
