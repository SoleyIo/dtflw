from dtflw.flow_context import FlowContext


class InputTable():
    """
    Represents a table which is required by a notebook as an input.
    """

    def __init__(self, name: str, abs_file_path: str, ctx: FlowContext, is_optional: bool = False):
        self.__name = name
        self.__abs_file_path = abs_file_path
        self.__ctx = ctx
        self.__is_optional = is_optional

    @property
    def name(self) -> str:
        return self.__name

    @property
    def abs_file_path(self) -> str:
        """
        Returns absolute file path of the table, or None if no suitable table was found.
        """
        return self.__abs_file_path
    
    @property
    def is_optional(self) -> bool:
        """
        Returns the optionality indicator of the table.
        """
        return self.__is_optional

    def needs_eval(self) -> bool:
        """
        Returns True if the table needs to be evaluated, and False otherwise.
        """
        if self.is_optional:
            return False
        else:
            return not self.abs_file_path or len(self.__ctx.storage.list(self.abs_file_path)) == 0

    def validate(self, strict: bool = False):
        """
        Checks if the table is valid.
        Raises an exception if not.

        Parameters
        ----
        strict: bool
            Default: False
            No used here but added for compatiblity with output_table.validate.
        """
        if self.needs_eval():
            raise Exception("Required input not found.")
