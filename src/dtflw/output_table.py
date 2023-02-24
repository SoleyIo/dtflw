from dtflw.flow_context import FlowContext
import dtflw.assertions as A


class OutputTable():
    """
    Represents a table promised by a notebook as an output.
    """

    def __init__(self, name: str, abs_file_path: str, cols: list, ctx: FlowContext):
        self.__name = name
        self.__abs_file_path = abs_file_path
        self.__expected_columns = cols
        self.__ctx = ctx

    @property
    def name(self) -> str:
        return self.__name

    @property
    def abs_file_path(self) -> str:
        return self.__abs_file_path

    def needs_eval(self) -> bool:
        """
        Returns True if the table needs to be evaluated, and False otherwise.
        """
        return not self.__ctx.storage.exists(self.abs_file_path)

    def validate(self, strict: bool = False):
        """
        Checks if the table is valid.
        Raises an exception if not.

        Parameters
        ----
        strict: bool
            Default: False
            Do a strict validation on the table, meaning validation will also fail if more columns then expected are found.
        """
        if self.needs_eval():
            raise Exception("Expected output not found.")

        df = self.__ctx.storage.read_table(self.abs_file_path)

        A.assert_schema_compatible(
            df.dtypes,
            self.__expected_columns or [],
            strict
        )
