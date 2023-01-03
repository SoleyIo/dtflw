from __future__ import annotations
from dtflw.events import FlowEvents
from dtflw.runs_recorder import NotebookRun
import dtflw.databricks
import typing
from dtflw.flow_context import FlowContext
from dtflw.input_table import InputTable
from dtflw.output_table import OutputTable


class LazyNotebook:

    class AliasedOutputTable(OutputTable):
        """
        Output table with an alias.
        """

        def __init__(self, name: str, abs_file_path: str, cols: list, ctx: FlowContext, alias: str):
            super().__init__(name, abs_file_path, cols, ctx)
            self.alias = alias

    OUTPUT_TABLE_SUFFIX = "_out"
    INPUT_TABLE_SUFFIX = "_in"

    def __init__(self, rel_path: str, ctx: FlowContext):
        self.__rel_path = rel_path
        self.__args = {}
        self.__timeout = 0

        self.ctx = ctx

        self.__inputs = {}
        self.__outputs = {}

        self.__last_run_result = None

    @property
    def rel_path(self):
        return self.__rel_path

    def args(self, args: typing.Dict[str, str]) -> LazyNotebook:
        self.__args = args
        return self

    def get_args(self):
        """
        Returns arguments.
        """
        return self.__args

    def timeout(self, timeout: int) -> LazyNotebook:
        self.__timeout = timeout
        return self

    def input(self, name: str, file_path: str = None, source_table: str = None) -> LazyNotebook:
        """
        Registers an input table which is required by the notebook to run.

        Parameters
        ----------
        name: str
            Name of an input table.
        file_path: str = None
            File path to an input table.
            - If None (default) then input path is gets resolved.
            - If given as a relative path then gets changed to absolute.
            - If given as an absolute path then it is used as given.
        source_table: str
            Name of the source table to pass an an input. 
            If 'source_table' is not given then 'name' is used.
        """
        if name is None or len(name) == 0:
            raise ValueError("Input's name cannot be empty.")

        input_file_path = file_path
        if input_file_path is None:
            # Resolve the input's file either by given 'source' or its `name`
            source_table_name = name
            if source_table is not None and len(source_table) > 0:
                source_table_name = source_table

            input_file_path = self.ctx.resolve_table(
                source_table_name,
                self.rel_path
            )

        elif not self.ctx.storage.is_abs_path(input_file_path):
            # Bind the input to a given specific file
            input_file_path = self.ctx.storage.get_abs_path(input_file_path)

        self.__inputs[name] = InputTable(
            name,
            input_file_path,
            self.ctx)
        return self

    def get_inputs(self):
        """
        Returns input tables.
        """
        return self.__inputs

    def output(self, name: str, cols: list = None, file_path: str = None, alias: str = None) -> LazyNotebook:
        """
        Registers an output table which is expected to be produced by the notebook.

        Parameters
        ----------
        name: str
            Name of an output table.
        cols: list[str,str]
            Schema of an expected output (DataFrame.dtypes format).
        file_path: str = None
            File path to an output table.
            - If None (default) then output path is built from notebook's path and output's name.
            - If given as a relative path then gets changed to absolute.
            - If given as an absolute path then it is used as given.
        alias: str
            Alias by which the output is registered in Flow. If not specified then 'name' value is used.
        """
        if name is None or len(name) == 0:
            raise ValueError("Output's name cannot be empty.")

        output_file_path = file_path

        if output_file_path is None:
            # default behavior

            # path of this notebook starting with project's dir
            project_based_nb_path = dtflw.databricks.get_path_relative_to_project_dir(
                self.rel_path)

            output_file_path = self.ctx.storage.get_abs_path(
                self.ctx.storage.get_path_in_root_dir(
                    self.ctx.storage.get_path_with_file_extension(
                        f"{project_based_nb_path}/{name}"
                    )
                )
            )
        elif not self.ctx.storage.is_abs_path(output_file_path):
            output_file_path = self.ctx.storage.get_abs_path(output_file_path)

        self.__outputs[name] = self.AliasedOutputTable(
            name,
            output_file_path,
            cols,
            self.ctx,
            alias if alias is not None and len(alias) > 0 else name)

        return self

    def get_outputs(self):
        """
        Returns output tables.
        """
        return self.__outputs

    def __validate_tables(self, tables, title: str, strict: bool = False) -> typing.Dict[str, str]:
        """
        Validates if an input/output table is valid.
        """
        for t in tables:
            self.ctx.logger.log(f"{title} '{t.name}': ")
            try:
                t.validate(strict)
                self.ctx.logger.log(f"\t'{t.abs_file_path}'")
            except Exception as e:
                self.ctx.logger.log("Error")
                raise

    @staticmethod
    def __run_notebook(path: str, timeout: int, args: dict, ctx: FlowContext):
        """
        Runs the current notebook.
        This method gets overriden in unit tests.
        """
        return ctx.dbutils.notebook.run(path, timeout, args)

    def collect_arguments(self):
        """
        Collects all the arguments: regular arguments (args) as well as inputs and outputs.

        Returns
        -------
        A list of arguments: [(name: str, value: str, suffix: str)] 
        """

        args = [(name, value, "") for (name, value) in self.__args.items()]

        args.extend([
            (i.name, i.abs_file_path, LazyNotebook.INPUT_TABLE_SUFFIX)
            for i
            in self.__inputs.values()
        ])

        args.extend([
            (o.name, o.abs_file_path, LazyNotebook.OUTPUT_TABLE_SUFFIX)
            for o
            in self.__outputs.values()
        ])

        return args

    def run(self, is_lazy: bool = False, strict_validation: bool = False):
        """
        Runs the notebook.

        Parameters
        ----------
        is_lazy: bool
            Lazy behavior depending on the condition
            is_lazy AND (at least one output needs to be evaluated)

            True, False  => skip
            True, True   => run
            False, False => run
            False, True  => run

        strict_validation: bool
            If True then raises an exception if output tables are not exactly as expected.
            Otherwise, allows an output to have more columns than expected.

        Returns
        -------
        Argument of the `dbutils.notebook.exit` if exists otherwise `None`.
        """
        self.ctx.events.fire(FlowEvents.NOTEBOOK_RUN_REQUESTED, self)

        # Check if inputs are valid. Raises an exception if not.
        self.__validate_tables(self.__inputs.values(), "Input")

        any_output_needs_eval = any(
            [o.needs_eval() for o in self.__outputs.values()]
        )

        if is_lazy and not any_output_needs_eval:
            self.ctx.logger.log(f"Skipped run:")
            self.ctx.logger.log(
                f"\t'{dtflw.databricks.get_notebook_abs_path(self.rel_path)}'")

        else:
            self.ctx.logger.log(f"Running:")
            self.ctx.logger.log(
                f"\t'{dtflw.databricks.get_notebook_abs_path(self.rel_path)}'")

            all_args = {f"{name}{suffix}": value
                        for (name, value, suffix) in self.collect_arguments()}

            self.__last_run_result = LazyNotebook.__run_notebook(
                self.rel_path,
                self.__timeout,
                all_args,
                self.ctx)

        # Check if outputs are valid. Raises an exception if not.
        self.__validate_tables(
            self.__outputs.values(),
            "Output",
            strict=strict_validation
        )

        outputs = {o.alias: o.abs_file_path for o in self.__outputs.values()}
        self.ctx.publish_tables(outputs, self.rel_path)

        # Record the completed notebook's run.
        inputs = {name: i.abs_file_path for name, i in self.__inputs.items()}

        self.ctx.runs.add(
            NotebookRun(
                dtflw.databricks.get_notebook_abs_path(self.rel_path),
                self.get_args(),
                inputs,
                outputs
            )
        )

        return self.__last_run_result

    def show(self):
        """
        Prints the current state of the notebook.
        """
        self.ctx.logger.log(f"Notebook: '{self.rel_path}'")

        self.ctx.logger.log("Args:")
        for (a, v) in self.__args.items():
            self.ctx.logger.log(f"\t'{a}': '{v}'")

        self.ctx.logger.log("Inputs:")
        for (a, i) in self.__inputs.items():
            self.ctx.logger.log(f"\t'{a}': '{i.abs_file_path}'")

        self.ctx.logger.log("Outputs:")
        for (a, o) in self.__outputs.items():
            self.ctx.logger.log(f"\t'{a}': '{o.abs_file_path}'")
