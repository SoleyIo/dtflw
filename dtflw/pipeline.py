class NotebookRun():
    """
    Carries information related to a run of a notebook.

    Parameters
    ----------
    notebook_path: str
        Absolute path to a notebook in a worspace or repo.

    args: {str: str}
        A dictionary of arguments.

    inputs: {str, str}
        A dictionary of input tables' names to absolute path they are stored.

    outputs: {str, str}
        A dictionary of outputs tables' names to absolute path they are stored.
    """

    def __init__(self, notebook_path, args, inputs, outputs):
        self.__notebook_path = notebook_path
        self.__args = args
        self.__inputs = inputs
        self.__outputs = outputs

    @property
    def notebook_path(self):
        return self.__notebook_path

    @property
    def args(self):
        return self.__args

    @property
    def inputs(self):
        return self.__inputs

    @property
    def outputs(self):
        return self.__outputs


class PipelineState():
    """
    Represents the current execution state of notebooks in a pipeline.
    """

    def __init__(self):
        # Keeps last completed notebooks' runs
        self.__runs = {}

    @property
    def runs(self):
        """
        Returns a generator over added runs.
        """
        for run in self.__runs.values():
            yield run

    def record_run(self, run: NotebookRun):
        """
        Records a notebook's run.
        If a run has a notebook's path remembered before then it overwrites the latter.

        Parameters
        ----------
        run: NotebookRun
            A run of a notebook.
        """
        if run is None:
            raise ValueError("run cannot be None.")

        self.__runs[run.notebook_path] = run
