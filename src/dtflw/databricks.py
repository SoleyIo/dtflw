from datetime import timedelta
from pyspark.sql import SparkSession

class _Singelton:
    def __init__(self, klass):
        self.klass = klass
        self.instance = None

    def __call__(self, **kwds):
        if self.instance == None:
            try:
                self.instance = self.klass(**kwds)
            except TypeError as err:
                raise RuntimeError(f"Please provide input parameters. {err.args}")

        return self.instance


def get_spark_session(
    host: str = None,
    cluster_id: str = None,
    token: str = None,
) -> SparkSession:
    """
    Parameters
    ----------
    host : str, optional
        The Databricks workspace URL
    cluster_id: str, optional
        The cluster identifier where the Databricks connect queries should be executed.
    token: str, optional
        The Databricks personal access token used to authenticate into the cluster and on
        whose behalf the queries are executed.

    Returns
    -------
    SparkSession instance.
    """
    session = SparkSession.getActiveSession()

    if session is None:
        if cluster_id is None:
            session = SparkSession.builder.getOrCreate()
        else:
            from databricks.connect import DatabricksSession

            session = DatabricksSession.builder.remote(
                host=host,
                cluster_id=cluster_id,
                token=token,
            ).getOrCreate()

    return session


def set_runtime_config_property(key: str, value: str):
    """
    Wraps `spark.conf.set`.
    """
    get_spark_session().conf.set(key, value)


def get_runtime_config_property(key: str):
    """
    Wraps `spark.conf.get`.
    """
    return get_spark_session().conf.get(key)


def runtime_config_has(key: str):
    """
    Returns True if spark.conf has a value by the given `key`.
    """
    try:
        get_runtime_config_property(key)
    except:
        return False

    return True


class DatabricksWorkspace:

    def __init__(self, config):
        """
        Parameters
        ----------
        config : databricks.sdk.core.Config

        """

        from databricks.sdk import WorkspaceClient

        self._workspace_client = WorkspaceClient(config=config)

    def get_workspace_client(self):
        return self._workspace_client

    def get_dbutils(self):
        """
        Returns a dbutils instance.
        """
        return self._workspace_client.dbutils

    def cluster_details(self, cluster_id: str):
        return self._workspace_client.clusters.get(cluster_id=cluster_id)

    def start_cluster(self, cluster_id: str, timeout=timedelta(minutes=15)):
        """
        Returns ClusterDetails instance
        """
        details = self.cluster_details(cluster_id=cluster_id)
        
        from databricks.sdk.service.compute import State
        if details.state in [State.TERMINATED, State.UNKNOWN, State.ERROR]:
            return self._workspace_client.clusters.start_and_wait(cluster_id=cluster_id, timeout=timeout)
        else:
            return details


_databricks_workspace = _Singelton(DatabricksWorkspace)


def get_databricks_workspace(
    host: str = None,
    token: str = None,
    cluster_id: str = None,
) -> DatabricksWorkspace:
    """
    Parameters
    ----------
    host : str, optional
        The Databricks workspace URL
    token: str, optional
        The Databricks personal access token used to authenticate into the cluster and on
        whose behalf the queries are executed.

    """
    try:
        return _databricks_workspace()
    except RuntimeError as err:
        from databricks.sdk.core import Config

        return _databricks_workspace(
            config=Config(
                host=host,
                token=token,
                cluster_id=cluster_id,
            )
        )


def get_dbutils():
    """
    Returns a dbutils instance.
    """

    spark = get_spark_session()

    if spark.conf.get("spark.databricks.service.server.enabled", "false") == "true":
        return get_databricks_workspace().get_dbutils()
    else:
        import IPython

        return IPython.get_ipython().user_ns["dbutils"]


def run_notebook(path: str, timeout: int, arguments: dict):
    """
    Runs the current notebook.
    """
    return get_dbutils().notebook.run(path, timeout, arguments)


def get_current_username() -> str:
    """
    Returns current spark username.
    """
    return get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().userName().get()


def get_this_notebook_abs_path() -> str:
    """
    Returns an absolute path of the current notebook in the workspace.
    """
    return get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()


def get_this_notebook_abs_cwd() -> str:
    """
    Returns an absolute path to a folder of the current notebook in the workspace.
    """
    return "/".join(get_this_notebook_abs_path().split("/")[:-1])


def get_notebook_abs_path(rel_path) -> str:
    """
    Returns an absolute path to a notebook based on its relative path.
    """
    return "/".join([get_this_notebook_abs_cwd(), rel_path])


def get_path_relative_to_project_dir(rel_path: str) -> str:
    """
    Returns a path starting with a project's directory for a given relative path.

    Example:

    If 'rel_path' is "dir/notebook" and the function is called from '/Repos/user@a.b/project/main'
    then 'project/dir/notebook' is returned.
    """
    return "/".join(get_notebook_abs_path(rel_path).split("/")[3:])


def try_get_context_tag(key, default=None):
    """
    Tries to get a value from `dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()`
    by a given key. Returns default if not found.
    """
    try:
        return get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().tags().apply(key)
    except:
        return default


def is_job_interactive() -> bool:
    """
    Returns True if the current notebook is executed in an interactive job, and False otherwise.
    """
    job_type = try_get_context_tag("jobType")
    return job_type is None
