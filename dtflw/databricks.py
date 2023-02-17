from pyspark.sql import SparkSession


def run_notebook(path: str, timeout: int, arguments: dict):
    """
    Runs the current notebook.
    """
    return get_dbutils().notebook.run(path, timeout, arguments)


def get_spark_session() -> SparkSession:
    """
    Returns a SparkSession instance.
    """
    return SparkSession.builder.getOrCreate()


def get_dbutils():
    """
    Returns a DBUtils instance.
    """
    spark = get_spark_session()

    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    else:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


def get_current_username() -> str:
    """
    Returns current spark username.
    """
    dbutils = get_dbutils()
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')


def get_this_notebook_abs_path() -> str:
    """
    Returns an absolute path of the current notebook in the workspace.
    """
    return get_dbutils().notebook().entry_point.getDbutils().notebook().getContext().notebookPath().get()


def get_this_notebook_abs_cwd() -> str:
    """
    Returns an absolute path to a folder of the current notebook in the workspace.
    """
    return '/'.join(get_this_notebook_abs_path().split('/')[:-1])


def get_notebook_abs_path(rel_path) -> str:
    """
    Returns an absolute path to a notebook based on its relative path.
    """
    return '/'.join([get_this_notebook_abs_cwd(), rel_path])


def get_path_relative_to_project_dir(rel_path: str) -> str:
    """
    Returns a path starting with a project's directory for a given relative path.

    Example:

    If 'rel_path' is "dir/notebook" and the function is called from '/Repos/user@a.b/project/main'
    then 'project/dir/notebook' is returned.
    """
    return "/".join(get_notebook_abs_path(rel_path).split("/")[3:])


def try_get_context_tag(key, defaut=None):
    """
    Tries to get a value from `dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags()`
    by a given key. Returns default if not found.
    """
    try:
        dbutils = get_dbutils()
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply(key)
    except Exception as e:
        if "java.util.NoSuchElementException: key not found" in str(e):
            return defaut
        else:
            raise


def is_job_interactive() -> bool:
    """
    Returns True if the current notebook is executed in an interactive job, and False otherwise.
    """
    job_type = try_get_context_tag("jobType")
    return job_type is None


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
    except Exception as e:
        if "java.util.NoSuchElementException" in str(e):
            return False
    return True
