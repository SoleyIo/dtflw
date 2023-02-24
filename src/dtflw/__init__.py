import dtflw.arguments as a
from dtflw.flow_context import FlowContext
from dtflw.flow import Flow
import dtflw.databricks as db
from dtflw.storage.fs import FileStorageBase
from dtflw.logger import DefaultLogger, LoggerBase
from pyspark.sql import SparkSession


def init_flow(storage: FileStorageBase, spark: SparkSession = None, dbutils=None, logger: LoggerBase = None) -> Flow:
    """
    Initializes a new, configured by default, instance of Flow and returns it.
    """

    if spark is None:
        spark = db.get_spark_session()

    if dbutils is None:
        dbutils = db.get_dbutils()

    if logger is None:
        logger = DefaultLogger()

    ctx = FlowContext(storage, spark, dbutils, logger)

    return Flow(ctx)


def init_args(*args):
    """
    Initializes args

    Parameters
    ----------
    *args
        Option 1:
            Given as a variable number of non-keyword arguments, e.g. `args = init_args("a", "b", "c")`.
            All the args will have an empty string as a value.
        Option 2:
            Given as a dictionary with default values, e.g. `args = init_args({'a': 42})`.
            All non-string values will be stringified.

    Returns
    -------
    dict[str: Argument]
    """
    return a.Argument.create(*args)


def init_inputs(*inputs):
    """
    Initializes input tables.

    Parameters
    ----------
    *inputs
        Option 1:
            Given as a variable number of non-keyword arguments, e.g. `inputs = init_inputs("Orders", "Customers")`.
            All the inputs will have an empty string as a value.
        Option 2:
            Given as a dictionary with default values, e.g. `inputs = init_inputs({'Orders': 'orders.parquet'})`.
            All non-string values will be stringified.

    Returns
    -------
    dict[str: Input]
    """
    return a.Input.create(*inputs)


def init_outputs(*outputs):
    """
    Initializes output tables.

    Parameters
    ----------
    *outputs
        Option 1:
            Given as a variable number of non-keyword arguments, e.g. `outputs = init_outputs("Orders", "Customers")`.
            All the outputs will have an empty string as a value.
        Option 2:
            Given as a dictionary with default values, e.g. `outputs = init_outputs({'Orders': 'orders.parquet'})`.
            All non-string values will be stringified.

    Returns
    -------
    dict[str: Output]
    """
    return a.Output.create(*outputs)
