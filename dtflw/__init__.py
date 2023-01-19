from dtflw.flow_context import FlowContext
from dtflw.flow import Flow
import dtflw.databricks as db
from dtflw.logger import DefaultLogger, LoggerBase
from pyspark.sql import SparkSession


def init_flow(storage, spark: SparkSession = None, dbutils=None, logger: LoggerBase = None) -> Flow:
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
