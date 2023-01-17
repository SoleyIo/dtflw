from build.lib.dtflw.flow_context import FlowContext
from dtflw.flow import Flow
import dtflw.databricks as db
from dtflw.logger import DefaultLogger


def init_flow(storage, spark=None, dbutils=None) -> Flow:
    """
    Initializes a new, configured by default, instance of Flow and returns it.
    """

    if not spark:
        spark = db.get_spark_session()

    if not dbutils:
        dbutils = db.get_dbutils()

    logger = DefaultLogger()
    ctx = FlowContext(storage, spark, dbutils, logger)

    return Flow(ctx)
