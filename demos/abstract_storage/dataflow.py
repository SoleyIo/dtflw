from dtflw import init_flow
from dtflw.storage.dbfs import DbfsStorage
import dtflw.databricks as db
      
def get_storage():
    root_dir = f"FileStore/{db.get_current_username()}"
    spark = db.get_spark_session()
    dbutils = db.get_dbutils()

    return DbfsStorage(root_dir, spark, dbutils)

def get_flow():
    return init_flow(get_storage())