# Databricks notebook source
# Requirement
# -----------
# Some features require the entry below to be set in a Spark config:
# spark.databricks.session.share true

# COMMAND ----------

from dtflw import init_flow
from dtflw.storage.dbfs import init_storage

# Hint
# ----
# Check dtflw.storage namespace for other possible storages.
#
storage = init_storage()
flow = init_flow(storage)
is_lazy = True

# COMMAND ----------

flow.ctx.logger.verbosity = "verbose"

# COMMAND ----------

(
  flow.notebook("ingest_data")
    #
    # Feature
    # -------
    # Specify `file_path` of an input to pass it to a notebook.
    # If it is not set then flow attempts to find it from outputs evaluated upstream.
    #
    .input("Orders", file_path = storage.get_abs_path("FileStore/tables/dtflw_intro/orders.csv"))
    .input("OrderDetails", storage.get_abs_path("FileStore/tables/dtflw_intro/order_details.csv"))
    #
    # Feature
    # -------
    # Flow checks if an expected output has been saved to the storage after a notebook run.
    #
    # - If an output does not exist then flow raises an error and, thus, interrupts the pipeline.
    # - If an output does not have specified `cols` then flow raises an error.
    # - If type of a column is `None` then flow checks only if a column with such name exist disregarding its type.
    #
    .output("Orders", cols=[
      ('orderID', 'string'),
      ('customerID', 'string'),
      ('orderDate', 'date'),
      ('requiredDate', 'date'),
      ('shippedDate', 'date'),
      ('shipCity', 'string'),
      ('shipCountry', 'string')
    ])
    .output("OrderDetails", cols=[
      ('orderID', 'string'),
      ('productID', 'string'),
      ('unitPrice', 'double'),
      ('quantity', 'double'),
      ('discount', 'double')
    ])
    #
    # Feature
    # -------
    # Call `show()` instead of `run()` to see the status of the notebook.
    #
    # Feature
    # -------
    # Call `share_arguments()` to makes values of `args`, `inputs` and `outputs` available 
    # for `init_*` functions inside the notebook.
    #
    # Feature
    # -------
    # If `strict_validation` is `True` then flow checks that outputs have exectly the specified `cols`.
    # Otherwise, flow checks only if `cols` of are among columns of the outputs.
    #
    .run(is_lazy, strict_validation=True)
)

# COMMAND ----------

from dtflw.databricks import get_current_username

# Feature
# -------
# On the storage, file paths of outputs repeat the path of a notebook that saves them.
#

display(
  dbutils.fs.ls(
    storage.get_abs_path(storage.get_path_in_root_dir("dtflw_intro/ingest_data"))
  )
)

# COMMAND ----------

(
  flow.notebook("import_data")
    .input("Orders")
    .input("OrderDetails")
    .output(
      "SalesOrderItems", 
      cols=[
        ('price', 'double'),
        ('revenue', 'double')
      ],
      #
      # Feature
      # -------
      # Give an output table an alias which will be used downstream instead of its name.
      #
      alias="SalesOrderPositions"
    )
    .run(is_lazy, strict_validation=False)
)

# COMMAND ----------

storage.read_table(flow["SalesOrderPositions"]).display()

# COMMAND ----------

(
  flow.notebook("analysis/main")
    .args({
      "year": "1997"
    })
    #
    # Feature
    # -------
    # If a table intended as an input has a different name then specify it with `source_table` parameter.
    #
    .input("SalesOrderItems", source_table="SalesOrderPositions")
    .output("ProductRevenueStats")
    .output("ShippingDelays")
    .run(is_lazy)
)

# COMMAND ----------

# Feature
# -------
# Check the current status of the pipeline.
# 
flow.show()
